package ha

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
)

const (
	HealthPath    = "/healthz"
	ReadinessPath = "/readyz"
	MetricsPath   = "/metrics"
)

type ObservabilityStatus struct {
	Status               string            `json:"status"`
	Ready                bool              `json:"ready"`
	NodeName             string            `json:"node_name"`
	ReplicationID        string            `json:"replication_id"`
	Leader               bool              `json:"leader"`
	LeaderTarget         string            `json:"leader_target,omitempty"`
	NATSConfigured       bool              `json:"nats_configured"`
	NATSConnected        bool              `json:"nats_connected"`
	NATSStatus           string            `json:"nats_status"`
	PublishedSequence    uint64            `json:"published_sequence"`
	AppliedSequence      uint64            `json:"applied_sequence"`
	ReplicationLag       uint64            `json:"replication_lag_messages"`
	ReplicationLagByNode map[string]uint64 `json:"replication_lag_by_node"`
	LastSnapshotTime     *time.Time        `json:"last_snapshot_time,omitempty"`
	ObservabilityErrors  []string          `json:"observability_errors,omitempty"`
}

func (c *Connector) HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !observabilityGET(w, r) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		status := c.observabilityStatus(ctx, false)
		status.Status = "ok"
		writeObservabilityStatus(w, http.StatusOK, status)
	})
}

func (c *Connector) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !observabilityGET(w, r) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		status := c.observabilityStatus(ctx, true)
		code := http.StatusOK
		if !status.Ready {
			code = http.StatusServiceUnavailable
		}
		writeObservabilityStatus(w, code, status)
	})
}

func (c *Connector) MetricsHandler() http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(newConnectorCollector(c))
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !observabilityGET(w, r) {
			return
		}
		handler.ServeHTTP(w, r)
	})
}

func (c *Connector) ObservabilityHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(HealthPath, c.HealthHandler())
	mux.Handle(ReadinessPath, c.ReadinessHandler())
	mux.Handle(MetricsPath, c.MetricsHandler())
	if c.telemetry != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, span := c.telemetry.tracer.Start(r.Context(), "go_ha.observability.http")
			route := "unmatched"
			switch r.URL.Path {
			case HealthPath, ReadinessPath, MetricsPath:
				route = r.URL.Path
			}
			span.SetAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("http.route", route),
			)
			mux.ServeHTTP(w, r.WithContext(ctx))
			span.End()
		})
	}
	return mux
}

func observabilityGET(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	w.Header().Set("Allow", http.MethodGet)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func writeObservabilityStatus(w http.ResponseWriter, code int, status ObservabilityStatus) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(status); err != nil {
		slog.Error("failed to write observability status", "error", err)
	}
}

func (c *Connector) observabilityStatus(ctx context.Context, checkDatabase bool) ObservabilityStatus {
	started := c.started.Load()
	status := ObservabilityStatus{
		Status:               "ok",
		NodeName:             c.name,
		ReplicationID:        c.replicationID,
		NATSConfigured:       c.natsConn != nil,
		NATSStatus:           "not_configured",
		ReplicationLagByNode: make(map[string]uint64),
	}
	if started {
		status.PublishedSequence = c.PubSeq()
		status.AppliedSequence = c.LatestSeq()
	}
	if status.PublishedSequence > status.AppliedSequence {
		status.ReplicationLag = status.PublishedSequence - status.AppliedSequence
	}
	status.ReplicationLagByNode[c.name] = status.ReplicationLag

	if c.leaderProvider != nil {
		status.Leader = c.leaderProvider.IsLeader()
		status.LeaderTarget = c.leaderProvider.RedirectTarget()
	}
	if c.natsConn != nil {
		status.NATSConnected = c.natsConn.IsConnected()
		status.NATSStatus = c.natsConn.Status().String()
		if status.NATSConnected {
			consumerCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			info, err := c.subscriber.DeliveredInfo(consumerCtx, "")
			if err != nil {
				status.ObservabilityErrors = append(status.ObservabilityErrors, fmt.Sprintf("list replication consumers: %v", err))
			} else if consumers, ok := info.([]*jetstream.ConsumerInfo); ok {
				for _, consumer := range consumers {
					if consumer == nil || consumer.Config.FilterSubject != c.replicationSubject {
						continue
					}
					node := consumer.Config.Description
					if node == "" {
						node = c.consumerNodeName(consumer.Name)
					}
					lag := uint64(consumer.NumAckPending) + consumer.NumPending
					status.ReplicationLagByNode[node] = lag
					if node == c.name {
						status.ReplicationLag = lag
					}
				}
			} else {
				status.ObservabilityErrors = append(status.ObservabilityErrors, "list replication consumers: unexpected response type")
			}
		}
	}

	if snapshotter, ok := c.snapshotter.(interface{ LatestSnapshotTime() time.Time }); ok {
		if timestamp := snapshotter.LatestSnapshotTime(); !timestamp.IsZero() {
			status.LastSnapshotTime = &timestamp
		}
	}

	status.Ready = started
	if checkDatabase && status.Ready {
		if c.db == nil {
			status.Ready = false
			status.ObservabilityErrors = append(status.ObservabilityErrors, "database is not initialized")
		} else if err := c.db.PingContext(ctx); err != nil {
			status.Ready = false
			status.ObservabilityErrors = append(status.ObservabilityErrors, fmt.Sprintf("database ping failed: %v", err))
		}
	}
	if c.natsConn != nil && !status.NATSConnected {
		status.Ready = false
	}
	if provider, ok := c.leaderProvider.(interface{ IsReady() bool }); ok && !provider.IsReady() {
		status.Ready = false
	}
	if !status.Ready {
		status.Status = "not_ready"
	}
	return status
}

func (c *Connector) consumerNodeName(consumer string) string {
	prefix := normalizeNatsIdentifier(c.replicationID) + "_"
	if prefix != "" {
		if node := strings.TrimPrefix(consumer, prefix); node != consumer {
			return node
		}
	}
	return consumer
}

type connectorCollector struct {
	connector  *Connector
	leader     *prometheus.Desc
	leaderInfo *prometheus.Desc
	ready      *prometheus.Desc
	published  *prometheus.Desc
	applied    *prometheus.Desc
	lag        *prometheus.Desc
	nats       *prometheus.Desc
	snapshot   *prometheus.Desc
	errors     *prometheus.Desc
}

func newConnectorCollector(c *Connector) *connectorCollector {
	constLabels := []string{"node", "replication_id"}
	return &connectorCollector{
		connector:  c,
		leader:     prometheus.NewDesc("go_ha_leader", "Whether this node is the current cluster leader.", constLabels, nil),
		leaderInfo: prometheus.NewDesc("go_ha_leader_info", "Current cluster leader target.", []string{"node", "replication_id", "target"}, nil),
		ready:      prometheus.NewDesc("go_ha_ready", "Whether this node is ready to serve requests.", constLabels, nil),
		published:  prometheus.NewDesc("go_ha_replication_published_sequence", "Latest replication sequence published by this node.", constLabels, nil),
		applied:    prometheus.NewDesc("go_ha_replication_applied_sequence", "Latest replication sequence applied by this node.", constLabels, nil),
		lag:        prometheus.NewDesc("go_ha_replication_lag_messages", "Unapplied replication messages for a cluster node.", constLabels, nil),
		nats:       prometheus.NewDesc("go_ha_nats_connected", "Whether the configured NATS connection is connected.", constLabels, nil),
		snapshot:   prometheus.NewDesc("go_ha_snapshot_last_timestamp_seconds", "Unix timestamp of the most recent database snapshot, or zero if none exists.", constLabels, nil),
		errors:     prometheus.NewDesc("go_ha_observability_collection_error", "Whether cluster observability data collection encountered an error.", constLabels, nil),
	}
}

func (c *connectorCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.leader
	ch <- c.leaderInfo
	ch <- c.ready
	ch <- c.published
	ch <- c.applied
	ch <- c.lag
	ch <- c.nats
	ch <- c.snapshot
	ch <- c.errors
}

func (c *connectorCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status := c.connector.observabilityStatus(ctx, true)
	labels := []string{status.NodeName, status.ReplicationID}

	gauge(ch, c.leader, boolGauge(status.Leader), labels...)
	ch <- prometheus.MustNewConstMetric(c.leaderInfo, prometheus.GaugeValue, 1, status.NodeName, status.ReplicationID, status.LeaderTarget)
	gauge(ch, c.ready, boolGauge(status.Ready), labels...)
	gauge(ch, c.published, float64(status.PublishedSequence), labels...)
	gauge(ch, c.applied, float64(status.AppliedSequence), labels...)
	gauge(ch, c.nats, boolGauge(status.NATSConnected), labels...)
	var snapshotTime float64
	if status.LastSnapshotTime != nil {
		snapshotTime = float64(status.LastSnapshotTime.UnixNano()) / float64(time.Second)
	}
	gauge(ch, c.snapshot, snapshotTime, labels...)
	gauge(ch, c.errors, boolGauge(len(status.ObservabilityErrors) > 0), labels...)
	for node, lag := range status.ReplicationLagByNode {
		gauge(ch, c.lag, float64(lag), node, status.ReplicationID)
	}
}

func gauge(ch chan<- prometheus.Metric, desc *prometheus.Desc, value float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
