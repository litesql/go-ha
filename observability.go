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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const (
	HealthPath    = "/healthz"
	ReadinessPath = "/readyz"
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

func (c *Connector) ObservabilityHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(HealthPath, c.HealthHandler())
	mux.Handle(ReadinessPath, c.ReadinessHandler())
	if c.telemetry != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, span := c.telemetry.tracer.Start(r.Context(), "go_ha.observability.http")
			started := time.Now()
			route := "unmatched"
			switch r.URL.Path {
			case HealthPath, ReadinessPath:
				route = r.URL.Path
			}
			span.SetAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("http.route", route),
				attribute.String("node", c.name),
				attribute.String("replication_id", c.replicationID),
			)
			response := &observabilityResponseWriter{ResponseWriter: w}
			defer func() {
				statusCode := response.statusCode
				if statusCode == 0 {
					statusCode = http.StatusOK
				}
				span.SetAttributes(attribute.Int("http.response.status_code", statusCode))
				if statusCode >= http.StatusInternalServerError {
					span.SetStatus(codes.Error, http.StatusText(statusCode))
				}
				c.telemetry.recordHTTPRequest(ctx, started, statusCode, r.Method, route, c.name, c.replicationID)
				span.End()
			}()
			mux.ServeHTTP(response, r.WithContext(ctx))
		})
	}
	return mux
}

type observabilityResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *observabilityResponseWriter) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *observabilityResponseWriter) Write(body []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
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
