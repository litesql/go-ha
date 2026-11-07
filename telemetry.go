package ha

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const telemetryInstrumentationName = "github.com/litesql/go-ha"

type openTelemetry struct {
	tracer trace.Tracer

	publishedChangesets metric.Int64Counter
	publishErrors       metric.Int64Counter
	publishDuration     metric.Float64Histogram
	appliedChangesets   metric.Int64Counter
	applyErrors         metric.Int64Counter
	applyDuration       metric.Float64Histogram
	createdSnapshots    metric.Int64Counter
	snapshotErrors      metric.Int64Counter
	snapshotDuration    metric.Float64Histogram
	httpRequests        metric.Int64Counter
	httpDuration        metric.Float64Histogram

	leader            metric.Int64ObservableGauge
	ready             metric.Int64ObservableGauge
	natsConnected     metric.Int64ObservableGauge
	publishedSequence metric.Int64ObservableGauge
	appliedSequence   metric.Int64ObservableGauge
	replicationLag    metric.Int64ObservableGauge
	snapshotTimestamp metric.Float64ObservableGauge
	collectionErrors  metric.Int64ObservableGauge
	registration      metric.Registration
}

func sequenceToInt64(sequence uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if sequence > maxInt64 {
		return int64(maxInt64)
	}
	return int64(sequence)
}

func newOpenTelemetry(c *Connector) (*openTelemetry, error) {
	meterProvider := c.otelMeterProvider
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}
	tracerProvider := c.otelTracerProvider
	if tracerProvider == nil {
		tracerProvider = otel.GetTracerProvider()
	}

	meter := meterProvider.Meter(telemetryInstrumentationName)
	t := &openTelemetry{
		tracer: tracerProvider.Tracer(telemetryInstrumentationName),
	}
	var err error
	if t.publishedChangesets, err = meter.Int64Counter("go_ha.replication.changesets.published", metric.WithDescription("Number of published replication changesets.")); err != nil {
		return nil, fmt.Errorf("create published changesets instrument: %w", err)
	}
	if t.publishErrors, err = meter.Int64Counter("go_ha.replication.publish.errors", metric.WithDescription("Number of failed replication publish operations.")); err != nil {
		return nil, fmt.Errorf("create publish errors instrument: %w", err)
	}
	if t.publishDuration, err = meter.Float64Histogram("go_ha.replication.publish.duration", metric.WithUnit("s"), metric.WithDescription("Time spent publishing replication changesets.")); err != nil {
		return nil, fmt.Errorf("create publish duration instrument: %w", err)
	}
	if t.appliedChangesets, err = meter.Int64Counter("go_ha.replication.changesets.applied", metric.WithDescription("Number of successfully applied replication changesets.")); err != nil {
		return nil, fmt.Errorf("create applied changesets instrument: %w", err)
	}
	if t.applyErrors, err = meter.Int64Counter("go_ha.replication.apply.errors", metric.WithDescription("Number of failed replication apply operations.")); err != nil {
		return nil, fmt.Errorf("create apply errors instrument: %w", err)
	}
	if t.applyDuration, err = meter.Float64Histogram("go_ha.replication.apply.duration", metric.WithUnit("s"), metric.WithDescription("Time spent applying replication changesets.")); err != nil {
		return nil, fmt.Errorf("create apply duration instrument: %w", err)
	}
	if t.createdSnapshots, err = meter.Int64Counter("go_ha.snapshots.created", metric.WithDescription("Number of successfully created snapshots.")); err != nil {
		return nil, fmt.Errorf("create snapshots instrument: %w", err)
	}
	if t.snapshotErrors, err = meter.Int64Counter("go_ha.snapshot.errors", metric.WithDescription("Number of failed snapshot operations.")); err != nil {
		return nil, fmt.Errorf("create snapshot errors instrument: %w", err)
	}
	if t.snapshotDuration, err = meter.Float64Histogram("go_ha.snapshot.duration", metric.WithUnit("s"), metric.WithDescription("Time spent creating database snapshots.")); err != nil {
		return nil, fmt.Errorf("create snapshot duration instrument: %w", err)
	}
	if t.httpRequests, err = meter.Int64Counter("go_ha.http.requests", metric.WithDescription("Number of requests handled by the observability HTTP handler.")); err != nil {
		return nil, fmt.Errorf("create HTTP requests instrument: %w", err)
	}
	if t.httpDuration, err = meter.Float64Histogram("go_ha.http.request.duration", metric.WithUnit("s"), metric.WithDescription("Time spent handling requests by the observability HTTP handler.")); err != nil {
		return nil, fmt.Errorf("create HTTP request duration instrument: %w", err)
	}
	if t.leader, err = meter.Int64ObservableGauge("go_ha.cluster.leader", metric.WithDescription("Whether this node is the current cluster leader.")); err != nil {
		return nil, fmt.Errorf("create leader instrument: %w", err)
	}
	if t.ready, err = meter.Int64ObservableGauge("go_ha.cluster.ready", metric.WithDescription("Whether this node is ready to serve requests.")); err != nil {
		return nil, fmt.Errorf("create readiness instrument: %w", err)
	}
	if t.natsConnected, err = meter.Int64ObservableGauge("go_ha.nats.connected", metric.WithDescription("Whether the configured NATS connection is connected.")); err != nil {
		return nil, fmt.Errorf("create NATS instrument: %w", err)
	}
	if t.publishedSequence, err = meter.Int64ObservableGauge("go_ha.replication.sequence.published", metric.WithDescription("Latest replication sequence published by this node.")); err != nil {
		return nil, fmt.Errorf("create published sequence instrument: %w", err)
	}
	if t.appliedSequence, err = meter.Int64ObservableGauge("go_ha.replication.sequence.applied", metric.WithDescription("Latest replication sequence applied by this node.")); err != nil {
		return nil, fmt.Errorf("create applied sequence instrument: %w", err)
	}
	if t.replicationLag, err = meter.Int64ObservableGauge("go_ha.replication.lag", metric.WithUnit("{message}"), metric.WithDescription("Unapplied replication messages for a cluster node.")); err != nil {
		return nil, fmt.Errorf("create replication lag instrument: %w", err)
	}
	if t.snapshotTimestamp, err = meter.Float64ObservableGauge("go_ha.snapshot.last.timestamp", metric.WithUnit("s"), metric.WithDescription("Unix timestamp of the most recent database snapshot.")); err != nil {
		return nil, fmt.Errorf("create snapshot timestamp instrument: %w", err)
	}
	if t.collectionErrors, err = meter.Int64ObservableGauge("go_ha.observability.collection.errors", metric.WithDescription("Whether cluster observability collection encountered an error.")); err != nil {
		return nil, fmt.Errorf("create collection errors instrument: %w", err)
	}

	t.registration, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		status := c.observabilityStatus(ctx, true)
		attrs := metric.WithAttributes(
			attribute.String("node", status.NodeName),
			attribute.String("replication_id", status.ReplicationID),
		)
		observer.ObserveInt64(t.leader, boolToInt64(status.Leader), attrs)
		observer.ObserveInt64(t.ready, boolToInt64(status.Ready), attrs)
		observer.ObserveInt64(t.natsConnected, boolToInt64(status.NATSConnected), attrs)
		observer.ObserveInt64(t.publishedSequence, sequenceToInt64(status.PublishedSequence), attrs)
		observer.ObserveInt64(t.appliedSequence, sequenceToInt64(status.AppliedSequence), attrs)
		observer.ObserveInt64(t.collectionErrors, boolToInt64(len(status.ObservabilityErrors) > 0), attrs)
		for node, lag := range status.ReplicationLagByNode {
			observer.ObserveInt64(t.replicationLag, sequenceToInt64(lag), metric.WithAttributes(
				attribute.String("node", node),
				attribute.String("replication_id", status.ReplicationID),
			))
		}
		if status.LastSnapshotTime != nil {
			observer.ObserveFloat64(t.snapshotTimestamp, float64(status.LastSnapshotTime.UnixNano())/float64(time.Second), attrs)
		}
		return nil
	},
		t.leader,
		t.ready,
		t.natsConnected,
		t.publishedSequence,
		t.appliedSequence,
		t.replicationLag,
		t.snapshotTimestamp,
		t.collectionErrors,
	)
	if err != nil {
		return nil, fmt.Errorf("register cluster metrics callback: %w", err)
	}
	return t, nil
}

func (t *openTelemetry) close() error {
	if t == nil || t.registration == nil {
		return nil
	}
	return t.registration.Unregister()
}

func (t *openTelemetry) recordPublish(ctx context.Context, started time.Time, err error, node, replicationID string) {
	attrs := metric.WithAttributes(
		attribute.String("node", node),
		attribute.String("replication_id", replicationID),
		attribute.String("result", operationResult(err)),
	)
	t.publishDuration.Record(ctx, time.Since(started).Seconds(), attrs)
	if err == nil {
		t.publishedChangesets.Add(ctx, 1, attrs)
	} else {
		t.publishErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("node", node),
			attribute.String("replication_id", replicationID),
		))
	}
}

func (t *openTelemetry) recordApply(ctx context.Context, started time.Time, err error, node, replicationID string) {
	attrs := metric.WithAttributes(
		attribute.String("node", node),
		attribute.String("replication_id", replicationID),
		attribute.String("result", operationResult(err)),
	)
	t.applyDuration.Record(ctx, time.Since(started).Seconds(), attrs)
	if err == nil {
		t.appliedChangesets.Add(ctx, 1, attrs)
	} else {
		t.applyErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("node", node),
			attribute.String("replication_id", replicationID),
		))
	}
}

func (t *openTelemetry) recordSnapshot(ctx context.Context, started time.Time, sequence uint64, err error, node, replicationID string) {
	attrs := metric.WithAttributes(
		attribute.String("node", node),
		attribute.String("replication_id", replicationID),
		attribute.String("result", operationResult(err)),
	)
	t.snapshotDuration.Record(ctx, time.Since(started).Seconds(), attrs)
	if err == nil && sequence > 0 {
		t.createdSnapshots.Add(ctx, 1, attrs)
	} else if err != nil {
		t.snapshotErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("node", node),
			attribute.String("replication_id", replicationID),
		))
	}
}

func (t *openTelemetry) recordHTTPRequest(ctx context.Context, started time.Time, statusCode int, method, route, node, replicationID string) {
	attrs := metric.WithAttributes(
		attribute.Int("http.response.status_code", statusCode),
		attribute.String("http.request.method", method),
		attribute.String("http.route", route),
		attribute.String("node", node),
		attribute.String("replication_id", replicationID),
	)
	t.httpRequests.Add(ctx, 1, attrs)
	t.httpDuration.Record(ctx, time.Since(started).Seconds(), attrs)
}

func operationResult(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

func boolToInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

type telemetryPublisher struct {
	publisher Publisher
	telemetry *openTelemetry
	node      string
	id        string
}

type telemetryTwoPhaseCommitPublisher struct {
	*telemetryPublisher
	preparer TwoPhaseCommitPreparer
}

func wrapTelemetryPublisher(publisher Publisher, telemetry *openTelemetry, node, id string) Publisher {
	instrumented := &telemetryPublisher{
		publisher: publisher,
		telemetry: telemetry,
		node:      node,
		id:        id,
	}
	if preparer, ok := publisher.(TwoPhaseCommitPreparer); ok {
		return &telemetryTwoPhaseCommitPublisher{
			telemetryPublisher: instrumented,
			preparer:           preparer,
		}
	}
	return instrumented
}

func (p *telemetryPublisher) Publish(ctx context.Context, cs *ChangeSet) error {
	ctx, span := p.telemetry.tracer.Start(ctx, "go_ha.replication.publish")
	started := time.Now()
	carrier := propagation.MapCarrier{}
	cs.TraceContext = nil
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) > 0 {
		cs.TraceContext = carrier
	}
	span.SetAttributes(
		attribute.String("node", p.node),
		attribute.String("replication_id", p.id),
		attribute.Int("changeset.change_count", len(cs.Changes)),
	)
	err := p.publisher.Publish(ctx, cs)
	span.SetAttributes(attribute.Int64("messaging.message.sequence", sequenceToInt64(p.publisher.Sequence())))
	p.telemetry.recordPublish(ctx, started, err, p.node, p.id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "published")
	}
	span.End()
	return err
}

func (p *telemetryTwoPhaseCommitPublisher) PrepareTwoPhaseCommit(ctx context.Context, cs *ChangeSet) (PreparedTwoPhaseCommit, error) {
	ctx, span := p.telemetry.tracer.Start(ctx, "go_ha.replication.prepare")
	carrier := propagation.MapCarrier{}
	cs.TraceContext = nil
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) > 0 {
		cs.TraceContext = carrier
	}
	span.SetAttributes(
		attribute.String("node", p.node),
		attribute.String("replication_id", p.id),
		attribute.Int("changeset.change_count", len(cs.Changes)),
	)
	prepared, err := p.preparer.PrepareTwoPhaseCommit(ctx, cs)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "prepared")
	}
	span.End()
	return prepared, err
}

func (p *telemetryTwoPhaseCommitPublisher) RecoverTwoPhaseCommits(ctx context.Context) error {
	if recover, ok := p.publisher.(interface {
		RecoverTwoPhaseCommits(ctx context.Context) error
	}); ok {
		ctx, span := p.telemetry.tracer.Start(ctx, "go_ha.replication.recover")
		err := recover.RecoverTwoPhaseCommits(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "recovered")
		}
		span.End()
		return err
	}
	return nil
}

func (p *telemetryPublisher) Sequence() uint64 {
	return p.publisher.Sequence()
}

func (p *telemetryPublisher) SequenceRange() (uint64, uint64) {
	if rp, ok := p.publisher.(interface{ SequenceRange() (uint64, uint64) }); ok {
		return rp.SequenceRange()
	}
	seq := p.publisher.Sequence()
	return seq, seq
}

func (p *telemetryPublisher) BindLocalDB(db *sql.DB) error {
	if binder, ok := p.publisher.(interface{ BindLocalDB(*sql.DB) error }); ok {
		return binder.BindLocalDB(db)
	}
	return nil
}
