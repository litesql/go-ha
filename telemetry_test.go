package ha

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOpenTelemetryInstruments(t *testing.T) {
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTextMapPropagator(previousPropagator)
	})
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	spanExporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(spanExporter)),
	)
	t.Cleanup(func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
		if err := tracerProvider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})

	connector := &Connector{
		name:               "node-a",
		replicationID:      "example",
		publisher:          &observabilityTestPublisher{},
		subscriber:         &observabilityTestSubscriber{},
		leaderProvider:     &StaticLeader{},
		otelMeterProvider:  meterProvider,
		otelTracerProvider: tracerProvider,
	}
	connector.started.Store(true)
	telemetry, err := newOpenTelemetry(connector)
	if err != nil {
		t.Fatalf("create OpenTelemetry instruments: %v", err)
	}
	t.Cleanup(func() {
		if err := telemetry.close(); err != nil {
			t.Errorf("unregister metric callback: %v", err)
		}
	})

	publisher := &telemetryPublisher{
		publisher: &NoopPublisher{},
		telemetry: telemetry,
		node:      connector.name,
		id:        connector.replicationID,
	}
	if err := publisher.Publish(context.TODO(), &ChangeSet{Changes: []Change{{}}}); err != nil {
		t.Fatalf("publish changeset: %v", err)
	}
	changeset := &ChangeSet{Changes: []Change{{}}}
	if err := publisher.Publish(context.TODO(), changeset); err != nil {
		t.Fatalf("publish changeset with trace context: %v", err)
	}
	if changeset.TraceContext["traceparent"] == "" {
		t.Error("published changeset does not contain propagated trace context")
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	metricNames := make(map[string]bool)
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			metricNames[item.Name] = true
		}
	}
	for _, name := range []string{
		"go_ha.replication.changesets.published",
		"go_ha.cluster.leader",
		"go_ha.replication.lag",
	} {
		if !metricNames[name] {
			t.Errorf("collected metrics do not include %q", name)
		}
	}

	spans := spanExporter.GetSpans()
	if len(spans) != 2 || spans[0].Name != "go_ha.replication.publish" {
		t.Errorf("exported spans = %+v, want two replication publish spans", spans)
	}
}
