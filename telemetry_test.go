package ha

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
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
	failedPublisher := &telemetryPublisher{
		publisher: failingTelemetryTestPublisher{},
		telemetry: telemetry,
		node:      connector.name,
		id:        connector.replicationID,
	}
	if err := failedPublisher.Publish(context.Background(), &ChangeSet{}); err == nil {
		t.Fatal("failed publish did not return an error")
	}

	connector.telemetry = telemetry
	response := httptest.NewRecorder()
	connector.ObservabilityHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, ReadinessPath, nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	metricNames := make(map[string]bool)
	counterTotals := make(map[string]int64)
	expectedCounters := map[string]int64{
		"go_ha.replication.changesets.published": 2,
		"go_ha.replication.publish.errors":       1,
		"go_ha.http.requests":                    1,
	}
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			metricNames[item.Name] = true
			if _, expected := expectedCounters[item.Name]; expected {
				sum, ok := item.Data.(metricdata.Sum[int64])
				if !ok {
					t.Errorf("metric %q data type = %T, want an integer sum", item.Name, item.Data)
					continue
				}
				for _, point := range sum.DataPoints {
					counterTotals[item.Name] += point.Value
				}
			}
		}
	}
	for _, name := range []string{
		"go_ha.replication.changesets.published",
		"go_ha.replication.publish.errors",
		"go_ha.cluster.leader",
		"go_ha.replication.lag",
		"go_ha.http.requests",
		"go_ha.http.request.duration",
	} {
		if !metricNames[name] {
			t.Errorf("collected metrics do not include %q", name)
		}
	}
	for name, want := range expectedCounters {
		if got := counterTotals[name]; got != want {
			t.Errorf("metric %q total = %d, want %d", name, got, want)
		}
	}

	spans := spanExporter.GetSpans()
	if len(spans) != 4 || spans[0].Name != "go_ha.replication.publish" || spans[3].Name != "go_ha.observability.http" {
		t.Errorf("exported spans = %+v, want three publish spans and one HTTP span", spans)
	}
	if len(spans) >= 3 && spans[2].Status.Code != codes.Error {
		t.Errorf("failed publish span status = %v, want error", spans[2].Status.Code)
	}
	if len(spans) >= 4 && spans[3].Status.Code != codes.Error {
		t.Errorf("HTTP readiness span status = %v, want error for HTTP 503", spans[3].Status.Code)
	}
}

type failingTelemetryTestPublisher struct{}

func (failingTelemetryTestPublisher) Publish(context.Context, *ChangeSet) error {
	return errors.New("publish failed")
}

func (failingTelemetryTestPublisher) Sequence() uint64 {
	return 0
}
