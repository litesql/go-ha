package ha

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type observabilityTestPublisher struct {
	NoopPublisher
	sequence uint64
}

func (p *observabilityTestPublisher) Sequence() uint64 {
	return p.sequence
}

type observabilityTestSubscriber struct {
	NoopSubscriber
	sequence uint64
}

func (s *observabilityTestSubscriber) LatestSeq() uint64 {
	return s.sequence
}

func TestObservabilityHandler(t *testing.T) {
	connector := &Connector{
		name:           "node-a",
		replicationID:  "example",
		publisher:      &observabilityTestPublisher{sequence: 10},
		subscriber:     &observabilityTestSubscriber{sequence: 7},
		leaderProvider: &StaticLeader{},
	}
	connector.started.Store(true)
	handler := connector.ObservabilityHandler()

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, HealthPath, nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", health.Code, http.StatusOK)
	}
	for _, field := range []string{
		`"leader":true`,
		`"published_sequence":10`,
		`"applied_sequence":7`,
		`"replication_lag_messages":3`,
		`"node-a":3`,
	} {
		if !strings.Contains(health.Body.String(), field) {
			t.Errorf("health response %q does not contain %q", health.Body.String(), field)
		}
	}

	readiness := httptest.NewRecorder()
	handler.ServeHTTP(readiness, httptest.NewRequest(http.MethodGet, ReadinessPath, nil))
	if readiness.Code != http.StatusServiceUnavailable {
		t.Errorf("readiness status = %d, want %d", readiness.Code, http.StatusServiceUnavailable)
	}

	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusNotFound {
		t.Errorf("metrics status = %d, want %d", metrics.Code, http.StatusNotFound)
	}
}

func TestObservabilityHandlersRequireGET(t *testing.T) {
	connector := &Connector{}
	for _, handler := range []http.Handler{
		connector.HealthHandler(),
		connector.ReadinessHandler(),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", nil))
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("handler status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
		}
	}
}
