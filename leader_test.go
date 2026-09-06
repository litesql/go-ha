package ha

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The leader election bucket keeps the only copy of the redirect target, so it
// must be replicated across the cluster. A single replica bucket is lost with
// the node that created it and the new leader cannot read the redirect key.
func TestLeaderElectionBucketReplicas(t *testing.T) {
	const (
		clusterSize = 3
		replicas    = 3
		subject     = "replication.leader_election_test"
		bucket      = "replication_leader_election_test"
	)

	nc := startNATSCluster(t, clusterSize)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := startLeaderElection(ctx, "127.0.0.1:5000", nc, subject, clusterSize, replicas,
		filepath.Join(t.TempDir(), "ha-election.log"))
	if err != nil {
		t.Fatalf("failed to start leader election: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("failed to create jetstream context: %v", err)
	}
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		t.Fatalf("failed to get %q bucket: %v", bucket, err)
	}
	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatalf("failed to get %q bucket status: %v", bucket, err)
	}
	if got := status.Config().Replicas; got != replicas {
		t.Errorf("bucket replicas = %d, want %d", got, replicas)
	}
}

// startNATSCluster starts an in-process JetStream cluster and returns a
// connection to its first server.
func startNATSCluster(t *testing.T, size int) *nats.Conn {
	t.Helper()

	routes := make([]*url.URL, 0, size)
	clusterPorts := make([]int, size)
	for i := range clusterPorts {
		clusterPorts[i] = freePort(t)
		routes = append(routes, &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", clusterPorts[i])})
	}

	servers := make([]*server.Server, size)
	for i := range servers {
		opts := server.Options{
			ServerName: fmt.Sprintf("node%d", i+1),
			Host:       "127.0.0.1",
			Port:       -1,
			JetStream:  true,
			StoreDir:   t.TempDir(),
			NoLog:      true,
			NoSigs:     true,
			Cluster: server.ClusterOpts{
				Name: "test-cluster",
				Host: "127.0.0.1",
				Port: clusterPorts[i],
			},
			Routes: routes,
		}
		ns, err := server.NewServer(&opts)
		if err != nil {
			t.Fatalf("failed to create NATS server: %v", err)
		}
		go ns.Start()
		t.Cleanup(ns.Shutdown)
		servers[i] = ns
	}

	for _, ns := range servers {
		if !ns.ReadyForConnections(20 * time.Second) {
			t.Fatalf("NATS server %q is not ready for connections", ns.Name())
		}
	}
	waitForJetStreamCluster(t, servers)

	nc, err := nats.Connect(servers[0].ClientURL())
	if err != nil {
		t.Fatalf("failed to connect to NATS server: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func waitForJetStreamCluster(t *testing.T, servers []*server.Server) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, ns := range servers {
			if !ns.JetStreamEnabled() || !ns.JetStreamIsCurrent() {
				ready = false
				break
			}
			// only the meta leader knows every peer
			if ns.JetStreamIsLeader() && len(ns.JetStreamClusterPeers()) < len(servers) {
				ready = false
				break
			}
		}
		if ready && metaLeader(servers) != nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("JetStream cluster is not ready")
}

func metaLeader(servers []*server.Server) *server.Server {
	for _, ns := range servers {
		if ns.JetStreamIsLeader() {
			return ns
		}
	}
	return nil
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
