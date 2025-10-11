package ha

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type natsClientServer struct {
	server *server.Server
	client *nats.Conn
	count  int
}

func runEmbeddedNATSServer(cfg EmbeddedNatsConfig) (*natsClientServer, error) {
	var (
		opts *server.Options
		err  error
	)
	if cfg.File != "" {
		opts, err = server.ProcessConfigFile(cfg.File)
		if err != nil {
			return nil, fmt.Errorf("failed to process nats config file: %w", err)
		}
	} else {
		opts = &server.Options{
			ServerName: cfg.Name,
			Port:       cfg.Port,
			StoreDir:   cfg.StoreDir,
		}
		if cfg.User != "" && cfg.Pass != "" {
			appAcct := server.NewAccount("app")
			appAcct.EnableJetStream(map[string]server.JetStreamAccountLimits{
				"": {
					MaxMemory:    -1,
					MaxStore:     -1,
					MaxStreams:   -1,
					MaxConsumers: -1,
				},
			})
			opts.Accounts = []*server.Account{appAcct}
			opts.Users = []*server.User{
				{
					Username: cfg.User,
					Password: cfg.Pass,
					Account:  appAcct,
				},
			}
		}
	}
	opts.JetStream = true
	opts.DisableJetStreamBanner = true

	for _, ncs := range natsClientServers {
		addr := ncs.server.Addr()
		if addr == nil {
			continue
		}
		if tcpAddr, ok := addr.(*net.TCPAddr); ok {
			if opts.Port == tcpAddr.Port {
				slog.Warn("reusing existed NATS server", "port", tcpAddr.Port)
				return ncs, nil
			}
		}
	}

	if opts.Cluster.Name != "" && opts.ServerName == "" {
		opts.ServerName, err = os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("failed to get hostname for nats server name: %w", err)
		}
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}
	if cfg.EnableLogs {
		ns.ConfigureLogger()
	}

	go ns.Start()

	slog.Info("starting HA embedded NATS server", "port", opts.Port, "store_dir", ns.StoreDir())

	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, fmt.Errorf("embedded NATS is not ready for connections")
	}

	if ns.ClusterName() != "" {
		// wait for the leader
		waitForLeader(ns, opts.Cluster.PoolSize)
	}

	slog.Debug("embedded NATS server is ready", "cluster", ns.ClusterName(), "jetstream", ns.JetStreamEnabled())
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		ns.Shutdown()
		return nil, err
	}
	return &natsClientServer{
		server: ns,
		client: nc,
	}, nil
}

func waitForLeader(ns *server.Server, size int) {
	for {
		raftz := ns.Raftz(&server.RaftzOptions{})
		if raftz != nil {
			for _, raftGroup := range *raftz {
				for _, raft := range raftGroup {
					if raft.Leader != "" {
						return
					}
				}
			}
		}
		slog.Debug("waiting for the cluster leader", "peers", len(ns.JetStreamClusterPeers()), "size", size)
		time.Sleep(500 * time.Millisecond)
	}
}

var natsIdentifierNormalizer = regexp.MustCompile(`[.\/\s*>*]`)

func normalizeNatsIdentifier(name string) string {
	s := natsIdentifierNormalizer.ReplaceAllString(name, "_")
	s = strings.Trim(s, "_")
	if len(s) > 32 {
		return s[len(s)-32:]
	}
	return s
}
