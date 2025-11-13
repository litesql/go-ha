package ha

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const redirectKey = "redirect"

type StaticLeader struct {
	Target string
}

func (s *StaticLeader) IsLeader() bool {
	return s.Target == ""
}

func (s *StaticLeader) RedirectTarget() string {
	return s.Target
}

func (s *StaticLeader) Ready() chan struct{} {
	c := make(chan struct{}, 1)
	c <- struct{}{}
	return c
}

type DynamicLeader struct {
	node        *graft.Node
	readyCh     chan struct{}
	localTarget string
	target      string
}

func (d *DynamicLeader) Ready() chan struct{} {
	return d.readyCh
}

func (d *DynamicLeader) IsLeader() bool {
	return d.node.State() == graft.LEADER && d.target == d.localTarget
}

func (d *DynamicLeader) RedirectTarget() string {
	return d.target
}

func (d *DynamicLeader) setTarget(newTarget string) {
	if d.target == "" && newTarget != "" {
		go func() {
			d.readyCh <- struct{}{}
		}()
	}
	if newTarget != d.target {
		slog.Info("leader election has a new target", "old", d.target, "new", newTarget, "leader", d.node.Leader())
	}
	d.target = newTarget
}

func startLeaderElection(ctx context.Context, redirectTarget string, nc *nats.Conn, subject string, clusterSize int, logFile string) (*DynamicLeader, error) {
	rpc, err := graft.NewNatsRpcFromConn(nc)
	if err != nil {
		return nil, err
	}
	errChan := make(chan error)
	stateChangeChan := make(chan graft.StateChange)
	handler := graft.NewChanHandler(stateChangeChan, errChan)
	clusterName := strings.ReplaceAll(subject, ".", "_")

	node, err := graft.New(graft.ClusterInfo{
		Name: clusterName,
		Size: clusterSize,
	}, handler, rpc, logFile)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: clusterName,
		TTL:    1 * time.Minute,
	})
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketExists) {
			kv, err = js.KeyValue(ctx, clusterName)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	dl := DynamicLeader{
		node:        node,
		localTarget: redirectTarget,
		readyCh:     make(chan struct{}, 1),
	}

	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case sc := <-stateChangeChan:
				if sc.To == graft.LEADER {
					slog.Info("i am the leader")
					_, err := kv.PutString(ctx, redirectKey, redirectTarget)
					if err != nil {
						slog.Error("leader failed to store redirect target", "error", err)
						continue
					}
					dl.setTarget(redirectTarget)
					continue
				}
				if sc.From == graft.LEADER {
					slog.Info("i am not the leader anymore")
				}
				redirect, err := kv.Get(ctx, redirectKey)
				if err != nil {
					slog.Error("failed to get redirect target", "error", err)
					dl.setTarget("")
					continue
				}
				dl.setTarget(string(redirect.Value()))
			case err := <-errChan:
				slog.Error("leader election", "error", err)
			case <-ticker.C:
				redirect, err := kv.Get(ctx, redirectKey)
				if err != nil {
					slog.Error("failed to get redirect target", "error", err)
					if errors.Is(err, jetstream.ErrKeyNotFound) && node.State() == graft.LEADER {
						_, err := kv.PutString(ctx, redirectKey, redirectTarget)
						if err != nil {
							slog.Error("leader failed to store redirect target", "error", err)
							continue
						}
						dl.setTarget(redirectTarget)
						continue
					}
					dl.setTarget("")
					continue
				}
				dl.setTarget(string(redirect.Value()))
			case <-ctx.Done():
				node.Close()
				return
			}
		}
	}()
	return &dl, nil
}
