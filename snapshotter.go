package ha

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const latestSnapshotName = "latest"

var latestSnapshotSeq uint64

type SequenceProvider interface {
	LatestSeq() uint64
}

type snapshotter struct {
	objectStore jetstream.ObjectStore
	seqProvider SequenceProvider
	mu          sync.Mutex
}

func newSnapshotter(ctx context.Context, nc *nats.Conn, replicas int, stream string, db *sql.DB, interval time.Duration) (*snapshotter, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	bucketName := stream + "_SNAPSHOTS"
	objectStore, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      bucketName,
		Storage:     jetstream.FileStorage,
		Compression: true,
		Replicas:    replicas,
	})
	if err != nil {
		if !errors.Is(err, jetstream.ErrBucketExists) {
			return nil, err
		}
		objectStore, err = js.ObjectStore(ctx, bucketName)
		if err != nil {
			return nil, err
		}
	}
	s := &snapshotter{
		objectStore: objectStore,
	}
	latestSnapshotSeq, _ = s.LatestSnapshotSequence(ctx)
	if interval > 0 {
		go s.start(ctx, db, interval)
	}
	return s, nil
}

func (s *snapshotter) SetSeqProvider(p SequenceProvider) {
	s.seqProvider = p
}

func (s *snapshotter) start(ctx context.Context, db *sql.DB, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			sequence, err := s.TakeSnapshot(ctx, db)
			if err != nil {
				slog.Error("failed to take snapshot", "error", err)
			} else if sequence > 0 {
				slog.Debug("snapshot taken", "sequence", sequence)
			}
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (s *snapshotter) TakeSnapshot(ctx context.Context, db *sql.DB) (sequence uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sequence = s.seqProvider.LatestSeq()
	if sequence <= latestSnapshotSeq {
		return 0, nil
	}
	headers := make(nats.Header)
	headers.Set("seq", fmt.Sprint(sequence))
	bkpFile := fmt.Sprintf("bkp_%d", time.Now().Nanosecond())
	if err := s.objectStore.UpdateMeta(ctx, latestSnapshotName, jetstream.ObjectMeta{
		Name: bkpFile,
	}); err != nil && !errors.Is(err, jetstream.ErrUpdateMetaDeleted) {
		return 0, err
	}
	defer func() {
		if err == nil {
			s.objectStore.Delete(ctx, bkpFile)
		} else {
			s.objectStore.UpdateMeta(ctx, bkpFile, jetstream.ObjectMeta{
				Name: latestSnapshotName,
			})
		}
	}()

	reader, writer := io.Pipe()
	errReaderCh := make(chan error, 1)
	errWriterCh := make(chan error, 1)
	go func() {
		errWriterCh <- Backup(ctx, db, writer)
	}()

	go func() {
		info, err := s.objectStore.Put(ctx, jetstream.ObjectMeta{
			Name:    latestSnapshotName,
			Headers: headers,
		}, reader)
		if err != nil {
			errReaderCh <- err
		} else {
			latestSnapshotSeq = sequence
			slog.Debug("snapshot stored", "bucket", info.Bucket, "name", info.Name, "size", info.Size, "modTime", info.ModTime)
			errReaderCh <- nil
		}
	}()

	select {
	case err2 := <-errWriterCh:
		if err2 != nil {
			writer.CloseWithError(err2)
			err = errors.Join(err, err2)
		} else {
			writer.Close()
		}
		select {
		case err2 := <-errReaderCh:
			err = errors.Join(err, err2)
		case <-ctx.Done():
			err = errors.Join(err, ctx.Err())
		}
	case err2 := <-errReaderCh:
		err = errors.Join(err, err2)
		select {
		case err2 := <-errWriterCh:
			if err2 != nil {
				writer.CloseWithError(err2)
				err = errors.Join(err, err2)
			} else {
				writer.Close()
			}
		case <-ctx.Done():
			err = errors.Join(err, ctx.Err())
		}
	case <-ctx.Done():
		err = ctx.Err()
	}

	return
}

func (s *snapshotter) LatestSnapshot(ctx context.Context) (uint64, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.objectStore.GetInfo(ctx, latestSnapshotName)
	if err != nil {
		return 0, nil, err
	}
	sequenceStr := info.Headers.Get("seq")
	var sequence uint64
	if sequenceStr != "" {
		sequence, err = strconv.ParseUint(sequenceStr, 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("convert sequence header: %w", err)
		}
	}

	reader, err := s.objectStore.Get(ctx, latestSnapshotName)
	return sequence, reader, err
}

func (s *snapshotter) LatestSnapshotSequence(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.objectStore.GetInfo(ctx, latestSnapshotName)
	if err != nil {
		return 0, err
	}
	sequenceStr := info.Headers.Get("seq")
	var sequence uint64
	if sequenceStr != "" {
		sequence, err = strconv.ParseUint(sequenceStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("convert sequence header: %w", err)
		}
	}
	return sequence, err
}
