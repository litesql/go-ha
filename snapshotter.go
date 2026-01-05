package ha

import (
	"bytes"
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

type SequenceProvider interface {
	LatestSeq() uint64
}

type NoopSnapshotter struct {
	seq uint64
}

func (s *NoopSnapshotter) SetDB(_ *sql.DB) {}

func (s *NoopSnapshotter) Start() {}

func (s *NoopSnapshotter) TakeSnapshot(ctx context.Context) (sequence uint64, err error) {
	s.seq++
	return s.seq, nil
}

func (s *NoopSnapshotter) LatestSnapshot(ctx context.Context) (uint64, io.ReadCloser, error) {
	return s.seq, io.NopCloser(bytes.NewReader([]byte{})), nil
}

func NewNoopSnapshotter() *NoopSnapshotter {
	return &NoopSnapshotter{}
}

type BackupFn func(context.Context, *sql.DB, io.Writer) error

type NATSSnapshotter struct {
	started           bool
	backupFn          BackupFn
	objectStore       jetstream.ObjectStore
	seqProvider       SequenceProvider
	objectName        string
	latestSnapshotSeq uint64
	db                *sql.DB
	interval          time.Duration
	mu                sync.Mutex
}

func NewNATSSnapshotter(ctx context.Context, nc *nats.Conn, replicas int, stream string, db *sql.DB, backupFn BackupFn, interval time.Duration, sequenceProvider SequenceProvider, objectName string) (*NATSSnapshotter, error) {
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
	s := &NATSSnapshotter{
		backupFn:    backupFn,
		objectStore: objectStore,
		seqProvider: sequenceProvider,
		objectName:  objectName,
		db:          db,
		interval:    interval,
	}
	s.latestSnapshotSeq, _ = s.LatestSnapshotSequence(ctx)
	return s, nil
}

func (s *NATSSnapshotter) SetDB(db *sql.DB) {
	s.db = db
}

func (s *NATSSnapshotter) Start() {
	if s.interval <= 0 {
		return
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	go func() {
		ticker := time.NewTicker(s.interval)
		for {
			sequence, err := s.TakeSnapshot(context.Background())
			if err != nil {
				slog.Error("failed to take snapshot", "error", err)
			} else if sequence > 0 {
				slog.Debug("snapshot taken", "sequence", sequence)
			}
			<-ticker.C
		}
	}()

}

func (s *NATSSnapshotter) TakeSnapshot(ctx context.Context) (sequence uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sequence = s.seqProvider.LatestSeq()
	if sequence <= s.latestSnapshotSeq {
		return 0, nil
	}
	headers := make(nats.Header)
	headers.Set("seq", fmt.Sprint(sequence))
	bkpFile := fmt.Sprintf("bkp_%s_%d", s.objectName, time.Now().Nanosecond())
	if err := s.objectStore.UpdateMeta(ctx, s.objectName, jetstream.ObjectMeta{
		Name: bkpFile,
	}); err != nil && !errors.Is(err, jetstream.ErrUpdateMetaDeleted) {
		return 0, err
	}
	defer func() {
		if err == nil {
			s.objectStore.Delete(ctx, bkpFile)
		} else {
			s.objectStore.UpdateMeta(ctx, bkpFile, jetstream.ObjectMeta{
				Name: s.objectName,
			})
		}
	}()

	reader, writer := io.Pipe()
	errReaderCh := make(chan error, 1)
	errWriterCh := make(chan error, 1)
	go func() {
		errWriterCh <- s.backupFn(ctx, s.db, writer)
	}()

	go func() {
		info, err := s.objectStore.Put(ctx, jetstream.ObjectMeta{
			Name:    s.objectName,
			Headers: headers,
		}, reader)
		if err != nil {
			errReaderCh <- err
		} else {
			s.latestSnapshotSeq = sequence
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

func (s *NATSSnapshotter) LatestSnapshot(ctx context.Context) (uint64, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.objectStore.GetInfo(ctx, s.objectName)
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

	reader, err := s.objectStore.Get(ctx, s.objectName)
	return sequence, reader, err
}

func (s *NATSSnapshotter) LatestSnapshotSequence(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.objectStore.GetInfo(ctx, s.objectName)
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
