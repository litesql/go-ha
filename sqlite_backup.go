package ha

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
)

func BackupWithAdapter[T any](
	ctx context.Context,
	db *sql.DB,
	w io.Writer,
	serialize func(rawConn T) ([]byte, error),
	backupToFile func(ctx context.Context, rawConn T, destName string) error,
) (retErr error) {
	telemetry, err := defaultSQLiteTelemetry()
	if err != nil {
		return fmt.Errorf("initialize SQLite telemetry: %w", err)
	}
	ctx, finish := telemetry.startBackup(ctx)
	var written int64
	defer func() { finish(retErr, written) }()

	srcConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer srcConn.Close()

	sqliteSrcConn, err := ExtractRawConn[T](srcConn)
	if err != nil {
		return err
	}

	var isMemDB bool
	if fnGetter, ok := any(sqliteSrcConn).(interface{ GetFilename(string) string }); ok {
		isMemDB = fnGetter.GetFilename("") == ""
	} else {
		var filename string
		if err := srcConn.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name = ?", "main").Scan(&filename); err != nil {
			return err
		}
		isMemDB = filename == ""
	}

	if isMemDB {
		data, err := serialize(sqliteSrcConn)
		if err != nil {
			return err
		}
		n, writeErr := w.Write(data)
		written = int64(n)
		if writeErr == nil && n != len(data) {
			writeErr = io.ErrShortWrite
		}
		return writeErr
	}

	dest, err := os.CreateTemp("", "ha-*.db")
	if err != nil {
		return err
	}
	destName := dest.Name()
	defer os.Remove(destName)
	if err := dest.Close(); err != nil {
		return err
	}

	if err := backupToFile(ctx, sqliteSrcConn, destName); err != nil {
		return err
	}

	final, err := os.Open(destName)
	if err != nil {
		return err
	}
	defer final.Close()

	written, err = io.Copy(w, final)
	return err
}
