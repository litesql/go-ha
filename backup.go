package ha

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
)

func Backup(ctx context.Context, db *sql.DB, w io.Writer) error {
	srcConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer srcConn.Close()

	sqliteSrcConn, err := sqliteConn(srcConn)
	if err != nil {
		return err
	}
	// memdb
	if sqliteSrcConn.GetFilename("") == "" {
		b, err := sqliteSrcConn.Serialize("")
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		return err
	}
	dest, err := os.CreateTemp("", "ha-*.db")
	if err != nil {
		return err
	}
	defer os.Remove(dest.Name())

	destDb, err := sql.Open("sqlite3", dest.Name())
	if err != nil {
		return err
	}
	defer destDb.Close()

	destConn, err := destDb.Conn(ctx)
	if err != nil {
		return err
	}
	defer destConn.Close()

	sqliteDestConn, err := sqliteConn(destConn)
	if err != nil {
		return err
	}

	bkp, err := sqliteDestConn.Backup("main", sqliteSrcConn, "main")
	if err != nil {
		return err
	}

	for more := true; more; {
		more, err = bkp.Step(-1)
		if err != nil {
			return fmt.Errorf("backup step error: %w", err)
		}
		if bkp.Remaining() == 0 {
			break
		}
	}

	err = bkp.Finish()
	if err != nil {
		return fmt.Errorf("backup finish error: %w", err)
	}

	err = bkp.Close()
	if err != nil {
		return fmt.Errorf("backup close error: %w", err)
	}

	err = dest.Close()
	if err != nil {
		return err
	}

	final, err := os.Open(dest.Name())
	if err != nil {
		return err
	}
	defer final.Close()

	_, err = io.Copy(w, final)
	return err
}
