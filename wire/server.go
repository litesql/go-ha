package wire

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/go-mysql-org/go-mysql/server"
)

type Server struct {
	ConnectorProvider ConnectorProvider
	Databases         Databases
	Port              int
	listener          net.Listener
	closed            bool
	ReferenceCount    int
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return err
	}
	s.listener = l

	go func() {
		slog.Info("MySQL server listening", "port", s.Port)
		mysqlServer := server.NewDefaultServer()
		for {
			c, err := l.Accept()
			if err != nil {
				if s.closed {
					return
				}
				slog.Error("Accept conn", "error", err)
				continue
			}

			go func(c net.Conn) {
				defer c.Close()

				slog.Debug("New mysql connection", "remote", c.RemoteAddr().String())
				conn, err := mysqlServer.NewConn(c, "root", "", &Handler{
					cp:   s.ConnectorProvider,
					list: s.Databases,
				})
				if err != nil {
					slog.Error("New conn", "error", err)
					return
				}
				for {
					if err := conn.HandleCommand(); err != nil {
						slog.Error("HandleCommand", "error", err)
						return
					}
				}
			}(c)
		}
	}()
	return nil
}

func (s *Server) Close() error {
	s.closed = true
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
