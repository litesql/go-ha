package ha

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"
)

const TXCookieName = "_txseq"

func (c *Connector) ForwardToLeader(timeout time.Duration, methods ...string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(c.ForwardToLeaderFunc(h.ServeHTTP, timeout, methods...))
	}
}

func (c *Connector) ForwardToLeaderFunc(h http.HandlerFunc, timeout time.Duration, methods ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if !slices.Contains(methods, r.Method) {
			h(w, r)
			return
		}

		isLeader := c.leaderProvider.IsLeader()
		if isLeader {
			h(c.ResponseWriter(w), r)
			return
		}

		target := c.leaderProvider.RedirectTarget()
		if target == "" {
			http.Error(w, "leader redirect URL not found", http.StatusInternalServerError)
			return
		}

		if r.URL.Query().Get("forward") == "false" {
			w.Header().Set("location", string(target))
			w.WriteHeader(http.StatusMovedPermanently)
			return
		}

		resp, err := forwardTo(target, r, timeout)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			for i, value := range v {
				if i == 0 {
					w.Header().Set(k, value)
					continue
				}
				w.Header().Add(k, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}

func (c *Connector) ResponseWriter(w http.ResponseWriter) http.ResponseWriter {
	return &responseWriter{
		ResponseWriter: w,
		pub:            c.publisher,
	}
}

func (c *Connector) ConsistentReader(timeout time.Duration, methods ...string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(c.ConsistentReaderFunc(h.ServeHTTP, timeout, methods...))
	}
}

func (c *Connector) ConsistentReaderFunc(h http.HandlerFunc, timeout time.Duration, methods ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if !slices.Contains(methods, r.Method) || c.leaderProvider.IsLeader() {
			h(w, r)
			return
		}

		var txSeq uint64
		if cookie, _ := r.Cookie(TXCookieName); cookie != nil {
			var err error
			txSeq, err = strconv.ParseUint(cookie.Value, 10, 64)
			if err != nil {
				slog.Warn("invalid cookie", "name", TXCookieName, "error", err)
				h(w, r)
				return
			}
		}

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
	LOOP:
		for {
			if c.LatestSeq() >= txSeq {
				break LOOP
			}

			select {
			case <-ctx.Done():
				if r.URL.Query().Get("forward") == "false" {
					http.Error(w, "cosistent reader timeout", http.StatusGatewayTimeout)
					return
				}
				target := c.leaderProvider.RedirectTarget()
				if target == "" {
					http.Error(w, "leader redirect URL not found", http.StatusInternalServerError)
					return
				}
				resp, err := forwardTo(target, r, timeout)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer resp.Body.Close()
				for k, v := range resp.Header {
					for i, value := range v {
						if i == 0 {
							w.Header().Set(k, value)
							continue
						}
						w.Header().Add(k, value)
					}
				}
				w.WriteHeader(resp.StatusCode)
				io.Copy(w, resp.Body)
				return
			case <-ticker.C:
			}
		}
		h(w, r)
	}
}

func forwardTo(addr string, req *http.Request, timeout time.Duration) (*http.Response, error) {
	newURL := addr + req.URL.Path + "?" + req.URL.RawQuery

	var buf bytes.Buffer
	defer req.Body.Close()
	_, err := io.Copy(&buf, req.Body)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	newReq, err := http.NewRequestWithContext(ctx, req.Method, newURL, &buf)
	if err != nil {
		return nil, err
	}
	for k, v := range req.Header {
		for i, value := range v {
			if i == 0 {
				newReq.Header.Set(k, value)
				continue
			}
			newReq.Header.Add(k, value)
		}
	}
	return http.DefaultClient.Do(newReq)
}

type responseWriter struct {
	http.ResponseWriter
	pub        Publisher
	statusCode int
	written    bool
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written && (rw.statusCode == 0 || (rw.statusCode >= 200 && rw.statusCode < 300)) {
		rw.setTxCookie()
	}
	return rw.ResponseWriter.Write(b)
}

func (rw *responseWriter) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
	if rw.statusCode >= 200 && rw.statusCode < 300 {
		rw.setTxCookie()
	}
	rw.written = true
	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *responseWriter) setTxCookie() {
	http.SetCookie(rw.ResponseWriter, &http.Cookie{
		Name:     TXCookieName,
		Value:    fmt.Sprint(rw.pub.Sequence()),
		Expires:  time.Now().Add(5 * time.Minute),
		HttpOnly: true,
	})
}
