package grpc

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"

	"connectrpc.com/connect"
)

const tokenHeader = "authorization"

var errInvalidToken = errors.New("invalid authorization token")

type authInterceptor struct {
	token string
}

func NewAuthInterceptor(token string) *authInterceptor {
	return &authInterceptor{
		token: token,
	}
}

func (i *authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(
		ctx context.Context,
		req connect.AnyRequest,
	) (connect.AnyResponse, error) {
		if req.Spec().IsClient {
			req.Header().Set(tokenHeader, "Bearer "+i.token)
		} else if !equalTokens(strings.TrimPrefix(req.Header().Get(tokenHeader), "Bearer "), i.token) {
			return nil, connect.NewError(connect.CodeUnauthenticated, errInvalidToken)
		}
		return next(ctx, req)
	}
}

func (i *authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(
		ctx context.Context,
		spec connect.Spec,
	) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set(tokenHeader, i.token)
		return conn
	}
}

func (i *authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(
		ctx context.Context,
		conn connect.StreamingHandlerConn,
	) error {
		if !equalTokens(strings.TrimPrefix(conn.RequestHeader().Get(tokenHeader), "Bearer "), i.token) {
			return connect.NewError(connect.CodeUnauthenticated, errInvalidToken)
		}
		return next(ctx, conn)
	}
}

func equalTokens(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
