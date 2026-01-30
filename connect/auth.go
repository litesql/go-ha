package grpc

import (
	"context"
	"errors"

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
			req.Header().Set(tokenHeader, i.token)
		} else if req.Header().Get(tokenHeader) != i.token {
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
		if conn.RequestHeader().Get(tokenHeader) != i.token {
			return connect.NewError(connect.CodeUnauthenticated, errInvalidToken)
		}
		return next(ctx, conn)
	}
}
