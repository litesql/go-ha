package grpc

import "net/http"

type Server struct {
	*http.Server
	ReferenceCount int
}
