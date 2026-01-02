package grpc

import "google.golang.org/grpc"

type Server struct {
	*grpc.Server
	ReferenceCount int
}
