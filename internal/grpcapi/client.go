package grpcapi

import (
	"github.com/nebula/nebula/internal/transport"
	"google.golang.org/grpc"
)

// ClientDialOptions delegates to the centralized transport package.
func ClientDialOptions(extraOpts ...grpc.DialOption) []grpc.DialOption {
	return transport.ClientDialOptions(extraOpts...)
}

// NewClientConn delegates to the centralized transport package.
func NewClientConn(target string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return transport.NewClientConn(target, extraOpts...)
}

// ServerOptions delegates to the centralized transport package.
func ServerOptions(extraOpts ...grpc.ServerOption) []grpc.ServerOption {
	return transport.ServerOptions(extraOpts...)
}
