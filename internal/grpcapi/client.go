package grpcapi

import (
	"crypto/tls"

	"github.com/nebula/nebula/internal/transport"
	"google.golang.org/grpc"
)

// ClientDialOptions delegates to the centralized transport package.
func ClientDialOptions(extraOpts ...grpc.DialOption) []grpc.DialOption {
	return transport.ClientDialOptions(extraOpts...)
}

// ClientDialOptionsWithTLS delegates to the centralized transport package with mTLS.
func ClientDialOptionsWithTLS(tlsCfg *tls.Config, extraOpts ...grpc.DialOption) []grpc.DialOption {
	return transport.ClientDialOptionsWithTLS(tlsCfg, extraOpts...)
}

// NewClientConn delegates to the centralized transport package.
func NewClientConn(target string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return transport.NewClientConn(target, extraOpts...)
}

// NewClientConnWithTLS delegates to the centralized transport package with mTLS.
func NewClientConnWithTLS(target string, tlsCfg *tls.Config, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return transport.NewClientConnWithTLS(target, tlsCfg, extraOpts...)
}

// ServerOptions delegates to the centralized transport package.
func ServerOptions(extraOpts ...grpc.ServerOption) []grpc.ServerOption {
	return transport.ServerOptions(extraOpts...)
}

// ServerOptionsWithTLS delegates to the centralized transport package with mTLS.
func ServerOptionsWithTLS(tlsCfg *tls.Config, extraOpts ...grpc.ServerOption) []grpc.ServerOption {
	return transport.ServerOptionsWithTLS(tlsCfg, extraOpts...)
}
