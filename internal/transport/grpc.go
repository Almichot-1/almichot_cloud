package transport

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TransportSecurityConfig configures transport-level security for gRPC clients and servers.
type TransportSecurityConfig struct {
	EnableTLS bool
	CertFile  string
	KeyFile   string
	CAFile    string
}

// ClientDialOptions returns the centralized dial options for gRPC clients across the cluster.
// In Sprint 2.1 (mTLS), this constructor injects mutual TLS credentials using the cluster CA.
func ClientDialOptions(extraOpts ...grpc.DialOption) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	return append(opts, extraOpts...)
}

// NewClientConn creates a gRPC client connection using the centralized transport credentials.
func NewClientConn(target string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, ClientDialOptions(extraOpts...)...)
}

// ServerOptions returns the centralized server options for gRPC servers.
// In Sprint 2.1 (mTLS), this constructor configures server-side TLS and client CA validation.
func ServerOptions(extraOpts ...grpc.ServerOption) []grpc.ServerOption {
	return extraOpts
}
