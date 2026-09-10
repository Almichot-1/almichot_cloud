package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TransportSecurityConfig configures transport-level security for gRPC clients and servers.
type TransportSecurityConfig struct {
	EnableTLS bool
	CertFile  string
	KeyFile   string
	CAFile    string
}

// NewServerTLSConfig builds an mTLS tls.Config for gRPC servers requiring valid client certs (§21.1, G-35).
func NewServerTLSConfig(caCertPEM []byte, serverCert tls.Certificate) (*tls.Config, error) {
	if len(caCertPEM) == 0 {
		return nil, errors.New("CA certificate PEM cannot be empty")
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCertPEM) {
		return nil, errors.New("failed to append CA certificate to pool")
	}

	return &tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// NewClientTLSConfig builds an mTLS tls.Config for gRPC clients presenting a client cert.
func NewClientTLSConfig(caCertPEM []byte, clientCert tls.Certificate, serverName string) (*tls.Config, error) {
	if len(caCertPEM) == 0 {
		return nil, errors.New("CA certificate PEM cannot be empty")
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCertPEM) {
		return nil, errors.New("failed to append CA certificate to pool")
	}

	return &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientDialOptions returns the centralized dial options for gRPC clients across the cluster.
func ClientDialOptions(extraOpts ...grpc.DialOption) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	return append(opts, extraOpts...)
}

// ClientDialOptionsWithTLS returns dial options with mTLS credentials.
func ClientDialOptionsWithTLS(tlsCfg *tls.Config, extraOpts ...grpc.DialOption) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	}
	return append(opts, extraOpts...)
}

// NewClientConn creates a gRPC client connection using the default transport credentials.
func NewClientConn(target string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, ClientDialOptions(extraOpts...)...)
}

// NewClientConnWithTLS creates a gRPC client connection with mutual TLS transport credentials.
func NewClientConnWithTLS(target string, tlsCfg *tls.Config, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if tlsCfg == nil {
		return nil, fmt.Errorf("tls configuration cannot be nil")
	}
	return grpc.NewClient(target, ClientDialOptionsWithTLS(tlsCfg, extraOpts...)...)
}

// ServerOptions returns the centralized server options for gRPC servers.
func ServerOptions(extraOpts ...grpc.ServerOption) []grpc.ServerOption {
	return extraOpts
}

// ServerOptionsWithTLS returns server options with mutual TLS credentials.
func ServerOptionsWithTLS(tlsCfg *tls.Config, extraOpts ...grpc.ServerOption) []grpc.ServerOption {
	if tlsCfg == nil {
		return extraOpts
	}
	opts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(tlsCfg)),
	}
	return append(opts, extraOpts...)
}
