package grpcapi

import (
	"fmt"
	"net"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// Server wraps a gRPC server and its network listener.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	log        zerolog.Logger
	addr       string
}

// NewServer creates a new gRPC server listening on the specified address.
func NewServer(addr string, log zerolog.Logger, registerFn func(s *grpc.Server), opts ...grpc.ServerOption) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer(ServerOptions(opts...)...)
	if registerFn != nil {
		registerFn(grpcServer)
	}

	return &Server{
		grpcServer: grpcServer,
		listener:   lis,
		log:        log.With().Str("component", "grpc-server").Logger(),
		addr:       lis.Addr().String(),
	}, nil
}

// Serve starts listening for incoming gRPC connections.
func (s *Server) Serve() error {
	s.log.Info().Str("addr", s.addr).Msg("gRPC server listening")
	return s.grpcServer.Serve(s.listener)
}

// GracefulStop gracefully stops the gRPC server.
func (s *Server) GracefulStop() {
	s.log.Info().Msg("gracefully stopping gRPC server")
	s.grpcServer.GracefulStop()
}

// Stop immediately stops the gRPC server.
func (s *Server) Stop() {
	s.grpcServer.Stop()
}

// Addr returns the network address the server is listening on.
func (s *Server) Addr() string {
	return s.addr
}
