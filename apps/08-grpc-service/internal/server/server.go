// Package server serves the inventory API over gRPC.
package server

//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.73.0 lint ../..
//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.73.0 generate ../.. --template ../../buf.gen.yaml --output ../..

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	inventoryv1 "github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/gen/inventory/v1"
	"github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/internal/inventory"
)

// Inventory is the stock storage behind the inventory service.
type Inventory interface {
	Get(ctx context.Context, sku string) (inventory.Item, error)
	List(ctx context.Context, f inventory.Filter) ([]inventory.Item, error)
	Reserve(ctx context.Context, id string, lines []inventory.Line) (inventory.Reservation, error)
}

// Server is a gRPC server exposing the inventory service, health checks and reflection.
type Server struct {
	grpc   *grpc.Server
	health *health.Server
}

// New returns a server backed by inv. Unary calls that arrive without a
// deadline are cancelled after defaultTimeout.
func New(logger *slog.Logger, inv Inventory, defaultTimeout time.Duration) *Server {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			unaryLogging(logger),
			unaryRecovery(logger),
			unaryDefaultTimeout(defaultTimeout),
		),
		grpc.ChainStreamInterceptor(
			streamLogging(logger),
			streamRecovery(logger),
		),
	)
	hs := health.NewServer()

	inventoryv1.RegisterInventoryServiceServer(srv, &service{inv: inv})
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)

	hs.SetServingStatus(inventoryv1.InventoryService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	return &Server{grpc: srv, health: hs}
}

// Serve accepts connections on lis until the server is shut down.
// It returns nil after Shutdown, even when Shutdown ran before Serve.
func (s *Server) Serve(lis net.Listener) error {
	if err := s.grpc.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serve grpc: %w", err)
	}
	return nil
}

// Shutdown reports NOT_SERVING to health checks, stops accepting new calls and
// waits for in-flight calls to finish. If ctx is done first, the remaining
// calls are cancelled and ctx's error is returned.
func (s *Server) Shutdown(ctx context.Context) error {
	s.health.Shutdown()

	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.grpc.Stop()
		<-stopped
		return fmt.Errorf("graceful stop: %w", ctx.Err())
	}
}
