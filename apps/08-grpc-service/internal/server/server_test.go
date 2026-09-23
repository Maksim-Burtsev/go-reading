package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	inventoryv1 "github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/gen/inventory/v1"
	"github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/internal/inventory"
	"github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/internal/server"
)

type blockingInventory struct {
	server.Inventory

	started chan struct{}
}

func (b blockingInventory) Reserve(ctx context.Context, _ string, _ []inventory.Line) (inventory.Reservation, error) {
	if b.started != nil {
		close(b.started)
	}
	<-ctx.Done()
	return inventory.Reservation{}, ctx.Err()
}

type panickingInventory struct{ server.Inventory }

func (panickingInventory) Get(context.Context, string) (inventory.Item, error) {
	panic("store corrupted")
}

func (panickingInventory) List(context.Context, inventory.Filter) ([]inventory.Item, error) {
	panic("store corrupted")
}

func newStore(t *testing.T) *inventory.Store {
	t.Helper()
	s, err := inventory.NewStore([]inventory.Item{
		{SKU: "BOOK-1", Name: "Book one", Available: 3},
		{SKU: "BOOK-2", Name: "Book two", Available: 0},
		{SKU: "MUG-1", Name: "Mug", Available: 10},
	}, time.Now)
	require.NoError(t, err)
	return s
}

func start(t *testing.T, inv server.Inventory, defaultTimeout time.Duration) (*server.Server, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := server.New(slog.New(slog.NewJSONHandler(io.Discard, nil)), inv, defaultTimeout)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, srv.Shutdown(ctx))
		require.NoError(t, <-serveErr)
	})
	return srv, conn
}

func dial(t *testing.T, inv server.Inventory, defaultTimeout time.Duration) inventoryv1.InventoryServiceClient {
	t.Helper()
	_, conn := start(t, inv, defaultTimeout)
	return inventoryv1.NewInventoryServiceClient(conn)
}

func collect(stream grpc.ServerStreamingClient[inventoryv1.ListItemsResponse]) ([]string, error) {
	var skus []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return skus, nil
		}
		if err != nil {
			return skus, err
		}
		skus = append(skus, resp.GetItem().GetSku())
	}
}

func TestGetItem(t *testing.T) {
	t.Parallel()
	client := dial(t, newStore(t), time.Second)

	tests := []struct {
		name     string
		sku      string
		wantCode codes.Code
		want     *inventoryv1.Item
	}{
		{name: "found", sku: "BOOK-1", want: &inventoryv1.Item{Sku: "BOOK-1", Name: "Book one", Available: 3}},
		{name: "empty sku", sku: "", wantCode: codes.InvalidArgument},
		{name: "unknown sku", sku: "NOPE", wantCode: codes.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp, err := client.GetItem(t.Context(), &inventoryv1.GetItemRequest{Sku: tt.sku})
			require.Equal(t, tt.wantCode, status.Code(err), "error: %v", err)
			if tt.wantCode != codes.OK {
				return
			}
			require.True(t, proto.Equal(tt.want, resp.GetItem()), "got %v", resp.GetItem())
		})
	}
}

func TestListItems(t *testing.T) {
	t.Parallel()
	client := dial(t, newStore(t), time.Second)

	tests := []struct {
		name     string
		req      *inventoryv1.ListItemsRequest
		want     []string
		wantCode codes.Code
	}{
		{name: "all", req: &inventoryv1.ListItemsRequest{}, want: []string{"BOOK-1", "BOOK-2", "MUG-1"}},
		{name: "prefix", req: &inventoryv1.ListItemsRequest{SkuPrefix: "BOOK-"}, want: []string{"BOOK-1", "BOOK-2"}},
		{name: "in stock only", req: &inventoryv1.ListItemsRequest{InStockOnly: true}, want: []string{"BOOK-1", "MUG-1"}},
		{name: "limit", req: &inventoryv1.ListItemsRequest{Limit: 1}, want: []string{"BOOK-1"}},
		{name: "negative limit", req: &inventoryv1.ListItemsRequest{Limit: -1}, wantCode: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stream, err := client.ListItems(t.Context(), tt.req)
			require.NoError(t, err)

			got, err := collect(stream)
			require.Equal(t, tt.wantCode, status.Code(err), "error: %v", err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestReserve(t *testing.T) {
	t.Parallel()

	line := func(sku string, qty int64) *inventoryv1.ReservationLine {
		return &inventoryv1.ReservationLine{Sku: sku, Quantity: qty}
	}
	tests := []struct {
		name     string
		prior    *inventoryv1.ReserveRequest
		req      *inventoryv1.ReserveRequest
		wantCode codes.Code
	}{
		{
			name: "reserved",
			req:  &inventoryv1.ReserveRequest{ReservationId: "r1", Lines: []*inventoryv1.ReservationLine{line("BOOK-1", 2), line("MUG-1", 5)}},
		},
		{
			name:     "missing reservation id",
			req:      &inventoryv1.ReserveRequest{Lines: []*inventoryv1.ReservationLine{line("BOOK-1", 1)}},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "no lines",
			req:      &inventoryv1.ReserveRequest{ReservationId: "r1"},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "zero quantity",
			req:      &inventoryv1.ReserveRequest{ReservationId: "r1", Lines: []*inventoryv1.ReservationLine{line("BOOK-1", 0)}},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "unknown sku",
			req:      &inventoryv1.ReserveRequest{ReservationId: "r1", Lines: []*inventoryv1.ReservationLine{line("NOPE", 1)}},
			wantCode: codes.NotFound,
		},
		{
			name:     "insufficient stock",
			req:      &inventoryv1.ReserveRequest{ReservationId: "r1", Lines: []*inventoryv1.ReservationLine{line("BOOK-1", 4)}},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:  "replay with same lines",
			prior: &inventoryv1.ReserveRequest{ReservationId: "r1", Lines: []*inventoryv1.ReservationLine{line("BOOK-1", 1)}},
			req:   &inventoryv1.ReserveRequest{ReservationId: "r1", Lines: []*inventoryv1.ReservationLine{line("BOOK-1", 1)}},
		},
		{
			name:     "reused id with different lines",
			prior:    &inventoryv1.ReserveRequest{ReservationId: "r1", Lines: []*inventoryv1.ReservationLine{line("BOOK-1", 1)}},
			req:      &inventoryv1.ReserveRequest{ReservationId: "r1", Lines: []*inventoryv1.ReservationLine{line("BOOK-1", 2)}},
			wantCode: codes.AlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := dial(t, newStore(t), time.Second)
			if tt.prior != nil {
				_, err := client.Reserve(t.Context(), tt.prior)
				require.NoError(t, err)
			}

			resp, err := client.Reserve(t.Context(), tt.req)
			require.Equal(t, tt.wantCode, status.Code(err), "error: %v", err)
			if tt.wantCode == codes.OK {
				require.Equal(t, tt.req.GetReservationId(), resp.GetReservation().GetId())
				require.Len(t, resp.GetReservation().GetLines(), len(tt.req.GetLines()))
				require.NotNil(t, resp.GetReservation().GetCreateTime())
			}
		})
	}
}

func TestReserveReplayDoesNotReserveTwice(t *testing.T) {
	t.Parallel()
	client := dial(t, newStore(t), time.Second)
	req := &inventoryv1.ReserveRequest{
		ReservationId: "r1",
		Lines:         []*inventoryv1.ReservationLine{{Sku: "BOOK-1", Quantity: 2}},
	}

	first, err := client.Reserve(t.Context(), req)
	require.NoError(t, err)
	replay, err := client.Reserve(t.Context(), req)
	require.NoError(t, err)
	require.True(t, first.GetReservation().GetCreateTime().AsTime().Equal(replay.GetReservation().GetCreateTime().AsTime()))

	got, err := client.GetItem(t.Context(), &inventoryv1.GetItemRequest{Sku: "BOOK-1"})
	require.NoError(t, err)
	require.Equal(t, int64(1), got.GetItem().GetAvailable())
	require.Equal(t, int64(2), got.GetItem().GetReserved())
}

func TestDeadlines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		defaultTimeout time.Duration
		clientTimeout  time.Duration
	}{
		{name: "server default applies without client deadline", defaultTimeout: 50 * time.Millisecond},
		{name: "client deadline shorter than default", defaultTimeout: time.Hour, clientTimeout: 50 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := dial(t, blockingInventory{}, tt.defaultTimeout)
			ctx := t.Context()
			if tt.clientTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.clientTimeout)
				defer cancel()
			}

			_, err := client.Reserve(ctx, &inventoryv1.ReserveRequest{
				ReservationId: "r1",
				Lines:         []*inventoryv1.ReservationLine{{Sku: "BOOK-1", Quantity: 1}},
			})
			require.Equal(t, codes.DeadlineExceeded, status.Code(err), "error: %v", err)
		})
	}
}

func TestPanicIsRecovered(t *testing.T) {
	t.Parallel()
	_, conn := start(t, panickingInventory{}, time.Second)
	client := inventoryv1.NewInventoryServiceClient(conn)

	tests := []struct {
		name string
		call func(ctx context.Context) error
	}{
		{
			name: "unary",
			call: func(ctx context.Context) error {
				_, err := client.GetItem(ctx, &inventoryv1.GetItemRequest{Sku: "BOOK-1"})
				return err
			},
		},
		{
			name: "stream",
			call: func(ctx context.Context) error {
				stream, err := client.ListItems(ctx, &inventoryv1.ListItemsRequest{})
				if err != nil {
					return err
				}
				_, err = collect(stream)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(t.Context())
			require.Equal(t, codes.Internal, status.Code(err), "error: %v", err)
		})
	}

	health, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{
		Service: inventoryv1.InventoryService_ServiceDesc.ServiceName,
	})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, health.GetStatus())
}

func TestServeAfterShutdownReturnsNil(t *testing.T) {
	t.Parallel()
	srv := server.New(slog.New(slog.NewJSONHandler(io.Discard, nil)), newStore(t), time.Second)

	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, srv.Serve(bufconn.Listen(1<<20)))
}

func TestShutdownCancelsCallsAfterTimeout(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	srv, conn := start(t, blockingInventory{started: started}, time.Hour)
	client := inventoryv1.NewInventoryServiceClient(conn)

	callErr := make(chan error, 1)
	go func() {
		_, err := client.Reserve(t.Context(), &inventoryv1.ReserveRequest{
			ReservationId: "r1",
			Lines:         []*inventoryv1.ReservationLine{{Sku: "BOOK-1", Quantity: 1}},
		})
		callErr <- err
	}()
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, srv.Shutdown(ctx), context.DeadlineExceeded)
	require.Equal(t, codes.Unavailable, status.Code(<-callErr))
}
