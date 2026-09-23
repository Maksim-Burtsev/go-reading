package server

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	inventoryv1 "github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/gen/inventory/v1"
	"github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/internal/inventory"
)

const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

type service struct {
	inventoryv1.UnimplementedInventoryServiceServer

	inv Inventory
}

func (s *service) GetItem(ctx context.Context, req *inventoryv1.GetItemRequest) (*inventoryv1.GetItemResponse, error) {
	if req.GetSku() == "" {
		return nil, status.Error(codes.InvalidArgument, "sku is required")
	}
	item, err := s.inv.Get(ctx, req.GetSku())
	if err != nil {
		return nil, toStatus(err)
	}
	return &inventoryv1.GetItemResponse{Item: toProtoItem(item)}, nil
}

func (s *service) ListItems(req *inventoryv1.ListItemsRequest, stream grpc.ServerStreamingServer[inventoryv1.ListItemsResponse]) error {
	if req.GetLimit() < 0 {
		return status.Error(codes.InvalidArgument, "limit must not be negative")
	}
	limit := defaultListLimit
	if n := int(req.GetLimit()); n > 0 {
		limit = min(n, maxListLimit)
	}

	ctx := stream.Context()
	items, err := s.inv.List(ctx, inventory.Filter{
		SKUPrefix:   req.GetSkuPrefix(),
		InStockOnly: req.GetInStockOnly(),
		Limit:       limit,
	})
	if err != nil {
		return toStatus(err)
	}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return toStatus(err)
		}
		if err := stream.Send(&inventoryv1.ListItemsResponse{Item: toProtoItem(item)}); err != nil {
			return fmt.Errorf("send item %s: %w", item.SKU, err)
		}
	}
	return nil
}

func (s *service) Reserve(ctx context.Context, req *inventoryv1.ReserveRequest) (*inventoryv1.ReserveResponse, error) {
	lines := make([]inventory.Line, 0, len(req.GetLines()))
	for _, l := range req.GetLines() {
		lines = append(lines, inventory.Line{SKU: l.GetSku(), Quantity: l.GetQuantity()})
	}
	r, err := s.inv.Reserve(ctx, req.GetReservationId(), lines)
	if err != nil {
		return nil, toStatus(err)
	}
	return &inventoryv1.ReserveResponse{Reservation: toProtoReservation(r)}, nil
}

func (s *service) ReleaseReservation(ctx context.Context, req *inventoryv1.ReleaseReservationRequest) (*inventoryv1.ReleaseReservationResponse, error) {
	r, err := s.inv.Release(ctx, req.GetReservationId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &inventoryv1.ReleaseReservationResponse{Reservation: toProtoReservation(r)}, nil
}

func toStatus(err error) error {
	var code codes.Code
	switch {
	case errors.Is(err, inventory.ErrInvalidReservation):
		code = codes.InvalidArgument
	case errors.Is(err, inventory.ErrNotFound):
		code = codes.NotFound
	case errors.Is(err, inventory.ErrInsufficientStock):
		code = codes.FailedPrecondition
	case errors.Is(err, inventory.ErrReservationConflict):
		code = codes.AlreadyExists
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	default:
		code = codes.Internal
	}
	return status.Error(code, err.Error())
}

func toProtoItem(item inventory.Item) *inventoryv1.Item {
	return &inventoryv1.Item{
		Sku:       item.SKU,
		Name:      item.Name,
		Available: item.Available,
		Reserved:  item.Reserved,
	}
}

func toProtoReservation(r inventory.Reservation) *inventoryv1.Reservation {
	lines := make([]*inventoryv1.ReservationLine, 0, len(r.Lines))
	for _, l := range r.Lines {
		lines = append(lines, &inventoryv1.ReservationLine{Sku: l.SKU, Quantity: l.Quantity})
	}
	return &inventoryv1.Reservation{
		Id:          r.ID,
		Lines:       lines,
		CreateTime:  timestamppb.New(r.CreatedAt),
		ReleaseTime: timestamppb.New(r.ReleasedAt),
	}
}
