package server

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func unaryLogging(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logCall(ctx, logger, info.FullMethod, start, err)
		return resp, err
	}
}

func streamLogging(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logCall(ss.Context(), logger, info.FullMethod, start, err)
		return err
	}
}

func logCall(ctx context.Context, logger *slog.Logger, method string, start time.Time, err error) {
	code := status.Code(err)
	attrs := []any{
		slog.String("method", method),
		slog.String("code", code.String()),
		slog.Duration("duration", time.Since(start)),
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}

	switch code {
	case codes.Unknown, codes.Internal, codes.Unimplemented, codes.Unavailable, codes.DataLoss:
		logger.ErrorContext(ctx, "rpc failed", attrs...)
	default:
		logger.InfoContext(ctx, "rpc finished", attrs...)
	}
}

func unaryRecovery(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = recovered(ctx, logger, info.FullMethod, p)
			}
		}()
		return handler(ctx, req)
	}
}

func streamRecovery(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = recovered(ss.Context(), logger, info.FullMethod, p)
			}
		}()
		return handler(srv, ss)
	}
}

func recovered(ctx context.Context, logger *slog.Logger, method string, p any) error {
	logger.ErrorContext(ctx, "panic in handler",
		slog.String("method", method),
		slog.String("panic", fmt.Sprint(p)),
		slog.String("stack", string(debug.Stack())),
	)
	return status.Error(codes.Internal, "internal error")
}

func unaryDefaultTimeout(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := ctx.Deadline(); ok {
			return handler(ctx, req)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return handler(ctx, req)
	}
}
