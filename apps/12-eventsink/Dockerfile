# syntax=docker/dockerfile:1

FROM golang:1.27-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY apps/12-eventsink ./apps/12-eventsink
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/eventsink ./apps/12-eventsink/cmd/eventsink

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/eventsink /eventsink
USER nonroot:nonroot
EXPOSE 8012
ENTRYPOINT ["/eventsink"]
