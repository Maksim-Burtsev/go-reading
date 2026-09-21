PKGS := $(shell go list ./... 2>/dev/null)

.PHONY: lint test test-integration

lint:
	gofmt -l . | (! grep .)
ifneq ($(PKGS),)
	go vet ./...
	golangci-lint run
endif

test:
ifneq ($(PKGS),)
	go test -race ./...
endif

test-integration:
ifneq ($(PKGS),)
	go test -race -tags integration ./...
endif

run-%:
	go run ./apps/$*/cmd/... $(ARGS)
