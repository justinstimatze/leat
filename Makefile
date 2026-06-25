.PHONY: test vet fmt lint cover tidy

test:           ## run the test suite
	go test ./...

vet:            ## go vet
	go vet ./...

fmt:            ## format all sources
	gofmt -w .

lint: fmt vet  ## format + vet

cover:          ## test with coverage summary
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

tidy:           ## tidy the module
	go mod tidy
