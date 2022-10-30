.PHONY: unit-tests
unit-tests:
	go test -race -short -v --count 1 ./...

.PHONY: integration-tests
integration-tests:
	go test -race -run TestIntegration_ -v --count 1 ./...

.PHONY: generate-mocks
generate-mocks:
	mockery --inpackage --case snake --all --with-expecter --dir ./pkg


.PHONY: consumer
consumer:
	go run ./cmd/consumer/...

.PHONY: producer
producer:
	go run ./cmd/producer/...