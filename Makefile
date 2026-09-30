SHELL := /bin/sh

.PHONY: tidy test race vet check up down logs test-multiprocess integration e2e-compose e2e-keycloak e2e-restart scale-3 metrics-check

tidy:
	go mod tidy

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: tidy test race vet

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f api

test-multiprocess:
	TEST_DATABASE_URL="$${TEST_DATABASE_URL:-postgres://postgres:postgres@localhost:5432/wager?sslmode=disable}" go run ./cmd/multiprocess-concurrency-test

integration:
	TEST_DATABASE_URL="$${TEST_DATABASE_URL:-postgres://postgres:postgres@localhost:5432/wager?sslmode=disable}" go test -tags=integration ./internal/... -v

scale-3:
	docker compose up -d --build --scale api=3

e2e-compose:
	bash ./scripts/e2e-compose.sh

e2e-keycloak:
	bash ./scripts/e2e-keycloak.sh

e2e-restart:
	bash ./scripts/e2e-restart.sh

metrics-check:
	curl -fsS http://localhost:8080/metrics