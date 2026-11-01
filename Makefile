.PHONY: run run-memory test test-integration bench lint up down result

DB_URL ?= postgres://bets:bets@localhost:5432/bets?sslmode=disable

run: ## run locally against docker-compose Postgres and Kafka
	DATABASE_URL="$(DB_URL)" go run ./cmd/settlement

run-memory: ## run without any infrastructure
	STORAGE=memory go run ./cmd/settlement

test: ## unit tests with the race detector
	go test -race -count=1 ./...

test-integration: ## unit + Postgres integration tests (needs `make up`)
	TEST_DATABASE_URL="$(DB_URL)" go test -race -count=1 ./...

bench:
	go test -run=^$$ -bench=. -benchmem ./internal/worker/

lint:
	golangci-lint run

up:
	docker compose up -d --build

down:
	docker compose down -v

# Publish a match result to Kafka: make result MATCH=m1 OUTCOME=HOME
MATCH ?= m1
OUTCOME ?= HOME
result:
	echo '{"match_id":"$(MATCH)","outcome":"$(OUTCOME)"}' | docker compose exec -T kafka \
		/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic match-results
