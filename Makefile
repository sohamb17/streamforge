# Common tasks. Everything also works with plain go / docker compose.
COMPOSE := docker compose -f deploy/compose/docker-compose.yml
export GOTOOLCHAIN ?= auto

.PHONY: help data up down logs ps test test-raft mutation gate1 gate3 faults gate4 wasm web web-dev proto fmt

help: ## list targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

data: ## download the NYC TLC month and verify checksums
	data/fetch.sh

up: data ## build and start the whole stack (dashboard http://localhost:8080)
	$(COMPOSE) up -d --build

down: ## stop everything and delete volumes
	$(COMPOSE) down -v

logs: ## follow logs
	$(COMPOSE) logs -f --tail=50

ps: ## container status
	$(COMPOSE) ps

test: ## unit, property and Raft simulation tests
	go test ./...

test-raft: ## 1,000 randomized fault schedules against the Raft core
	RAFTSIM_SEEDS=1000 go test ./internal/raftsim/ -run 'TestRandomizedFaults$$' -v

mutation: ## re-insert classic Raft bugs and check the tests catch them
	scripts/mutation-test.sh

gate1: data ## single node: oracle, crash recovery, replay determinism
	scripts/gate1.sh

faults: ## crash, pause, partition, worker, Redis faults on the running stack
	faults/run-all.sh

gate3: ## serving, freshness, skew and parity checks on the running stack
	scripts/gate3.sh

gate4: ## the measured run (about 90 minutes)
	bench/gate4.sh

wasm: ## build the in-browser simulator into web/public
	GOOS=js GOARCH=wasm go build -o web/public/sim.wasm ./cmd/simwasm
	cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" web/public/

web: wasm ## build the dashboard into web/dist
	cd web && npm ci && npm run build

web-dev: wasm ## dashboard dev server on http://localhost:5173 (proxies /api to :8080)
	cd web && npm install && npm run dev

proto: ## regenerate gRPC code (needs buf, protoc-gen-go, protoc-gen-go-grpc)
	scripts/gen-proto.sh

fmt:
	gofmt -w cmd internal
