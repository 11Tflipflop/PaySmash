DB_URL      ?= postgres://pay:pay@localhost:5432/payengine?sslmode=disable
TEST_DB_URL ?= postgres://pay:pay@localhost:5432/payengine_test?sslmode=disable

.PHONY: db migrate bank serve test demo

db:        ## start postgres in docker
	docker compose up -d

migrate:   ## create the tables
	DATABASE_URL='$(DB_URL)' go run ./cmd/payengine migrate

bank:      ## fake bank with some chaos
	go run ./cmd/fakebank -error-rate 0.2 -hang-rate 0.05 -lost-rate 0.2

serve:     ## api + workers
	DATABASE_URL='$(DB_URL)' go run ./cmd/payengine serve

test:      ## all tests (needs the postgres from "make db")
	DATABASE_URL='$(TEST_DB_URL)' go test -race -count=1 ./...

demo:      ## live demo, kills a worker on purpose
	DATABASE_URL='$(DB_URL)' ./scripts/demo.sh
