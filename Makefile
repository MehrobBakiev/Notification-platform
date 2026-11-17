.PHONY: run test race vet build diagrams smoke

run:
	cd code && go run -buildvcs=false ./cmd/demo

test:
	cd code && go test ./...

race:
	cd code && go test -race ./...

vet:
	cd code && go vet ./...

build:
	mkdir -p bin
	cd code && go build -buildvcs=false -trimpath -o ../bin/notification-demo ./cmd/demo

diagrams:
	bash scripts/render-diagrams.sh

smoke:
	python3 scripts/smoke.py
