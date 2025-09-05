SHELL := /bin/bash

GOOSE_ENV = GOOSE_DRIVER=mysql GOOSE_DBSTRING="app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=Local" GOOSE_MIGRATION_DIR="./db/migrations"

.PHONY: goose worker monitor purge-workers

# Run database migrations using goose
goose:
	$(GOOSE_ENV) goose up

# Start the worker
worker:
	go run ./cmd/worker

# Start the monitor TUI
monitor:
	go run ./cmd/monitor

# Purge stale workers not referenced by any task
older ?= 24h
purge-workers:
	go run ./cmd/purgeworkers -older-than $(older)
