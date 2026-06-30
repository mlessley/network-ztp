.PHONY: up down dev logs reset test lint build

up:
	docker compose up -d

down:
	docker compose down

dev:
	docker compose -f docker-compose.yml -f docker-compose.override.yml up

logs:
	docker compose logs -f ztp-api ztp-worker

reset:
	docker compose down -v && docker compose up -d

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/

build:
	docker compose build
