.PHONY: init
init:
	docker-compose up -d

.PHONY: psql
psql:
	docker-compose exec -u 70 postgres.adventofcode psql

.PHONY: reset
reset:
	docker-compose down --remove-orphans
	docker-compose up -d