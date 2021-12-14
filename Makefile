.PHONY: psql
psql:
	docker-compose exec -u 70 postgres.adventofcode psql

.PHONY: reset
reset:
	docker-compose down
	docker-compose up -d