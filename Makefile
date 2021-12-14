.PHONY: psql
psql: # Open psql shell to DB
	docker-compose exec -u 70 postgres.adventofcode psql

.PHONY: reset
reset: # Reset the DB (deletes all data and schema)
	docker-compose down
	docker-compose up -d