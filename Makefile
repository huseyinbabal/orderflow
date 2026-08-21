# OrderFlow — Session 1 live-demo shortcuts
# Usage: make <target>   (run `make` alone to list targets)

.DEFAULT_GOAL := help

COMPOSE  := docker compose
KAFKA_CLI := $(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092

help: ## List all targets
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## --- Environment ---

up: ## Start RabbitMQ + Kafka + Redis (RabbitMQ UI → localhost:15672, guest/guest)
	$(COMPOSE) up -d
	$(COMPOSE) ps

down: ## Stop and remove containers (queues/topics are lost — fresh start next `make up`)
	$(COMPOSE) down

## --- App ---

run: ## Run the app on :8080
	./mvnw spring-boot:run

run-2: ## Run a SECOND instance on :8081 (watch the Kafka rebalance in its logs)
	./mvnw spring-boot:run -Dspring-boot.run.arguments=--server.port=8081

## --- Demos ---

order: ## Publish one order → RabbitMQ consumer + both Kafka groups log it
	curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
	  -d '{"orderId":"o-1","customerId":"c-1","amount":49.90}'

burst-rabbit: ## Burst N msgs into RabbitMQ (default N=100000) — watch queue depth in the UI
	curl -s -X POST 'localhost:8080/orders/burst/$(or $(N),100000)?broker=rabbit'

burst-kafka: ## Burst N msgs into Kafka (default N=100000) — then check `make lag`
	curl -s -X POST 'localhost:8080/orders/burst/$(or $(N),100000)?broker=kafka'

queue-depth: ## RabbitMQ: show orders queue depth (ready / unacked)
	$(COMPOSE) exec rabbitmq rabbitmqctl list_queues name messages messages_unacknowledged

lag: ## Kafka: show consumer lag for both groups
	@$(KAFKA_CLI) --group fulfillment --describe || true
	@$(KAFKA_CLI) --group analytics --describe

replay-analytics: ## Reset 'analytics' offsets to earliest — STOP the app first, restart after
	$(KAFKA_CLI) --group analytics --reset-offsets --to-earliest --topic orders --execute
