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

## --- Kubernetes (kind) ---

kind-up: ## Create 5-node kind cluster + cert-manager + RabbitMQ operator + kube-prometheus-stack + dashboard
	kind create cluster --config k8s/kind-config.yaml --wait 180s
	kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
	kubectl -n cert-manager wait --for=condition=Available deploy --all --timeout=180s
	kubectl apply -f https://github.com/rabbitmq/cluster-operator/releases/latest/download/cluster-operator.yml
	kubectl -n rabbitmq-system wait --for=condition=Available deploy --all --timeout=180s
	helm upgrade --install kps prometheus-community/kube-prometheus-stack -n monitoring --create-namespace \
	  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
	  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
	  --wait --timeout 10m
	kubectl -n monitoring create configmap rabbitmq-overview-dashboard \
	  --from-file=k8s/monitoring/rabbitmq-overview.json 2>/dev/null || true
	kubectl -n monitoring label cm rabbitmq-overview-dashboard grafana_dashboard=1 --overwrite

kind-deploy: ## Build the app image, side-load it into kind, deploy everything
	./mvnw -q package -DskipTests
	docker build -t orderflow:latest .
	kind load docker-image orderflow:latest --name orderflow
	kubectl apply -f k8s/rabbitmq/cluster.yaml
	kubectl apply -f k8s/kafka/kafka.yaml
	@echo "waiting for rabbitmq-default-user secret..." ; \
	until kubectl get secret rabbitmq-default-user >/dev/null 2>&1; do sleep 5; done
	kubectl apply -f k8s/app/orderflow.yaml
	kubectl rollout restart deploy/orderflow
	kubectl wait --for=condition=Available deploy/orderflow --timeout=300s
	kubectl get pods -o wide

kind-down: ## Delete the kind cluster
	kind delete cluster --name orderflow

app-forward: ## Port-forward the app → localhost:18080
	kubectl port-forward svc/orderflow 18080:8080

grafana: ## Port-forward Grafana → localhost:13000 (prints admin password)
	@echo "Grafana → http://localhost:13000  (admin / $$(kubectl -n monitoring get secret kps-grafana -o jsonpath='{.data.admin-password}' | base64 -d))"
	@echo "RabbitMQ dashboard → http://localhost:13000/d/Kn5xm-gZk/rabbitmq-overview"
	kubectl -n monitoring port-forward svc/kps-grafana 13000:80

prometheus: ## Port-forward Prometheus → localhost:19090
	kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 19090:9090

k8s-burst: ## Burst N msgs (default 5000) via the in-cluster app; BROKER=rabbit|kafka|notify (needs app-forward)
	curl -s -X POST 'localhost:18080/orders/burst/$(or $(N),5000)?broker=$(or $(BROKER),rabbit)'

workers-pause: ## Stop ALL rabbit listeners (intake keeps running — queues absorb the load)
	curl -s -X POST localhost:18080/admin/workers/pause

workers-resume: ## Restart the rabbit listeners (drain begins)
	curl -s -X POST localhost:18080/admin/workers/resume

## --- Load testing (k6-operator) ---

k6-install: ## Install the k6-operator (done once per cluster; kind-up does NOT include it)
	helm upgrade --install k6-operator grafana/k6-operator -n k6-operator --create-namespace --wait

k6-run: ## (Re)run the Black Friday load test (4 parallel runners, ~3.5 min)
	kubectl delete testrun order-load --ignore-not-found
	kubectl create configmap k6-order-load --from-file=k6/load-test.js --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f k6/testrun.yaml
	kubectl get testrun order-load

k6-status: ## Show TestRun stage and runner pods
	kubectl get testrun order-load -o jsonpath='stage={.status.stage}{"\n"}' ; kubectl get pods -l k6_cr=order-load

k6-summary: ## Print the k6 end-of-test summary from runner 1
	kubectl logs -l k6_cr=order-load,runner=true --tail=-1 | grep -A40 'TOTAL RESULTS' | head -45

k6-stop: ## Delete the TestRun (stops a running test)
	kubectl delete testrun order-load --ignore-not-found
