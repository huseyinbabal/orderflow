# OrderFlow — live-demo shortcuts (Sessions 1–3)
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

burst-whale: ## Burst N msgs ALL keyed by one whale customer — watch one partition's lag tower
	curl -s -X POST 'localhost:8080/orders/burst/$(or $(N),100000)?broker=whale'

## --- Session 3: Avro + Schema Registry (runs on kind — start registry-forward for the curls) ---

k8s-order: ## Publish one order via the in-cluster app (needs app-forward)
	curl -s -X POST localhost:18080/orders -H 'Content-Type: application/json' \
	  -d '{"orderId":"avro-1","customerId":"c-1","amount":49.90}'

registry-subjects: ## List registered subjects (expect ["orders-value"] after first publish)
	curl -s localhost:8081/subjects ; echo

registry-versions: ## Show schema versions for orders-value
	curl -s localhost:8081/subjects/orders-value/versions ; echo

registry-latest: ## Pretty-print the latest registered orders-value schema
	curl -s localhost:8081/subjects/orders-value/versions/latest | python3 -m json.tool

raw-bytes: ## Peek raw bytes on the orders topic — binary Avro, no field names on the wire
	kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 3

poison: ## Publish a NON-Avro string onto orders — watch ErrorHandlingDeserializer route it to the DLT
	echo 'this is not avro' | kubectl exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh \
	  --bootstrap-server localhost:9092 --topic orders

dlt-peek: ## Read the poison pill back from orders.DLT
	kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic orders.DLT --from-beginning --timeout-ms 5000

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
	$(MAKE) dashboards

dashboards: ## (Re)load ALL Grafana dashboards in k8s/monitoring/ into the cluster (idempotent)
	@for f in k8s/monitoring/*.json; do \
	  name=$$(basename $$f .json)-dashboard; \
	  kubectl -n monitoring create configmap $$name --from-file=$$f --dry-run=client -o yaml | kubectl apply -f - ; \
	  kubectl -n monitoring label cm $$name grafana_dashboard=1 --overwrite ; \
	done

kind-deploy: ## Build the app image, side-load it into kind, deploy everything (API + consumers + registry)
	./mvnw -q package -DskipTests
	docker build -t orderflow:latest .
	kind load docker-image orderflow:latest --name orderflow
	kubectl apply -f k8s/rabbitmq/cluster.yaml
	kubectl apply -f k8s/kafka/kafka.yaml
	kubectl apply -f k8s/kafka/schema-registry.yaml
	kubectl apply -f k8s/kafka/kafka-exporter.yaml
	kubectl apply -f k8s/kafka/kafka-ui.yaml
	@echo "waiting for rabbitmq-default-user secret..." ; \
	until kubectl get secret rabbitmq-default-user >/dev/null 2>&1; do sleep 5; done
	kubectl apply -f k8s/app/orderflow.yaml
	kubectl apply -f k8s/app/orderflow-consumer.yaml
	kubectl rollout restart deploy/orderflow deploy/orderflow-consumer
	kubectl wait --for=condition=Available deploy/orderflow deploy/orderflow-consumer --timeout=300s
	kubectl get pods -o wide

kind-down: ## Delete the kind cluster
	kind delete cluster --name orderflow

app-forward: ## Port-forward the app → localhost:18080
	kubectl port-forward svc/orderflow 18080:8080

grafana: ## Port-forward Grafana → localhost:13000 (prints admin password)
	@echo "Grafana → http://localhost:13000  (admin / $$(kubectl -n monitoring get secret kps-grafana -o jsonpath='{.data.admin-password}' | base64 -d))"
	@echo "RabbitMQ dashboard → http://localhost:13000/d/Kn5xm-gZk/rabbitmq-overview"
	@echo "Kafka dashboard    → http://localhost:13000/d/kafka-orderflow/kafka-orderflow-overview"
	kubectl -n monitoring port-forward svc/kps-grafana 13000:80

prometheus: ## Port-forward Prometheus → localhost:19090
	kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 19090:9090

k8s-burst: ## Burst N msgs (default 5000) via the in-cluster app; BROKER=rabbit|kafka|whale|notify (needs app-forward)
	curl -s -X POST 'localhost:18080/orders/burst/$(or $(N),5000)?broker=$(or $(BROKER),rabbit)'

consumers-scale: ## Scale the Kafka consumer deployment to N pods (default 3) — the live lag-drain demo
	kubectl scale deploy/orderflow-consumer --replicas=$(or $(N),3)
	kubectl get pods -l app=orderflow-consumer

consumers-logs: ## Tail all consumer pods (watch partitions rebalance as you scale)
	kubectl logs -l app=orderflow-consumer -f --max-log-requests=12 --tail=5

k8s-lag: ## Kafka consumer lag, in-cluster (fulfillment + analytics)
	@kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server localhost:9092 --group fulfillment --describe || true
	@kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server localhost:9092 --group analytics --describe

registry-forward: ## Port-forward the in-cluster schema registry → localhost:8081
	kubectl port-forward svc/cp-schema-registry 8081:8081

producer-tune: ## Live producer tuning on the API pod: linger.ms=20 + lz4 + 64K batches (triggers rollout)
	kubectl set env deploy/orderflow \
	  SPRING_KAFKA_PRODUCER_COMPRESSION_TYPE=lz4 \
	  SPRING_KAFKA_PRODUCER_BATCH_SIZE=65536 \
	  SPRING_KAFKA_PRODUCER_PROPERTIES_LINGER_MS=20

producer-untune: ## Revert producer tuning back to defaults (linger.ms=0, no compression)
	kubectl set env deploy/orderflow \
	  SPRING_KAFKA_PRODUCER_COMPRESSION_TYPE- \
	  SPRING_KAFKA_PRODUCER_BATCH_SIZE- \
	  SPRING_KAFKA_PRODUCER_PROPERTIES_LINGER_MS-

kafka-ui: ## Port-forward Kafka UI → localhost:8082 (topics, consumer groups, Avro-decoded messages)
	@echo "Kafka UI → http://localhost:8082"
	kubectl port-forward svc/kafka-ui 8082:8080

workers-pause: ## Stop ALL rabbit listeners (intake keeps running — queues absorb the load)
	curl -s -X POST localhost:18080/admin/workers/pause

workers-resume: ## Restart the rabbit listeners (drain begins)
	curl -s -X POST localhost:18080/admin/workers/resume

## --- Load testing (k6-operator) ---

k6-install: ## Install the k6-operator (done once per cluster; kind-up does NOT include it)
	helm upgrade --install k6-operator grafana/k6-operator -n k6-operator --create-namespace --wait

k6-run: ## (Re)run the Black Friday load test (4 runners, ~3.5 min); BROKER=all|rabbit|kafka (default all)
	kubectl delete testrun order-load --ignore-not-found
	kubectl create configmap k6-order-load --from-file=k6/load-test.js --dry-run=client -o yaml | kubectl apply -f -
	sed 's/BROKER_PLACEHOLDER/$(or $(BROKER),all)/' k6/testrun.yaml | kubectl apply -f -
	kubectl get testrun order-load

k6-status: ## Show TestRun stage and runner pods
	kubectl get testrun order-load -o jsonpath='stage={.status.stage}{"\n"}' ; kubectl get pods -l k6_cr=order-load

k6-summary: ## Print the k6 end-of-test summary from runner 1
	kubectl logs -l k6_cr=order-load,runner=true --tail=-1 | grep -A40 'TOTAL RESULTS' | head -45

k6-stop: ## Delete the TestRun (stops a running test)
	kubectl delete testrun order-load --ignore-not-found
