GO ?= go
BIN ?= bin
IMG ?= kafka-controller:dev

.PHONY: all
all: generate fmt vet test build

.PHONY: generate
generate: ## Regenerate deepcopy, CRDs, RBAC, and sync them into the chart.
	$(GO) tool controller-gen object:headerFile=hack/boilerplate.go.txt paths=./apis/...
	$(GO) tool controller-gen crd paths=./apis/... output:crd:artifacts:config=config/crd
	$(GO) tool controller-gen rbac:roleName=kafka-controller paths=./internal/... output:rbac:artifacts:config=config/rbac
	./hack/sync-chart.sh

.PHONY: helm-lint
helm-lint: ## Lint and render the chart, including with the toggles flipped.
	helm lint charts/kafka-controller
	helm template kafka-controller charts/kafka-controller >/dev/null
	helm template kafka-controller charts/kafka-controller --set crds.install=false >/dev/null
	helm template kafka-controller charts/kafka-controller --set rbac.create=false >/dev/null
	helm template kafka-controller charts/kafka-controller --set autoscaling.enabled=true >/dev/null
	helm template kafka-controller charts/kafka-controller --set podDisruptionBudget.maxUnavailable=1 >/dev/null
	helm template kafka-controller charts/kafka-controller --set metrics.serviceMonitor.enabled=true >/dev/null
	helm template kafka-controller charts/kafka-controller --set prometheusRule.enabled=true >/dev/null

# Exactly the paths `generate` writes; the rest of the chart is hand-maintained.
GENERATED_PATHS = config apis/kafka/v1alpha1/zz_generated.deepcopy.go \
                  charts/kafka-controller/templates/crds.yaml \
                  charts/kafka-controller/templates/rbac.yaml

.PHONY: check-generated
check-generated: generate ## Fail if the generated files are out of date.
	@git diff --exit-code -- $(GENERATED_PATHS) || \
		{ echo "generated files are stale -- run 'make generate' and commit"; exit 1; }

.PHONY: fmt
fmt: ## Format with gofumpt (the commit hook also runs the configured formatters).
	$(GO) tool gofumpt -w .

.PHONY: vet
vet:
	$(GO) vet -tags 'integration envtest' ./...

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: test
test: ## Unit tests. No cluster or Kafka required.
	$(GO) test ./... -race -coverprofile=cover.out

.PHONY: test-envtest
test-envtest: ## Run the controller against a real kube-apiserver (no cluster).
	KUBEBUILDER_ASSETS="$$($(GO) run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use -p path)" \
	  $(GO) test -tags envtest ./internal/controller/... -v

# The integration broker: KRaft, SASL/SCRAM and the standard authorizer, so
# ACLs and SCRAM credentials are real. See hack/kafka-test/.
KAFKA_TEST_BOOTSTRAP ?= 127.0.0.1:19092

.PHONY: test-integration
test-integration: kafka-up ## Run the engine against a real Kafka.
	KAFKA_TEST_BOOTSTRAP='$(KAFKA_TEST_BOOTSTRAP)' $(GO) test -tags integration ./internal/engine/kafka/ -v -count=1

.PHONY: kafka-up
kafka-up: ## Start the Kafka the integration tests use.
	./hack/kafka-test/up.sh

.PHONY: kafka-down
kafka-down: ## Stop the integration Kafka.
	-docker rm -f kafkatest

.PHONY: build
build:
	$(GO) build -o $(BIN)/manager ./cmd/manager
	$(GO) build -o $(BIN)/kactl ./cmd/kactl

.PHONY: docker-build
docker-build: ## Build the image from source.
	docker build -t $(IMG) .

.PHONY: clean
clean:
	rm -rf $(BIN) cover.out

.PHONY: check
check: vet test test-envtest helm-lint ## The non-Kafka half of CI. No Kafka or cluster needed.

.PHONY: help
help:
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
