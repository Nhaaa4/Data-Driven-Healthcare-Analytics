SHELL := /bin/bash

TERRAFORM_DIR := terraform
DOCKER_COMPOSE := docker compose
FLINK_JOB_PATH := /opt/src/job/pass_through.py
PRODUCER_PATH := data-streaming/producers/producer_real_time.py

.PHONY: help terraform-init terraform-plan terraform-apply terraform-destroy terraform-output \
	docker-build docker-up docker-down docker-logs flink-submit producer bruin-pipeline dashboard 

help:
	@echo "Available targets:"
	@echo "  terraform-init    Initialize Terraform"
	@echo "  terraform-plan    Show Terraform plan"
	@echo "  terraform-apply   Apply Terraform"
	@echo "  terraform-destroy Destroy Terraform-managed resources"
	@echo "  terraform-output  Show Terraform outputs"
	@echo "  docker-build      Build Flink image"
	@echo "  docker-up         Start Docker services"
	@echo "  docker-down       Stop Docker services"
	@echo "  docker-logs       Tail Docker logs"
	@echo "  flink-submit      Submit the PyFlink job to the jobmanager"
	@echo "  producer          Start the Kafka producer"
	@echo "  dashboard         Start the Streamlit dashboard"
	@echo "  bruin-pipeline     Run the Bruin pipeline"

terraform-init:
	cd $(TERRAFORM_DIR) && terraform init

terraform-plan:
	cd $(TERRAFORM_DIR) && terraform plan

terraform-apply:
	cd $(TERRAFORM_DIR) && terraform apply

terraform-destroy:
	cd $(TERRAFORM_DIR) && terraform destroy

terraform-output:
	cd $(TERRAFORM_DIR) && terraform output

docker-build:
	$(DOCKER_COMPOSE) build jobmanager taskmanager

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-logs:
	$(DOCKER_COMPOSE) logs -f

flink-submit:
	$(DOCKER_COMPOSE) exec jobmanager flink run -py $(FLINK_JOB_PATH)

producer:
	uv run python $(PRODUCER_PATH)

bruin-pipeline:
	bruin run pipeline

dashboard:
	uv run streamlit run dashboard/app.py