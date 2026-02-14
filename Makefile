# CDC Historical Warehouse Platform Makefile
# Provides convenient targets for managing the CDC pipeline

.PHONY: help start stop status restart loader test clean logs docker-up docker-down

# Default target
.DEFAULT_GOAL := help

# Configuration
PYTHON := python3
LOG_LEVEL ?= INFO
MUTATION_INTERVAL ?= 5
CDC_INTERVAL ?= 10

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

help: ## Show this help message
	@echo "$(BLUE)CDC Historical Warehouse Platform Makefile$(NC)"
	@echo "====================="
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Configuration:"
	@echo "  LOG_LEVEL=$(LOG_LEVEL)"
	@echo "  MUTATION_INTERVAL=$(MUTATION_INTERVAL)"
	@echo "  CDC_INTERVAL=$(CDC_INTERVAL)"

start: ## Start the complete CDC pipeline
	@echo "$(BLUE)Starting CDC Pipeline...$(NC)"
	@LOG_LEVEL=$(LOG_LEVEL) MUTATION_INTERVAL_SECONDS=$(MUTATION_INTERVAL) CDC_EXTRACTION_INTERVAL_SECONDS=$(CDC_INTERVAL) ./run_pipeline.sh start

stop: ## Stop all pipeline components
	@echo "$(BLUE)Stopping CDC Pipeline...$(NC)"
	@./run_pipeline.sh stop

status: ## Show pipeline status
	@echo "$(BLUE)Pipeline Status:$(NC)"
	@./run_pipeline.sh status

restart: ## Restart the pipeline
	@echo "$(BLUE)Restarting CDC Pipeline...$(NC)"
	@./run_pipeline.sh restart

loader: ## Run only the SCD2 loader
	@echo "$(BLUE)Running SCD2 Loader...$(NC)"
	@LOG_LEVEL=$(LOG_LEVEL) ./run_pipeline.sh loader

test: ## Run validation tests
	@echo "$(BLUE)Running Validation Tests...$(NC)"
	@LOG_LEVEL=$(LOG_LEVEL) $(PYTHON) tests/verify_scd2.py

test-rapid: ## Test rapid updates scenario
	@echo "$(BLUE)Testing Rapid Updates...$(NC)"
	@LOG_LEVEL=$(LOG_LEVEL) $(PYTHON) scripts/test_rapid_updates.py

docker-up: ## Start only the databases
	@echo "$(BLUE)Starting Databases...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)Databases started!$(NC)"
	@echo "  operational_db: localhost:5434"
	@echo "  warehouse_db: localhost:5433"

docker-down: ## Stop the databases
	@echo "$(BLUE)Stopping Databases...$(NC)"
	@docker-compose down

install: ## Install Python dependencies
	@echo "$(BLUE)Installing Dependencies...$(NC)"
	@$(PYTHON) -m pip install -r requirements.txt

env: ## Set up environment file
	@if [ ! -f .env ]; then \
		echo "$(BLUE)Creating .env from .env.example...$(NC)"; \
		cp .env.example .env; \
	else \
		echo "$(YELLOW).env file already exists$(NC)"; \
	fi

logs: ## Show recent log files
	@echo "$(BLUE)Recent Log Files:$(NC)"
	@echo ""
	@if [ -f logs/simulator.log ]; then \
		echo "$(GREEN)Simulator Log (last 20 lines):$(NC)"; \
		tail -20 logs/simulator.log; \
		echo ""; \
	fi
	@if [ -f logs/extractor.log ]; then \
		echo "$(GREEN)CDC Extractor Log (last 20 lines):$(NC)"; \
		tail -20 logs/extractor.log; \
		echo ""; \
	fi
	@if [ -f logs/src_warehouse_scd2_loader.log ]; then \
		echo "$(GREEN)SCD2 Loader Log (last 20 lines):$(NC)"; \
		tail -20 logs/src_warehouse_scd2_loader.log; \
	fi

clean: ## Clean up temporary files and logs
	@echo "$(BLUE)Cleaning up...$(NC)"
	@rm -rf logs/
	@rm -f .simulator.pid .extractor.pid
	@rm -f data/cdc_logs/.processed_files
	@rm -f scd2_lineage_report_*.md
	@echo "$(GREEN)Cleanup completed!$(NC)"

validate: ## Run comprehensive validation
	@echo "$(BLUE)Running Comprehensive Validation...$(NC)"
	@make test
	@make test-rapid
	@echo "$(GREEN)All validations completed!$(NC)"

dev-setup: ## Set up development environment
	@echo "$(BLUE)Setting up Development Environment...$(NC)"
	@make install
	@make env
	@mkdir -p logs data/cdc_logs scripts
	@echo "$(GREEN)Development environment ready!$(NC)"
	@echo ""
	@echo "Next steps:"
	@echo "  make start     # Start the pipeline"
	@echo "  make test      # Run validation tests"
	@echo "  make logs      # View logs"

# Quick start target
quick-start: dev-setup docker-up start ## Quick start for development

# Production-like target
prod-start: ## Production-like start with monitoring
	@echo "$(BLUE)Starting Pipeline in Production Mode...$(NC)"
	@LOG_LEVEL=WARNING MUTATION_INTERVAL=30 CDC_INTERVAL=60 make start
