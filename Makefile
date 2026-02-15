# ==============================================================================
# TOLEREX DEVELOPMENT MAKEFILE
# ==============================================================================
# This Makefile provides shortcuts for common development tasks:
# - Running tests and benchmarks
# - Cleaning up artifacts
# - Managing Docker deployments
#
# Usage:
#   make [target]
#
# Example:
#   make test        # Run unit tests
#   make deploy-up   # Start monitoring stack
# ==============================================================================

.PHONY: test test-race bench test-all clean-test-artifacts clean-test-artifacts-dry clean-runtime-artifacts clean-runtime-artifacts-dry clean-all-artifacts deploy-up deploy-down deploy-cluster-up deploy-cluster-down

# --- TESTING ---

# Run standard unit tests for core packages
test:
	powershell -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1

# Run tests with the Go race detector enabled (slower, but finds race conditions)
test-race:
	powershell -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1 -Race

# Run standard Go benchmarks
bench:
	powershell -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1 -Bench

# Run EVERYTHING: race tests and benchmarks (time consuming)
test-all:
	powershell -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1 -Race -Bench

# --- CLEANUP ---

# Remove test artifacts (coverage profiles, test binaries, logs)
clean-test-artifacts:
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-test-artifacts.ps1

# Show what WOULD be removed by clean-test-artifacts without deleting anything
clean-test-artifacts-dry:
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-test-artifacts.ps1 -DryRun

# Remove runtime artifacts (application logs, data directories, metrics)
clean-runtime-artifacts:
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-runtime-artifacts.ps1

# Show what WOULD be removed by clean-runtime-artifacts without deleting anything
clean-runtime-artifacts-dry:
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-runtime-artifacts.ps1 -DryRun

# Clean everything (tests + runtime data) - effectively a "factory reset" for data
clean-all-artifacts:
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-test-artifacts.ps1
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-runtime-artifacts.ps1

# --- DEPLOYMENT (DOCKER) ---

# Start the Monitoring Stack (Prometheus + Grafana) on the host
# Use this when running Leader/Member binaries manually on Windows.
deploy-monitoring-up:
	docker compose -f .\deploy\docker-compose.yml up -d

# Stop the Monitoring Stack
deploy-monitoring-down:
	docker compose -f .\deploy\docker-compose.yml down

# Start the Full Cluster (Leader + Members + Monitoring) in Docker
# This builds the images and runs everything in containers.
deploy-cluster-up:
	docker compose -f .\deploy\docker-compose.cluster.yml up -d --build

# Stop the Full Cluster
deploy-cluster-down:
	docker compose -f .\deploy\docker-compose.cluster.yml down

