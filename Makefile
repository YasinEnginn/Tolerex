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
#   make deploy-monitoring-up   # Start monitoring stack
# ==============================================================================

.PHONY: test test-race bench test-all clean-test-artifacts clean-test-artifacts-dry clean-runtime-artifacts clean-runtime-artifacts-dry clean-docker-artifacts clean-docker-artifacts-dry clean-all-artifacts deploy-monitoring-up deploy-monitoring-down deploy-cluster-up deploy-cluster-down deploy-cluster-status deploy-cluster-logs stress-client-30m stress-client-30m-unbuffered stress-clients-30m stress-clients-30m-unbuffered

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

# Remove Docker artifacts for this project (containers/images/networks/volumes/build cache)
clean-docker-artifacts:
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-docker-artifacts.ps1

# Show what WOULD be removed by clean-docker-artifacts without deleting anything
clean-docker-artifacts-dry:
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-docker-artifacts.ps1 -DryRun

# Clean everything (tests + runtime + docker project artifacts)
clean-all-artifacts:
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-test-artifacts.ps1
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-runtime-artifacts.ps1
	powershell -ExecutionPolicy Bypass -File .\scripts\clean-docker-artifacts.ps1

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

# Show Full Cluster container status
deploy-cluster-status:
	docker compose -f .\deploy\docker-compose.cluster.yml ps

# Stream Full Cluster logs
deploy-cluster-logs:
	docker compose -f .\deploy\docker-compose.cluster.yml logs -f --tail=100

# Run one measured stress client for 30 minutes and save CSV under measured/
stress-client-30m:
	powershell -ExecutionPolicy Bypass -File .\scripts\run-stress-client.ps1 -DurationMinutes 30 -ClientID stress-client-30m

# Same as stress-client-30m, but flush every write (unbuffered client behavior)
stress-client-30m-unbuffered:
	powershell -ExecutionPolicy Bypass -File .\scripts\run-stress-client.ps1 -DurationMinutes 30 -ClientID stress-client-30m-unbuffered -Flush 1

# Run 4 measured stress clients in parallel for 30 minutes
stress-clients-30m:
	powershell -ExecutionPolicy Bypass -File .\scripts\run-stress-clients.ps1 -DurationMinutes 30 -Clients 4

# Same as stress-clients-30m, but flush every write for all clients
stress-clients-30m-unbuffered:
	powershell -ExecutionPolicy Bypass -File .\scripts\run-stress-clients.ps1 -DurationMinutes 30 -Clients 4 -Flush 1
