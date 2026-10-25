.PHONY: test test-race bench test-all chaos-3m clean-test clean-runtime clean-docker clean-all monitoring-up monitoring-down cluster-up cluster-down cluster-status cluster-logs stress-client-30m stress-client-30m-immediate stress-clients-30m stress-clients-30m-immediate

POWERSHELL = powershell -NoProfile -ExecutionPolicy Bypass -File

test:
	$(POWERSHELL) .\scripts\run-tests.ps1

test-race:
	$(POWERSHELL) .\scripts\run-tests.ps1 -Race

bench:
	$(POWERSHELL) .\scripts\run-tests.ps1 -Bench

test-all:
	$(POWERSHELL) .\scripts\run-tests.ps1 -Race -Bench

chaos-3m:
	$(POWERSHELL) .\scripts\run-chaos-test.ps1 -DurationSeconds 180 -Clients 6 -MaxMessages 60000

clean-test:
	$(POWERSHELL) .\scripts\clean-test-artifacts.ps1

clean-runtime:
	$(POWERSHELL) .\scripts\clean-runtime-artifacts.ps1

clean-docker:
	$(POWERSHELL) .\scripts\clean-docker-artifacts.ps1

clean-all: clean-test clean-runtime clean-docker

monitoring-up:
	docker compose -f .\deploy\docker-compose.yml up -d

monitoring-down:
	docker compose -f .\deploy\docker-compose.yml down

cluster-up:
	docker compose -f .\deploy\docker-compose.cluster.yml up -d --build

cluster-down:
	docker compose -f .\deploy\docker-compose.cluster.yml down

cluster-status:
	docker compose -f .\deploy\docker-compose.cluster.yml ps

cluster-logs:
	docker compose -f .\deploy\docker-compose.cluster.yml logs -f --tail=100

stress-client-30m:
	$(POWERSHELL) .\scripts\run-stress-client.ps1 -DurationMinutes 30 -ClientID stress-client-30m

stress-client-30m-immediate:
	$(POWERSHELL) .\scripts\run-stress-client.ps1 -DurationMinutes 30 -ClientID stress-client-30m-immediate -Flush 1

stress-clients-30m:
	$(POWERSHELL) .\scripts\run-stress-clients.ps1 -DurationMinutes 30 -Clients 4

stress-clients-30m-immediate:
	$(POWERSHELL) .\scripts\run-stress-clients.ps1 -DurationMinutes 30 -Clients 4 -Flush 1
