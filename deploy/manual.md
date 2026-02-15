# Tolerex Deploy and Monitoring

This guide covers two deploy modes:

- Monitoring-only (Leader/Member processes run on host)
- Full Docker cluster (Leader + Members + Prometheus + Grafana in containers)

Client processes are intentionally excluded from deploy Compose files.

## Prerequisites

- Docker Engine
- Docker Compose v2 (`docker compose`)

## Mode 1: Monitoring Only (Host Services)

1. Run leader/member processes on host (ports `9090`, `9092-9097`).
2. Start monitoring stack:

```bash
docker compose -f deploy/docker-compose.yml up -d
```

3. Prometheus uses `deploy/prometheus.local.yml` and scrapes `host.docker.internal` targets.

## Mode 2: Full Docker Cluster

Start full stack:

```bash
docker compose -f deploy/docker-compose.cluster.yml up -d --build
```

This uses:

- `deploy/prometheus.cluster.yml` for in-network scrape targets (`leader`, `member-1` ... `member-6`)
- `Dockerfile` image `tolerex-node:latest` for leader/member services

Stop cluster:

```bash
docker compose -f deploy/docker-compose.cluster.yml down
```

Run a 30-minute measured stress client from repo root:

```bash
make stress-client-30m
```

Run 4 measured stress clients in parallel:

```bash
make stress-clients-30m
```

## Access

- Leader TCP: `localhost:6666`
- Leader gRPC: `localhost:5555`
- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3000` (`admin/admin`)

## Grafana Dashboard

Grafana is provisioned automatically at startup:

- Prometheus datasource (`deploy/grafana/provisioning/datasources/prometheus.yml`)
- Dashboard provider (`deploy/grafana/provisioning/dashboards/dashboards.yml`)
- Dashboard JSON (`deploy/grafana_dashboard.json`)

## Verification

- Prometheus targets: `http://localhost:9091/targets`
- Check `UP` state for expected jobs.
