# Deployment

Generate local certificates and create `.env` before using Compose:

```powershell
Copy-Item .env.example .env
go run ./cmd/certgen
```

Set a non-default `GRAFANA_ADMIN_PASSWORD` in `.env`.

## Monitoring host processes

Start the leader and members on the host, then run:

```powershell
docker compose -f deploy/docker-compose.yml up -d
```

Prometheus scrapes the host through `host.docker.internal`.

## Full cluster

```powershell
docker compose -f deploy/docker-compose.cluster.yml up -d --build
docker compose -f deploy/docker-compose.cluster.yml ps
```

The stack contains one leader, six members, Prometheus, and Grafana. Client processes run on the host.

```powershell
go run ./cmd/client -measure -duration 30s
```

Access:

- Leader TCP: `127.0.0.1:6666`
- Prometheus: `http://127.0.0.1:9091`
- Grafana: `http://127.0.0.1:3000`

Stop containers:

```powershell
docker compose -f deploy/docker-compose.cluster.yml down
```

Delete this project's containers, volumes, and images only when a full reset is intended:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\clean-docker-artifacts.ps1 -DryRun
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\clean-docker-artifacts.ps1
```
