FROM golang:1.26.10-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/leader ./cmd/leader \
    && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/member ./cmd/member \
    && mkdir -p /out/internal/data \
    && chown -R 65532:65532 /out/internal

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /out/leader ./leader
COPY --from=builder /out/member ./member
COPY --from=builder --chown=65532:65532 /out/internal ./internal
COPY --chown=65532:65532 config/tolerance.conf ./config/tolerance.conf

EXPOSE 5555 6666 9090 5556 9092 5557 9093 5558 9094

CMD ["./leader"]
