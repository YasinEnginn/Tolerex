FROM golang:1.24 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/leader ./cmd/leader
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/member ./cmd/member
RUN mkdir -p /out/internal/data

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /out/leader ./leader
COPY --from=builder /out/member ./member
COPY --from=builder /out/internal ./internal
COPY config ./config

EXPOSE 5555 6666 9090 5556 9092 5557 9093 5558 9094

CMD ["./leader"]
