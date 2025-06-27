# Stage 1: Builder
FROM golang:latest AS builder

WORKDIR /app

# Copy go mod and download deps
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /data_service ./services/data_service/cmd

# Stage 2: Runner
FROM debian:bullseye-slim

# Minimal setup
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy binary
COPY --from=builder /data_service /usr/local/bin/data_service

ENTRYPOINT ["data_service"]
