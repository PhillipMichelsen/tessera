# ---- Builder ----
FROM golang:1.24-alpine AS builder

ENV CGO_ENABLED=0 GOOS=linux
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -trimpath -ldflags="-s -w" \
      -o /out/data-service ./services/data_service/cmd/data_service

# ---- Runtime ----
FROM gcr.io/distroless/static:nonroot
EXPOSE 50051 50052 6000
COPY --from=builder /out/data-service /bin/data-service
USER nonroot:nonroot
ENTRYPOINT ["/bin/data-service"]
