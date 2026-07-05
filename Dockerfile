# core-api 独立镜像(开源版,不含 wallet/payment/provider-specific LLM client 等)。
# build context = openlinker-core/

FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git ca-certificates
ARG TARGETOS
ARG TARGETARCH

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH:-$(go env GOARCH)}" go build -ldflags="-w -s" -o api ./cmd/api

FROM alpine:3.19
ARG OPENLINKER_GIT_SHA=unknown
ARG OPENLINKER_RELEASE_ID=local
ARG OPENLINKER_DEPLOYED_AT=
LABEL org.opencontainers.image.revision="${OPENLINKER_GIT_SHA}" \
      openlinker.release="${OPENLINKER_RELEASE_ID}" \
      openlinker.deployed_at="${OPENLINKER_DEPLOYED_AT}"
RUN apk add --no-cache ca-certificates tzdata wget
WORKDIR /app
COPY --from=builder /app/api .
COPY --from=builder /app/migrations ./migrations

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1
CMD ["./api"]
