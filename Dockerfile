# core-api 独立镜像(开源版,不含 wallet/payment/provider-specific LLM client 等)。
# build context = openlinker-core/

FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git ca-certificates
ARG TARGETOS
ARG TARGETARCH

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    set -eu; \
    attempt=1; \
    until GODEBUG=http2client=0 go mod download; do \
      if [ "${attempt}" -ge 3 ]; then exit 1; fi; \
      sleep "$((attempt * 2))"; \
      attempt=$((attempt + 1)); \
    done

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH:-$(go env GOARCH)}" go build -ldflags="-w -s" -o api ./cmd/api && \
    CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH:-$(go env GOARCH)}" go build -ldflags="-w -s" -o runtime-cutover ./cmd/runtime-cutover

FROM alpine:3.23
ARG OPENLINKER_GIT_SHA=unknown
ARG OPENLINKER_RELEASE_ID=local
ARG OPENLINKER_DEPLOYED_AT=
ENV OPENLINKER_GIT_SHA=${OPENLINKER_GIT_SHA} \
    OPENLINKER_RELEASE_ID=${OPENLINKER_RELEASE_ID}
LABEL org.opencontainers.image.revision="${OPENLINKER_GIT_SHA}" \
      openlinker.release="${OPENLINKER_RELEASE_ID}" \
      openlinker.deployed_at="${OPENLINKER_DEPLOYED_AT}"
RUN apk add --no-cache ca-certificates tzdata wget
WORKDIR /app
COPY --from=builder /app/api .
COPY --from=builder /app/runtime-cutover .
COPY --from=builder /app/migrations ./migrations

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1
CMD ["./api"]
