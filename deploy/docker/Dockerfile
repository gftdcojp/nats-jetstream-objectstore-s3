FROM golang:1.25-alpine AS builder

ARG VERSION=dev

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION}" -o /nats-objstore-s3 ./cmd/nats-objstore-s3

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata
RUN adduser -D -u 1000 nos3

COPY --from=builder /nats-objstore-s3 /usr/local/bin/nats-objstore-s3

USER nos3
EXPOSE 9000 8081 9090
ENTRYPOINT ["nats-objstore-s3"]
CMD ["-config", "/etc/nos3/config.yaml"]
