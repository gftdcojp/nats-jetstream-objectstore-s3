# nats-jetstream-objectstore-s3

S3-compatible API gateway for [NATS JetStream ObjectStore](https://docs.nats.io/nats-concepts/jetstream/obj_store).

Use any S3 client (AWS CLI, aws-sdk-go, boto3, MinIO Client, etc.) to store and retrieve objects in NATS JetStream.

## Features

- S3 REST API (path-style addressing)
- Bucket and object CRUD
- ListObjectsV2 with prefix, delimiter, continuation token
- Multipart upload (create, upload parts, complete, abort, list parts)
- CopyObject (cross-bucket supported)
- AWS Signature V4 authentication (header + presigned URL)
- Auto-create buckets on first write
- ETag (MD5) computation
- Prometheus metrics + health endpoints
- Embeddable as a Go library (`pkg/s3gw`)
- Docker + Kubernetes ready

## Supported S3 Operations

| Operation | Method | Path |
|-----------|--------|------|
| ListBuckets | `GET /` | |
| CreateBucket | `PUT /{bucket}` | |
| HeadBucket | `HEAD /{bucket}` | |
| DeleteBucket | `DELETE /{bucket}` | |
| ListObjectsV2 | `GET /{bucket}?list-type=2` | prefix, delimiter, continuation-token, max-keys |
| GetObject | `GET /{bucket}/{key+}` | |
| PutObject | `PUT /{bucket}/{key+}` | |
| CopyObject | `PUT /{bucket}/{key+}` | `x-amz-copy-source` header |
| DeleteObject | `DELETE /{bucket}/{key+}` | |
| HeadObject | `HEAD /{bucket}/{key+}` | |
| CreateMultipartUpload | `POST /{bucket}/{key+}?uploads` | |
| UploadPart | `PUT /{bucket}/{key+}?partNumber&uploadId` | |
| CompleteMultipartUpload | `POST /{bucket}/{key+}?uploadId` | |
| AbortMultipartUpload | `DELETE /{bucket}/{key+}?uploadId` | |
| ListParts | `GET /{bucket}/{key+}?uploadId` | |

## S3 to NATS Mapping

| S3 Concept | NATS ObjectStore | Notes |
|---|---|---|
| S3 Bucket | ObjectStore bucket | Direct mapping |
| Object Key | Object name | `/` in keys preserved |
| ETag | MD5 computed on write | Stored in `X-Nos3-Etag` header |
| Content-Type | `ObjectMeta.Headers["Content-Type"]` | |
| x-amz-meta-* | `ObjectMeta.Headers["X-Amz-Meta-*"]` | Custom metadata passthrough |
| ListObjectsV2 | Client-side filtering of `obs.List()` | Sorted by key, base64 continuation token |
| Multipart | In-memory part staging | Parts concatenated on complete |

## Quick Start

### Binary

```bash
go install github.com/gftdcojp/nats-jetstream-objectstore-s3/cmd/nats-objstore-s3@latest
nats-objstore-s3 -config config.yaml
```

### Docker Compose

```bash
docker compose -f deploy/docker/docker-compose.yaml up
```

### Test with AWS CLI

```bash
# Configure endpoint
export AWS_ACCESS_KEY_ID=nats-s3-admin
export AWS_SECRET_ACCESS_KEY=changeme
export AWS_DEFAULT_REGION=us-east-1

# Create bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket

# Upload file
aws --endpoint-url http://localhost:9000 s3 cp README.md s3://mybucket/docs/README.md

# List objects
aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket/

# Download file
aws --endpoint-url http://localhost:9000 s3 cp s3://mybucket/docs/README.md out.md

# Copy object
aws --endpoint-url http://localhost:9000 s3 cp s3://mybucket/docs/README.md s3://mybucket/backup/README.md

# Delete object
aws --endpoint-url http://localhost:9000 s3 rm s3://mybucket/docs/README.md

# Multipart upload (automatic for large files)
aws --endpoint-url http://localhost:9000 s3 cp largefile.bin s3://mybucket/largefile.bin
```

## Configuration

```yaml
nats:
  url: "nats://localhost:4222"
  connection_name: "objstore-s3-gw"
  max_reconnects: -1
  reconnect_wait: "2s"

s3:
  listen: ":9000"
  region: "us-east-1"
  host_style: "path"

auth:
  enabled: true
  access_key_id: "nats-s3-admin"
  secret_access_key: "changeme"

buckets:
  auto_create: true
  default_chunk: 131072  # 128KB

observability:
  metrics:
    enabled: true
    listen: ":9090"
    path: "/metrics"
  health:
    enabled: true
    listen: ":8081"
    liveness_path: "/healthz"
    readiness_path: "/readyz"
  logging:
    level: "info"
    format: "json"
```

## Library Usage

```go
package main

import (
    "net/http"

    "github.com/gftdcojp/nats-jetstream-objectstore-s3/pkg/s3gw"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"
    "go.uber.org/zap"
)

func main() {
    nc, _ := nats.Connect("nats://localhost:4222")
    js, _ := jetstream.New(nc)
    logger, _ := zap.NewProduction()

    gw, _ := s3gw.New(s3gw.GatewayConfig{
        NC:               nc,
        JS:               js,
        AccessKeyID:      "mykey",
        SecretAccessKey:   "mysecret",
        Region:           "us-east-1",
        AutoCreateBucket: true,
        Logger:           logger,
    })

    http.ListenAndServe(":9000", gw.Handler())
}
```

## Kubernetes

```bash
kubectl apply -f deploy/kubernetes/deployment.yaml
```

Deploys to `spinkube` namespace with:
- S3 API on port 9000
- Health on port 8081 (`/healthz`, `/readyz`)
- Metrics on port 9090 (`/metrics`)

## Observability

### Prometheus Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `nos3_requests_total` | Counter | `method`, `operation`, `status` |
| `nos3_request_duration_seconds` | Histogram | `operation` |
| `nos3_bytes_received_total` | Counter | `bucket` |
| `nos3_bytes_sent_total` | Counter | `bucket` |
| `nos3_errors_total` | Counter | `code` |

### Health Checks

- `GET :8081/healthz` — liveness (always OK)
- `GET :8081/readyz` — readiness (checks NATS connectivity)

## Building

```bash
make build        # Build binary
make test         # Run tests
make docker       # Build Docker image
make docker-ghcr  # Build and push to GHCR (multi-platform)
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
