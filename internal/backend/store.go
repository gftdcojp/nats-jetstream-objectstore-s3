package backend

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrBucketExists   = errors.New("bucket already exists")
	ErrKeyNotFound    = errors.New("key not found")
	ErrBucketNotEmpty = errors.New("bucket not empty")
	ErrUploadNotFound = errors.New("upload not found")
)

// ObjectInfo represents metadata about a stored object.
type ObjectInfo struct {
	Bucket       string
	Key          string
	Size         uint64
	ContentType  string
	ETag         string
	LastModified time.Time
	Metadata     map[string]string
}

// CompletedPart describes a completed multipart upload part.
type CompletedPart struct {
	PartNumber int
	ETag       string
}

// PartInfo describes an uploaded part.
type PartInfo struct {
	PartNumber   int
	Size         int64
	ETag         string
	LastModified time.Time
}

// ObjectStoreBackend abstracts NATS ObjectStore operations for S3 API mapping.
type ObjectStoreBackend interface {
	// Bucket operations
	CreateBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	HeadBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]string, error)

	// Object operations
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error)
	PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (*ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	HeadObject(ctx context.Context, bucket, key string) (*ObjectInfo, error)
	CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (*ObjectInfo, error)

	// List
	ListObjects(ctx context.Context, bucket string) ([]ObjectInfo, error)

	// Multipart
	CreateMultipartUpload(ctx context.Context, bucket, key, contentType string, metadata map[string]string) (uploadID string, err error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader, size int64) (etag string, err error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []CompletedPart) (*ObjectInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
	ListParts(ctx context.Context, bucket, key, uploadID string) ([]PartInfo, error)

	// Health
	IsConnected() bool
}
