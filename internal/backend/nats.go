package backend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// NATSBackend implements ObjectStoreBackend using NATS JetStream ObjectStore.
type NATSBackend struct {
	nc           *nats.Conn
	js           jetstream.JetStream
	bucketCache  map[string]jetstream.ObjectStore
	mu           sync.RWMutex
	defaultChunk int
	autoCreate   bool
	logger       *zap.Logger
}

func NewNATSBackend(nc *nats.Conn, js jetstream.JetStream, defaultChunk int, autoCreate bool, logger *zap.Logger) *NATSBackend {
	if defaultChunk <= 0 {
		defaultChunk = 131072
	}
	return &NATSBackend{
		nc:           nc,
		js:           js,
		bucketCache:  make(map[string]jetstream.ObjectStore),
		defaultChunk: defaultChunk,
		autoCreate:   autoCreate,
		logger:       logger,
	}
}

func (b *NATSBackend) getObjStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error) {
	b.mu.RLock()
	obs, ok := b.bucketCache[bucket]
	b.mu.RUnlock()
	if ok {
		return obs, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Double check
	if obs, ok := b.bucketCache[bucket]; ok {
		return obs, nil
	}

	obs, err := b.js.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, err
	}
	b.bucketCache[bucket] = obs
	return obs, nil
}

func (b *NATSBackend) CreateBucket(ctx context.Context, bucket string) error {
	cfg := jetstream.ObjectStoreConfig{
		Bucket: bucket,
	}
	obs, err := b.js.CreateObjectStore(ctx, cfg)
	if err != nil {
		if strings.Contains(err.Error(), "already in use") {
			return ErrBucketExists
		}
		return err
	}
	b.mu.Lock()
	b.bucketCache[bucket] = obs
	b.mu.Unlock()
	return nil
}

func (b *NATSBackend) DeleteBucket(ctx context.Context, bucket string) error {
	err := b.js.DeleteObjectStore(ctx, bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	b.mu.Lock()
	delete(b.bucketCache, bucket)
	b.mu.Unlock()
	return nil
}

func (b *NATSBackend) HeadBucket(ctx context.Context, bucket string) error {
	_, err := b.getObjStore(ctx, bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	return nil
}

func (b *NATSBackend) ListBuckets(ctx context.Context) ([]string, error) {
	names := b.js.ObjectStoreNames(ctx)
	var result []string
	for name := range names.Name() {
		result = append(result, name)
	}
	if err := names.Error(); err != nil {
		return result, err
	}
	return result, nil
}

func (b *NATSBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error) {
	obs, err := b.getObjStore(ctx, bucket)
	if err != nil {
		return nil, nil, ErrBucketNotFound
	}
	result, err := obs.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, nil, ErrKeyNotFound
		}
		return nil, nil, err
	}
	nfo, err := result.Info()
	if err != nil {
		result.Close()
		return nil, nil, err
	}
	info := &ObjectInfo{
		Bucket:       bucket,
		Key:          key,
		Size:         nfo.Size,
		LastModified: nfo.ModTime,
		ContentType:  headerVal(nfo.Headers, "Content-Type", "application/octet-stream"),
		ETag:         headerVal(nfo.Headers, "X-Nos3-Etag", ""),
		Metadata:     extractMetadata(nfo.Headers),
	}
	return result, info, nil
}

func (b *NATSBackend) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (*ObjectInfo, error) {
	obs, err := b.getObjStore(ctx, bucket)
	if err != nil {
		if !b.autoCreate {
			return nil, ErrBucketNotFound
		}
		if cerr := b.CreateBucket(ctx, bucket); cerr != nil && !errors.Is(cerr, ErrBucketExists) {
			return nil, cerr
		}
		obs, err = b.getObjStore(ctx, bucket)
		if err != nil {
			return nil, err
		}
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}

	// Compute ETag (MD5)
	hash := md5.Sum(data)
	etag := hex.EncodeToString(hash[:])

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	headers := nats.Header{}
	headers.Set("Content-Type", contentType)
	headers.Set("X-Nos3-Etag", etag)
	for k, v := range metadata {
		headers.Set("X-Amz-Meta-"+k, v)
	}

	meta := jetstream.ObjectMeta{
		Name:    key,
		Headers: headers,
	}

	nfo, err := obs.Put(ctx, meta, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Bucket:       bucket,
		Key:          key,
		Size:         nfo.Size,
		ContentType:  contentType,
		ETag:         etag,
		LastModified: nfo.ModTime,
		Metadata:     metadata,
	}, nil
}

func (b *NATSBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	obs, err := b.getObjStore(ctx, bucket)
	if err != nil {
		return ErrBucketNotFound
	}
	return obs.Delete(ctx, key)
}

func (b *NATSBackend) HeadObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	obs, err := b.getObjStore(ctx, bucket)
	if err != nil {
		return nil, ErrBucketNotFound
	}
	nfo, err := obs.GetInfo(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return &ObjectInfo{
		Bucket:       bucket,
		Key:          key,
		Size:         nfo.Size,
		LastModified: nfo.ModTime,
		ContentType:  headerVal(nfo.Headers, "Content-Type", "application/octet-stream"),
		ETag:         headerVal(nfo.Headers, "X-Nos3-Etag", ""),
		Metadata:     extractMetadata(nfo.Headers),
	}, nil
}

func (b *NATSBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (*ObjectInfo, error) {
	reader, info, err := b.GetObject(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return b.PutObject(ctx, dstBucket, dstKey, bytes.NewReader(data), int64(len(data)), info.ContentType, info.Metadata)
}

func (b *NATSBackend) ListObjects(ctx context.Context, bucket string) ([]ObjectInfo, error) {
	obs, err := b.getObjStore(ctx, bucket)
	if err != nil {
		return nil, ErrBucketNotFound
	}
	infos, err := obs.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil, nil
		}
		return nil, err
	}
	var result []ObjectInfo
	for _, nfo := range infos {
		if nfo.Deleted {
			continue
		}
		result = append(result, ObjectInfo{
			Bucket:       bucket,
			Key:          nfo.Name,
			Size:         nfo.Size,
			LastModified: nfo.ModTime,
			ContentType:  headerVal(nfo.Headers, "Content-Type", "application/octet-stream"),
			ETag:         headerVal(nfo.Headers, "X-Nos3-Etag", ""),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

// Multipart upload support using in-memory storage for parts.
// In production, this should use NATS KV for durability.

type multipartUpload struct {
	Bucket      string
	Key         string
	ContentType string
	Metadata    map[string]string
	Parts       map[int][]byte
	CreatedAt   time.Time
}

var (
	uploads   = make(map[string]*multipartUpload)
	uploadsMu sync.Mutex
	uploadSeq int64
)

func (b *NATSBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string, metadata map[string]string) (string, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		if !b.autoCreate {
			return "", err
		}
		b.CreateBucket(ctx, bucket)
	}
	uploadsMu.Lock()
	uploadSeq++
	id := fmt.Sprintf("%s-%s-%d", bucket, key, uploadSeq)
	uploads[id] = &multipartUpload{
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		Metadata:    metadata,
		Parts:       make(map[int][]byte),
		CreatedAt:   time.Now(),
	}
	uploadsMu.Unlock()
	return id, nil
}

func (b *NATSBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader, size int64) (string, error) {
	uploadsMu.Lock()
	up, ok := uploads[uploadID]
	uploadsMu.Unlock()
	if !ok {
		return "", ErrUploadNotFound
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	hash := md5.Sum(data)
	etag := hex.EncodeToString(hash[:])

	uploadsMu.Lock()
	up.Parts[partNumber] = data
	uploadsMu.Unlock()

	return etag, nil
}

func (b *NATSBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []CompletedPart) (*ObjectInfo, error) {
	uploadsMu.Lock()
	up, ok := uploads[uploadID]
	if ok {
		delete(uploads, uploadID)
	}
	uploadsMu.Unlock()
	if !ok {
		return nil, ErrUploadNotFound
	}

	// Sort parts by number and concatenate
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })

	var buf bytes.Buffer
	for _, p := range parts {
		data, ok := up.Parts[p.PartNumber]
		if !ok {
			return nil, fmt.Errorf("part %d not found", p.PartNumber)
		}
		buf.Write(data)
	}

	return b.PutObject(ctx, bucket, key, &buf, int64(buf.Len()), up.ContentType, up.Metadata)
}

func (b *NATSBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	uploadsMu.Lock()
	_, ok := uploads[uploadID]
	if ok {
		delete(uploads, uploadID)
	}
	uploadsMu.Unlock()
	if !ok {
		return ErrUploadNotFound
	}
	return nil
}

func (b *NATSBackend) ListParts(ctx context.Context, bucket, key, uploadID string) ([]PartInfo, error) {
	uploadsMu.Lock()
	up, ok := uploads[uploadID]
	uploadsMu.Unlock()
	if !ok {
		return nil, ErrUploadNotFound
	}

	var result []PartInfo
	for num, data := range up.Parts {
		hash := md5.Sum(data)
		result = append(result, PartInfo{
			PartNumber:   num,
			Size:         int64(len(data)),
			ETag:         hex.EncodeToString(hash[:]),
			LastModified: up.CreatedAt,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PartNumber < result[j].PartNumber })
	return result, nil
}

func (b *NATSBackend) IsConnected() bool {
	return b.nc.IsConnected()
}

// helpers

func headerVal(h nats.Header, key, def string) string {
	if h == nil {
		return def
	}
	v := h.Get(key)
	if v == "" {
		return def
	}
	return v
}

func extractMetadata(h nats.Header) map[string]string {
	if h == nil {
		return nil
	}
	meta := make(map[string]string)
	for k, vals := range h {
		if strings.HasPrefix(k, "X-Amz-Meta-") && len(vals) > 0 {
			meta[strings.TrimPrefix(k, "X-Amz-Meta-")] = vals[0]
		}
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

// jsonMarshal is a helper for JSON encoding without html escaping.
func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
