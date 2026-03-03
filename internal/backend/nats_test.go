package backend_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/backend"
	server "github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

func startTestServer(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natsserver.RunServer(&opts)
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	return ns, nc, js
}

func TestNATSBackend_BucketLifecycle(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	logger := zap.NewNop()

	be := backend.NewNATSBackend(nc, js, 0, true, logger)

	// Create
	err := be.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Head
	err = be.HeadBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}

	// List
	names, err := be.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	found := false
	for _, n := range names {
		if n == "test-bucket" {
			found = true
		}
	}
	if !found {
		t.Fatalf("bucket 'test-bucket' not found in list: %v", names)
	}

	// Delete
	err = be.DeleteBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
}

func TestNATSBackend_ObjectCRUD(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	logger := zap.NewNop()

	be := backend.NewNATSBackend(nc, js, 0, true, logger)

	// Create bucket
	be.CreateBucket(ctx, "objects")

	// Put
	data := []byte("hello world")
	info, err := be.PutObject(ctx, "objects", "greet.txt", bytes.NewReader(data), int64(len(data)), "text/plain", nil)
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if info.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}
	if info.ContentType != "text/plain" {
		t.Fatalf("expected content-type text/plain, got %s", info.ContentType)
	}

	// Get
	reader, ginfo, err := be.GetObject(ctx, "objects", "greet.txt")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, _ := io.ReadAll(reader)
	reader.Close()
	if string(got) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", string(got))
	}
	if ginfo.ETag != info.ETag {
		t.Fatalf("ETag mismatch: %s vs %s", ginfo.ETag, info.ETag)
	}

	// Head
	hinfo, err := be.HeadObject(ctx, "objects", "greet.txt")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if hinfo.Size != uint64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), hinfo.Size)
	}

	// List
	objects, err := be.ListObjects(ctx, "objects")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(objects) != 1 || objects[0].Key != "greet.txt" {
		t.Fatalf("unexpected list result: %+v", objects)
	}

	// Delete
	err = be.DeleteObject(ctx, "objects", "greet.txt")
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
}

func TestNATSBackend_CopyObject(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	logger := zap.NewNop()

	be := backend.NewNATSBackend(nc, js, 0, true, logger)

	be.CreateBucket(ctx, "src")
	be.CreateBucket(ctx, "dst")

	data := []byte("copy me")
	be.PutObject(ctx, "src", "original.txt", bytes.NewReader(data), int64(len(data)), "text/plain", map[string]string{"foo": "bar"})

	info, err := be.CopyObject(ctx, "src", "original.txt", "dst", "copied.txt")
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if info.Key != "copied.txt" {
		t.Fatalf("expected key 'copied.txt', got %q", info.Key)
	}

	reader, _, err := be.GetObject(ctx, "dst", "copied.txt")
	if err != nil {
		t.Fatalf("GetObject copied: %v", err)
	}
	got, _ := io.ReadAll(reader)
	reader.Close()
	if string(got) != "copy me" {
		t.Fatalf("expected 'copy me', got %q", string(got))
	}
}

func TestNATSBackend_AutoCreateBucket(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	logger := zap.NewNop()

	be := backend.NewNATSBackend(nc, js, 0, true, logger)

	// Put to non-existent bucket with auto_create=true
	data := []byte("auto")
	_, err := be.PutObject(ctx, "auto-bucket", "key", bytes.NewReader(data), int64(len(data)), "", nil)
	if err != nil {
		t.Fatalf("PutObject auto-create: %v", err)
	}
}

func TestNATSBackend_MultipartUpload(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	logger := zap.NewNop()

	be := backend.NewNATSBackend(nc, js, 0, true, logger)
	be.CreateBucket(ctx, "multi")

	// Create
	uploadID, err := be.CreateMultipartUpload(ctx, "multi", "bigfile.bin", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	// Upload parts
	part1 := []byte("part-one-data")
	etag1, err := be.UploadPart(ctx, "multi", "bigfile.bin", uploadID, 1, bytes.NewReader(part1), int64(len(part1)))
	if err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}

	part2 := []byte("part-two-data")
	etag2, err := be.UploadPart(ctx, "multi", "bigfile.bin", uploadID, 2, bytes.NewReader(part2), int64(len(part2)))
	if err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}

	// List parts
	parts, err := be.ListParts(ctx, "multi", "bigfile.bin", uploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}

	// Complete
	info, err := be.CompleteMultipartUpload(ctx, "multi", "bigfile.bin", uploadID, []backend.CompletedPart{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	// Verify combined object
	reader, _, err := be.GetObject(ctx, "multi", "bigfile.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, _ := io.ReadAll(reader)
	reader.Close()
	expected := "part-one-datapart-two-data"
	if string(got) != expected {
		t.Fatalf("expected %q, got %q", expected, string(got))
	}
	_ = info
}

func TestNATSBackend_IsConnected(t *testing.T) {
	_, nc, js := startTestServer(t)
	logger := zap.NewNop()
	be := backend.NewNATSBackend(nc, js, 0, false, logger)
	if !be.IsConnected() {
		t.Fatal("expected IsConnected=true")
	}
}
