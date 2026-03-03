package auth_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/auth"
)

func TestSigV4_PresignedURL(t *testing.T) {
	v := auth.NewSigV4Verifier("AKID", "SECRET", "us-east-1")

	url := v.GeneratePresignedURL("GET", "localhost:9000", "mybucket", "mykey.txt", 3600*time.Second)
	if !strings.Contains(url, "X-Amz-Algorithm=AWS4-HMAC-SHA256") {
		t.Fatalf("presigned URL missing algorithm: %s", url)
	}
	if !strings.Contains(url, "X-Amz-Signature=") {
		t.Fatalf("presigned URL missing signature: %s", url)
	}
	if !strings.Contains(url, "/mybucket/mykey.txt") {
		t.Fatalf("presigned URL missing path: %s", url)
	}
}

func TestSigV4_MissingAuth(t *testing.T) {
	v := auth.NewSigV4Verifier("AKID", "SECRET", "us-east-1")
	r, _ := http.NewRequest("GET", "http://localhost:9000/bucket/key", nil)
	err := v.Verify(r)
	if err == nil {
		t.Fatal("expected error for missing auth")
	}
	if !strings.Contains(err.Error(), "missing Authorization") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSigV4_UnsupportedAlgorithm(t *testing.T) {
	v := auth.NewSigV4Verifier("AKID", "SECRET", "us-east-1")
	r, _ := http.NewRequest("GET", "http://localhost:9000/bucket/key", nil)
	r.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	err := v.Verify(r)
	if err == nil {
		t.Fatal("expected error for unsupported algorithm")
	}
}

func TestSigV4_InvalidAccessKey(t *testing.T) {
	v := auth.NewSigV4Verifier("AKID", "SECRET", "us-east-1")
	r, _ := http.NewRequest("GET", "http://localhost:9000/bucket/key", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=WRONG/20260303/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123")
	err := v.Verify(r)
	if err == nil {
		t.Fatal("expected error for invalid access key")
	}
	if !strings.Contains(err.Error(), "invalid access key") {
		t.Fatalf("unexpected error: %v", err)
	}
}
