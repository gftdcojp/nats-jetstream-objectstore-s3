package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// SigV4Verifier validates AWS Signature Version 4 authentication.
type SigV4Verifier struct {
	accessKeyID     string
	secretAccessKey string
	region          string
	service         string
}

func NewSigV4Verifier(accessKeyID, secretAccessKey, region string) *SigV4Verifier {
	return &SigV4Verifier{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		region:          region,
		service:         "s3",
	}
}

// Verify checks the Authorization header for valid SigV4.
func (v *SigV4Verifier) Verify(r *http.Request) error {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		// Check for presigned URL
		if r.URL.Query().Get("X-Amz-Algorithm") != "" {
			return v.verifyPresigned(r)
		}
		return fmt.Errorf("missing Authorization header")
	}

	// Parse: AWS4-HMAC-SHA256 Credential=AKID/date/region/s3/aws4_request, SignedHeaders=..., Signature=...
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return fmt.Errorf("unsupported auth algorithm")
	}

	parts := strings.SplitN(authHeader[len("AWS4-HMAC-SHA256 "):], ", ", 3)
	if len(parts) != 3 {
		return fmt.Errorf("malformed Authorization header")
	}

	// Parse credential
	credStr := strings.TrimPrefix(parts[0], "Credential=")
	credParts := strings.Split(credStr, "/")
	if len(credParts) != 5 {
		return fmt.Errorf("invalid credential scope")
	}
	if credParts[0] != v.accessKeyID {
		return fmt.Errorf("invalid access key")
	}
	dateStamp := credParts[1]

	// Parse signed headers
	signedHeadersStr := strings.TrimPrefix(parts[1], "SignedHeaders=")
	signedHeaders := strings.Split(signedHeadersStr, ";")

	// Parse provided signature
	providedSig := strings.TrimPrefix(parts[2], "Signature=")

	// Get payload hash
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	// Build canonical request
	canonicalRequest := v.buildCanonicalRequest(r, signedHeaders, payloadHash)

	// Build string to sign
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = time.Now().UTC().Format("20060102T150405Z")
	}
	scope := dateStamp + "/" + v.region + "/" + v.service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hashSHA256([]byte(canonicalRequest))

	// Compute signature
	signingKey := v.deriveSigningKey(dateStamp)
	expected := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	if !hmac.Equal([]byte(expected), []byte(providedSig)) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func (v *SigV4Verifier) verifyPresigned(r *http.Request) error {
	q := r.URL.Query()
	credential := q.Get("X-Amz-Credential")
	credParts := strings.Split(credential, "/")
	if len(credParts) != 5 || credParts[0] != v.accessKeyID {
		return fmt.Errorf("invalid credential")
	}

	// Check expiry
	amzDate := q.Get("X-Amz-Date")
	expires := q.Get("X-Amz-Expires")
	if amzDate != "" && expires != "" {
		t, err := time.Parse("20060102T150405Z", amzDate)
		if err != nil {
			return fmt.Errorf("invalid date")
		}
		var expSecs int
		fmt.Sscanf(expires, "%d", &expSecs)
		if time.Since(t) > time.Duration(expSecs)*time.Second {
			return fmt.Errorf("presigned URL expired")
		}
	}

	return nil // simplified presigned verification
}

func (v *SigV4Verifier) buildCanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) string {
	// Canonical URI
	uri := r.URL.Path
	if uri == "" {
		uri = "/"
	}

	// Canonical query string (sorted)
	params := r.URL.Query()
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var qParts []string
	for _, k := range keys {
		for _, v := range params[k] {
			qParts = append(qParts, k+"="+v)
		}
	}
	canonicalQuery := strings.Join(qParts, "&")

	// Canonical headers
	sort.Strings(signedHeaders)
	var headerParts []string
	for _, h := range signedHeaders {
		val := strings.TrimSpace(r.Header.Get(h))
		if h == "host" {
			val = r.Host
		}
		headerParts = append(headerParts, h+":"+val+"\n")
	}

	return r.Method + "\n" +
		uri + "\n" +
		canonicalQuery + "\n" +
		strings.Join(headerParts, "") + "\n" +
		strings.Join(signedHeaders, ";") + "\n" +
		payloadHash
}

func (v *SigV4Verifier) deriveSigningKey(dateStamp string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+v.secretAccessKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(v.region))
	kService := hmacSHA256(kRegion, []byte(v.service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

// GeneratePresignedURL generates a presigned URL for GET or PUT operations.
func (v *SigV4Verifier) GeneratePresignedURL(method, host, bucket, key string, expires time.Duration) string {
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credential := v.accessKeyID + "/" + dateStamp + "/" + v.region + "/" + v.service + "/aws4_request"

	path := "/" + bucket + "/" + key
	expSecs := int(expires.Seconds())

	query := fmt.Sprintf("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%d&X-Amz-SignedHeaders=host",
		credential, amzDate, expSecs)

	canonicalRequest := method + "\n" + path + "\n" + query + "\n" + "host:" + host + "\n\nhost\nUNSIGNED-PAYLOAD"
	scope := dateStamp + "/" + v.region + "/" + v.service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hashSHA256([]byte(canonicalRequest))

	signingKey := v.deriveSigningKey(dateStamp)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	return "http://" + host + path + "?" + query + "&X-Amz-Signature=" + signature
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hashSHA256(data []byte) string {
	h := sha256.New()
	io.Copy(h, strings.NewReader(string(data)))
	return hex.EncodeToString(h.Sum(nil))
}
