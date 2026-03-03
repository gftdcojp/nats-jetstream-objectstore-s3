package s3api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/auth"
	"github.com/gftdcojp/nats-jetstream-objectstore-s3/internal/backend"
	"go.uber.org/zap"
)

var reqCounter atomic.Int64

// Router dispatches S3 API requests.
type Router struct {
	backend backend.ObjectStoreBackend
	auth    *auth.SigV4Verifier
	logger  *zap.Logger
	region  string
}

type RouterConfig struct {
	Backend backend.ObjectStoreBackend
	Auth    *auth.SigV4Verifier
	Logger  *zap.Logger
	Region  string
}

func NewRouter(cfg RouterConfig) *Router {
	return &Router{
		backend: cfg.Backend,
		auth:    cfg.Auth,
		logger:  cfg.Logger,
		region:  cfg.Region,
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	reqID := fmt.Sprintf("%d", reqCounter.Add(1))
	w.Header().Set("x-amz-request-id", reqID)

	// Auth check
	if r.auth != nil {
		if err := r.auth.Verify(req); err != nil {
			writeS3Error(w, "SignatureDoesNotMatch", req.URL.Path, reqID)
			return
		}
	}

	// Parse bucket and key from path-style URL
	path := strings.TrimPrefix(req.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)

	bucket := ""
	key := ""
	if len(parts) > 0 {
		bucket = parts[0]
	}
	if len(parts) > 1 {
		key = parts[1]
	}

	// Service-level operations (no bucket)
	if bucket == "" {
		if req.Method == http.MethodGet {
			r.handleListBuckets(w, req, reqID)
			return
		}
		writeS3Error(w, "MethodNotAllowed", "/", reqID)
		return
	}

	// Bucket-level operations (no key)
	if key == "" {
		switch req.Method {
		case http.MethodPut:
			r.handleCreateBucket(w, req, bucket, reqID)
		case http.MethodHead:
			r.handleHeadBucket(w, req, bucket, reqID)
		case http.MethodDelete:
			r.handleDeleteBucket(w, req, bucket, reqID)
		case http.MethodGet:
			if req.URL.Query().Get("list-type") == "2" || req.URL.Query().Has("prefix") {
				r.handleListObjectsV2(w, req, bucket, reqID)
			} else {
				r.handleListObjectsV2(w, req, bucket, reqID)
			}
		default:
			writeS3Error(w, "MethodNotAllowed", "/"+bucket, reqID)
		}
		return
	}

	// Multipart operations
	q := req.URL.Query()
	if q.Has("uploads") && req.Method == http.MethodPost {
		r.handleCreateMultipartUpload(w, req, bucket, key, reqID)
		return
	}
	if q.Has("uploadId") {
		uploadID := q.Get("uploadId")
		switch req.Method {
		case http.MethodPut:
			r.handleUploadPart(w, req, bucket, key, uploadID, reqID)
		case http.MethodPost:
			r.handleCompleteMultipartUpload(w, req, bucket, key, uploadID, reqID)
		case http.MethodDelete:
			r.handleAbortMultipartUpload(w, req, bucket, key, uploadID, reqID)
		case http.MethodGet:
			r.handleListParts(w, req, bucket, key, uploadID, reqID)
		default:
			writeS3Error(w, "MethodNotAllowed", "/"+bucket+"/"+key, reqID)
		}
		return
	}

	// Object-level operations
	switch req.Method {
	case http.MethodGet:
		r.handleGetObject(w, req, bucket, key, reqID)
	case http.MethodPut:
		if req.Header.Get("X-Amz-Copy-Source") != "" {
			r.handleCopyObject(w, req, bucket, key, reqID)
		} else {
			r.handlePutObject(w, req, bucket, key, reqID)
		}
	case http.MethodDelete:
		r.handleDeleteObject(w, req, bucket, key, reqID)
	case http.MethodHead:
		r.handleHeadObject(w, req, bucket, key, reqID)
	default:
		writeS3Error(w, "MethodNotAllowed", "/"+bucket+"/"+key, reqID)
	}
}

func (r *Router) handleListBuckets(w http.ResponseWriter, req *http.Request, reqID string) {
	buckets, err := r.backend.ListBuckets(req.Context())
	if err != nil {
		writeS3Error(w, "InternalError", "/", reqID)
		return
	}
	result := ListAllMyBucketsResult{
		Xmlns: s3Xmlns,
		Owner: S3Owner{ID: "nats-objstore-s3", DisplayName: "nats-objstore-s3"},
	}
	for _, b := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, BucketXML{
			Name:         b,
			CreationDate: time.Now().UTC().Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, result)
}

func (r *Router) handleCreateBucket(w http.ResponseWriter, req *http.Request, bucket, reqID string) {
	err := r.backend.CreateBucket(req.Context(), bucket)
	if err != nil {
		if err == backend.ErrBucketExists {
			writeS3Error(w, "BucketAlreadyOwnedByYou", "/"+bucket, reqID)
			return
		}
		writeS3Error(w, "InternalError", "/"+bucket, reqID)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (r *Router) handleHeadBucket(w http.ResponseWriter, req *http.Request, bucket, reqID string) {
	err := r.backend.HeadBucket(req.Context(), bucket)
	if err != nil {
		if err == backend.ErrBucketNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (r *Router) handleDeleteBucket(w http.ResponseWriter, req *http.Request, bucket, reqID string) {
	err := r.backend.DeleteBucket(req.Context(), bucket)
	if err != nil {
		if err == backend.ErrBucketNotFound {
			writeS3Error(w, "NoSuchBucket", "/"+bucket, reqID)
			return
		}
		writeS3Error(w, "InternalError", "/"+bucket, reqID)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Router) handleListObjectsV2(w http.ResponseWriter, req *http.Request, bucket, reqID string) {
	q := req.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := 1000
	if mk := q.Get("max-keys"); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v > 0 {
			maxKeys = v
		}
	}
	contToken := q.Get("continuation-token")
	startAfter := q.Get("start-after")

	objects, err := r.backend.ListObjects(req.Context(), bucket)
	if err != nil {
		if err == backend.ErrBucketNotFound {
			writeS3Error(w, "NoSuchBucket", "/"+bucket, reqID)
			return
		}
		writeS3Error(w, "InternalError", "/"+bucket, reqID)
		return
	}

	// Filter by prefix
	var filtered []backend.ObjectInfo
	commonPrefixes := make(map[string]bool)
	for _, obj := range objects {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}
		if delimiter != "" {
			rest := obj.Key[len(prefix):]
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				commonPrefixes[prefix+rest[:idx+len(delimiter)]] = true
				continue
			}
		}
		filtered = append(filtered, obj)
	}

	// Sort
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Key < filtered[j].Key })

	// Apply startAfter / contToken
	startKey := startAfter
	if contToken != "" {
		startKey = contToken
	}
	if startKey != "" {
		idx := sort.Search(len(filtered), func(i int) bool { return filtered[i].Key > startKey })
		filtered = filtered[idx:]
	}

	// Paginate
	truncated := len(filtered) > maxKeys
	if truncated {
		filtered = filtered[:maxKeys]
	}

	var cpList []CommonPrefix
	for cp := range commonPrefixes {
		cpList = append(cpList, CommonPrefix{Prefix: cp})
	}
	sort.Slice(cpList, func(i, j int) bool { return cpList[i].Prefix < cpList[j].Prefix })

	result := ListBucketResult{
		Xmlns:             s3Xmlns,
		Name:              bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		IsTruncated:       truncated,
		KeyCount:          len(filtered),
		ContinuationToken: contToken,
		CommonPrefixes:    cpList,
	}
	if truncated && len(filtered) > 0 {
		result.NextContinuationToken = filtered[len(filtered)-1].Key
	}
	for _, obj := range filtered {
		result.Contents = append(result.Contents, S3Object{
			Key:          obj.Key,
			LastModified: obj.LastModified.UTC().Format(time.RFC3339),
			ETag:         `"` + obj.ETag + `"`,
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}
	writeXML(w, http.StatusOK, result)
}

func (r *Router) handleGetObject(w http.ResponseWriter, req *http.Request, bucket, key, reqID string) {
	reader, info, err := r.backend.GetObject(req.Context(), bucket, key)
	if err != nil {
		if err == backend.ErrKeyNotFound {
			writeS3Error(w, "NoSuchKey", "/"+bucket+"/"+key, reqID)
			return
		}
		if err == backend.ErrBucketNotFound {
			writeS3Error(w, "NoSuchBucket", "/"+bucket, reqID)
			return
		}
		writeS3Error(w, "InternalError", "/"+bucket+"/"+key, reqID)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("ETag", `"`+info.ETag+`"`)
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.FormatUint(info.Size, 10))
	for k, v := range info.Metadata {
		w.Header().Set("X-Amz-Meta-"+k, v)
	}
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}

func (r *Router) handlePutObject(w http.ResponseWriter, req *http.Request, bucket, key, reqID string) {
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	metadata := make(map[string]string)
	for k, vals := range req.Header {
		if strings.HasPrefix(k, "X-Amz-Meta-") && len(vals) > 0 {
			metadata[strings.TrimPrefix(k, "X-Amz-Meta-")] = vals[0]
		}
	}

	info, err := r.backend.PutObject(req.Context(), bucket, key, req.Body, req.ContentLength, contentType, metadata)
	if err != nil {
		if err == backend.ErrBucketNotFound {
			writeS3Error(w, "NoSuchBucket", "/"+bucket, reqID)
			return
		}
		r.logger.Error("PutObject failed", zap.Error(err))
		writeS3Error(w, "InternalError", "/"+bucket+"/"+key, reqID)
		return
	}

	w.Header().Set("ETag", `"`+info.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (r *Router) handleDeleteObject(w http.ResponseWriter, req *http.Request, bucket, key, reqID string) {
	r.backend.DeleteObject(req.Context(), bucket, key)
	w.WriteHeader(http.StatusNoContent)
}

func (r *Router) handleHeadObject(w http.ResponseWriter, req *http.Request, bucket, key, reqID string) {
	info, err := r.backend.HeadObject(req.Context(), bucket, key)
	if err != nil {
		if err == backend.ErrKeyNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("Content-Length", strconv.FormatUint(info.Size, 10))
	w.Header().Set("ETag", `"`+info.ETag+`"`)
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (r *Router) handleCopyObject(w http.ResponseWriter, req *http.Request, dstBucket, dstKey, reqID string) {
	src := req.Header.Get("X-Amz-Copy-Source")
	src = strings.TrimPrefix(src, "/")
	parts := strings.SplitN(src, "/", 2)
	if len(parts) != 2 {
		writeS3Error(w, "InvalidArgument", src, reqID)
		return
	}

	info, err := r.backend.CopyObject(req.Context(), parts[0], parts[1], dstBucket, dstKey)
	if err != nil {
		if err == backend.ErrKeyNotFound {
			writeS3Error(w, "NoSuchKey", src, reqID)
			return
		}
		writeS3Error(w, "InternalError", src, reqID)
		return
	}
	writeXML(w, http.StatusOK, CopyObjectResult{
		ETag:         `"` + info.ETag + `"`,
		LastModified: info.LastModified.UTC().Format(time.RFC3339),
	})
}

func (r *Router) handleCreateMultipartUpload(w http.ResponseWriter, req *http.Request, bucket, key, reqID string) {
	contentType := req.Header.Get("Content-Type")
	metadata := make(map[string]string)
	for k, vals := range req.Header {
		if strings.HasPrefix(k, "X-Amz-Meta-") && len(vals) > 0 {
			metadata[strings.TrimPrefix(k, "X-Amz-Meta-")] = vals[0]
		}
	}

	uploadID, err := r.backend.CreateMultipartUpload(req.Context(), bucket, key, contentType, metadata)
	if err != nil {
		writeS3Error(w, "InternalError", "/"+bucket+"/"+key, reqID)
		return
	}
	writeXML(w, http.StatusOK, InitiateMultipartUploadResult{
		Xmlns:    s3Xmlns,
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	})
}

func (r *Router) handleUploadPart(w http.ResponseWriter, req *http.Request, bucket, key, uploadID, reqID string) {
	partStr := req.URL.Query().Get("partNumber")
	partNumber, _ := strconv.Atoi(partStr)
	if partNumber <= 0 {
		writeS3Error(w, "InvalidArgument", "/"+bucket+"/"+key, reqID)
		return
	}

	etag, err := r.backend.UploadPart(req.Context(), bucket, key, uploadID, partNumber, req.Body, req.ContentLength)
	if err != nil {
		if err == backend.ErrUploadNotFound {
			writeS3Error(w, "NoSuchUpload", "/"+bucket+"/"+key, reqID)
			return
		}
		writeS3Error(w, "InternalError", "/"+bucket+"/"+key, reqID)
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (r *Router) handleCompleteMultipartUpload(w http.ResponseWriter, req *http.Request, bucket, key, uploadID, reqID string) {
	var xmlReq CompleteMultipartUploadRequest
	data, err := io.ReadAll(req.Body)
	if err != nil {
		writeS3Error(w, "MalformedXML", "/"+bucket+"/"+key, reqID)
		return
	}
	if err := xml.Unmarshal(data, &xmlReq); err != nil {
		writeS3Error(w, "MalformedXML", "/"+bucket+"/"+key, reqID)
		return
	}

	var parts []backend.CompletedPart
	for _, p := range xmlReq.Parts {
		parts = append(parts, backend.CompletedPart{
			PartNumber: p.PartNumber,
			ETag:       strings.Trim(p.ETag, `"`),
		})
	}

	info, err := r.backend.CompleteMultipartUpload(req.Context(), bucket, key, uploadID, parts)
	if err != nil {
		if err == backend.ErrUploadNotFound {
			writeS3Error(w, "NoSuchUpload", "/"+bucket+"/"+key, reqID)
			return
		}
		writeS3Error(w, "InternalError", "/"+bucket+"/"+key, reqID)
		return
	}
	writeXML(w, http.StatusOK, CompleteMultipartUploadResult{
		Xmlns:    s3Xmlns,
		Location: "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     `"` + info.ETag + `"`,
	})
}

func (r *Router) handleAbortMultipartUpload(w http.ResponseWriter, req *http.Request, bucket, key, uploadID, reqID string) {
	err := r.backend.AbortMultipartUpload(req.Context(), bucket, key, uploadID)
	if err != nil {
		if err == backend.ErrUploadNotFound {
			writeS3Error(w, "NoSuchUpload", "/"+bucket+"/"+key, reqID)
			return
		}
		writeS3Error(w, "InternalError", "/"+bucket+"/"+key, reqID)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Router) handleListParts(w http.ResponseWriter, req *http.Request, bucket, key, uploadID, reqID string) {
	parts, err := r.backend.ListParts(req.Context(), bucket, key, uploadID)
	if err != nil {
		if err == backend.ErrUploadNotFound {
			writeS3Error(w, "NoSuchUpload", "/"+bucket+"/"+key, reqID)
			return
		}
		writeS3Error(w, "InternalError", "/"+bucket+"/"+key, reqID)
		return
	}
	result := ListPartsResult{
		Xmlns:    s3Xmlns,
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
		MaxParts: 1000,
	}
	for _, p := range parts {
		result.Parts = append(result.Parts, PartXML{
			PartNumber:   p.PartNumber,
			ETag:         `"` + p.ETag + `"`,
			Size:         p.Size,
			LastModified: p.LastModified.UTC().Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, result)
}

func writeXML(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(v)
}
