package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

type s3ErrorDef struct {
	StatusCode int
	Message    string
}

var s3Errors = map[string]s3ErrorDef{
	"AccessDenied":            {http.StatusForbidden, "Access Denied"},
	"BucketAlreadyExists":     {http.StatusConflict, "The requested bucket name is not available"},
	"BucketAlreadyOwnedByYou": {http.StatusConflict, "Your previous request to create the named bucket succeeded"},
	"BucketNotEmpty":          {http.StatusConflict, "The bucket you tried to delete is not empty"},
	"EntityTooLarge":          {http.StatusBadRequest, "Your proposed upload exceeds the maximum allowed object size"},
	"InternalError":           {http.StatusInternalServerError, "We encountered an internal error"},
	"InvalidArgument":         {http.StatusBadRequest, "Invalid Argument"},
	"InvalidBucketName":       {http.StatusBadRequest, "The specified bucket is not valid"},
	"InvalidPart":             {http.StatusBadRequest, "One or more of the specified parts could not be found"},
	"InvalidPartOrder":        {http.StatusBadRequest, "The list of parts was not in ascending order"},
	"MalformedXML":            {http.StatusBadRequest, "The XML you provided was not well-formed"},
	"MethodNotAllowed":        {http.StatusMethodNotAllowed, "The specified method is not allowed against this resource"},
	"NoSuchBucket":            {http.StatusNotFound, "The specified bucket does not exist"},
	"NoSuchKey":               {http.StatusNotFound, "The specified key does not exist"},
	"NoSuchUpload":            {http.StatusNotFound, "The specified multipart upload does not exist"},
	"NotImplemented":          {http.StatusNotImplemented, "A header you provided implies functionality that is not implemented"},
	"SignatureDoesNotMatch":   {http.StatusForbidden, "The request signature we calculated does not match the signature you provided"},
}

func writeS3Error(w http.ResponseWriter, code, resource, requestID string) {
	def, ok := s3Errors[code]
	if !ok {
		def = s3ErrorDef{http.StatusInternalServerError, code}
	}

	errResp := S3Error{
		Code:      code,
		Message:   def.Message,
		Resource:  resource,
		RequestId: requestID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(def.StatusCode)
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(errResp)
}

func writeS3ErrorMsg(w http.ResponseWriter, code, message, resource string) {
	def, ok := s3Errors[code]
	if !ok {
		def = s3ErrorDef{http.StatusInternalServerError, code}
	}
	if message == "" {
		message = def.Message
	}

	errResp := S3Error{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestId: fmt.Sprintf("%d", reqCounter.Add(1)),
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(def.StatusCode)
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(errResp)
}
