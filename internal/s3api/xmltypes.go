package s3api

import "encoding/xml"

const s3Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"

// ListBucketResult is the S3 ListObjectsV2 XML response.
type ListBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	KeyCount              int            `xml:"KeyCount"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	Contents              []S3Object     `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type S3Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         uint64 `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// S3Error is the standard S3 error XML response.
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestId string   `xml:"RequestId"`
}

// InitiateMultipartUploadResult is returned by CreateMultipartUpload.
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// CompleteMultipartUploadResult is returned by CompleteMultipartUpload.
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// CompleteMultipartUploadRequest is the XML body of CompleteMultipartUpload.
type CompleteMultipartUploadRequest struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []CompletePartXML `xml:"Part"`
}

type CompletePartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CopyObjectResult is returned by CopyObject.
type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

// ListPartsResult is returned by ListParts.
type ListPartsResult struct {
	XMLName     xml.Name  `xml:"ListPartsResult"`
	Xmlns       string    `xml:"xmlns,attr"`
	Bucket      string    `xml:"Bucket"`
	Key         string    `xml:"Key"`
	UploadId    string    `xml:"UploadId"`
	IsTruncated bool      `xml:"IsTruncated"`
	MaxParts    int       `xml:"MaxParts"`
	Parts       []PartXML `xml:"Part"`
}

type PartXML struct {
	PartNumber   int    `xml:"PartNumber"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

// ListAllMyBucketsResult is returned by ListBuckets (GET /).
type ListAllMyBucketsResult struct {
	XMLName xml.Name   `xml:"ListAllMyBucketsResult"`
	Xmlns   string     `xml:"xmlns,attr"`
	Owner   S3Owner    `xml:"Owner"`
	Buckets BucketList `xml:"Buckets"`
}

type S3Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type BucketList struct {
	Bucket []BucketXML `xml:"Bucket"`
}

type BucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}
