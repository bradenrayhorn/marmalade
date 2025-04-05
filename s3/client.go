package s3

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	Endpoint   string
	Region     string
	AccessKey  string
	SecretKey  string
	BucketName string

	Insecure bool

	httpClient *http.Client
}

func NewClient(endpoint, region, accessKey, secretKey, bucketName string) *Client {
	return &Client{
		Endpoint:   endpoint,
		Region:     region,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		BucketName: bucketName,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

type Object struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         int64
	StorageClass string
}

type ObjectIdentifier struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

func (c *Client) buildURL(key string, query url.Values) string {
	path := fmt.Sprintf("/%s", c.BucketName)
	if key != "" {
		path = fmt.Sprintf("%s/%s", path, url.PathEscape(key))
	}

	scheme := "https"
	if c.Insecure {
		scheme = "http"
	}

	u := url.URL{
		Scheme: scheme,
		Host:   c.Endpoint,
		Path:   path,
	}

	if query != nil {
		u.RawQuery = query.Encode()
	}

	return u.String()
}
