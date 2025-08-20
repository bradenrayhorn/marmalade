package s3

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	endpoint     string
	region       string
	accessKey    string
	secretKey    string
	bucketName   string
	storageClass string

	insecure bool

	httpClient *http.Client
}

func NewClient(config Config) *Client {
	return &Client{
		endpoint:     config.URL,
		region:       config.Region,
		accessKey:    config.KeyID,
		secretKey:    config.KeySecret,
		bucketName:   config.Bucket,
		storageClass: config.StorageClass,
		insecure:     config.Insecure,
		httpClient:   &http.Client{Timeout: 600 * time.Second},
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
	path := fmt.Sprintf("/%s", c.bucketName)
	if key != "" {
		path = fmt.Sprintf("%s/%s", path, url.PathEscape(key))
	}

	scheme := "https"
	if c.insecure {
		scheme = "http"
	}

	u := url.URL{
		Scheme: scheme,
		Host:   c.endpoint,
		Path:   path,
	}

	if query != nil {
		u.RawQuery = query.Encode()
	}

	return u.String()
}
