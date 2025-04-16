package s3

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ObjectLockRetention struct {
	Mode  string // GOVERNANCE or COMPLIANCE
	Until time.Time
}

func (c *Client) PutObjectRetention(key string, retention *ObjectLockRetention) error {
	query := url.Values{}
	query.Set("retention", "")
	reqURL := c.buildURL(key, query)

	retentionXML := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Mode>%s</Mode>
  <RetainUntilDate>%s</RetainUntilDate>
</Retention>`, retention.Mode, retention.Until.Format(time.RFC3339))

	_, err := withRetries(func() (any, error) {
		bodyReader := strings.NewReader(retentionXML)

		req, err := http.NewRequest(http.MethodPut, reqURL, bodyReader)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/xml")
		req.ContentLength = int64(len(retentionXML))

		md5Sum := md5.Sum([]byte(retentionXML))
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5Sum[:]))

		if err := c.signV4(req, strings.NewReader(retentionXML)); err != nil {
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			err := fmt.Errorf("PutObjectRetention failed with status: %s, response: %s", resp.Status, string(body))

			if resp.StatusCode >= 500 {
				return nil, retriableError{err}
			} else {
				return nil, err
			}
		}

		return nil, nil
	})

	return err
}
