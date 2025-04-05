package s3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"time"
)

func (c *Client) PutObject(key string, data []byte, retention *ObjectLockRetention) error {
	reqURL := c.buildURL(key, nil)
	bodyReader := bytes.NewReader(data)

	req, err := http.NewRequest(http.MethodPut, reqURL, bodyReader)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))

	if retention != nil {
		req.Header.Set("x-amz-object-lock-mode", retention.Mode)
		req.Header.Set("x-amz-object-lock-retain-until-date", retention.Until.Format(time.RFC3339))
	}

	md5Sum := md5.Sum(data)
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5Sum[:]))

	if err := c.signV4(req, bytes.NewReader(data)); err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PutObject failed with status: %s, response: %s", resp.Status, string(body))
	}

	return nil
}
