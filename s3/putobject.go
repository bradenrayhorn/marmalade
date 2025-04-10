package s3

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"time"
)

func (c *Client) PutObject(key string, data io.ReadSeeker, dataLength int64, retention *ObjectLockRetention) error {
	reqURL := c.buildURL(key, nil)

	req, err := http.NewRequest(http.MethodPut, reqURL, data)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = dataLength

	if retention != nil {
		req.Header.Set("x-amz-object-lock-mode", retention.Mode)
		req.Header.Set("x-amz-object-lock-retain-until-date", retention.Until.Format(time.RFC3339))
	}

	if c.storageClass != "" {
		req.Header.Set("x-amz-storage-class", c.storageClass)
	}

	// compute md5 hash
	hash := md5.New()
	if _, err := io.Copy(hash, data); err != nil {
		return err
	}
	hashSum := hash.Sum(nil)
	if _, err := data.Seek(0, io.SeekStart); err != nil {
		return err
	}

	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(hashSum[:]))

	// sign and send request
	if err := c.signV4(req, data); err != nil {
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
