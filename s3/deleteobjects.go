package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type DeleteObjectsRequest struct {
	XMLName xml.Name           `xml:"Delete"`
	Objects []ObjectIdentifier `xml:"Object"`
	Quiet   bool               `xml:"Quiet"`
}

type DeleteObjectsResult struct {
	Deleted []DeletedObject `xml:"Deleted"`
	Error   []DeletedError  `xml:"Error"`
}

type DeletedObject struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

type DeletedError struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
}

func (c *Client) DeleteObjects(objects []ObjectIdentifier) (*DeleteObjectsResult, error) {
	query := url.Values{}
	query.Set("delete", "")
	reqURL := c.buildURL("", query)

	deleteReq := DeleteObjectsRequest{
		Objects: objects,
		Quiet:   true,
	}

	data, err := xml.Marshal(deleteReq)
	if err != nil {
		return nil, err
	}

	return withRetries(func() (*DeleteObjectsResult, error) {
		bodyReader := bytes.NewReader(data)
		req, err := http.NewRequest(http.MethodPost, reqURL, bodyReader)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/xml")
		req.ContentLength = int64(len(data))

		md5sum := getMD5Sum(data)
		req.Header.Set("Content-MD5", md5sum)

		if err := c.signV4(req, bytes.NewReader(data)); err != nil {
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, retriableError{err}
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			err := fmt.Errorf("DeleteObjects failed with status: %s, response: %s", resp.Status, string(body))

			if resp.StatusCode >= 500 {
				return nil, retriableError{err}
			} else {
				return nil, err
			}
		}

		result := &DeleteObjectsResult{}
		if err := xml.NewDecoder(resp.Body).Decode(result); err != nil {
			return nil, err
		}

		return result, nil
	})
}
