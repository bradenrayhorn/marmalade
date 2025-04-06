package s3

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

func (c *Client) signV4(req *http.Request, body io.ReadSeeker) error {
	parsedURL, err := url.Parse(req.URL.String())
	if err != nil {
		return err
	}

	// Time used for signature
	t := time.Now().UTC()
	amzDate := t.Format("20060102T150405Z")
	dateStamp := t.Format("20060102")

	// Set required headers
	req.Header.Set("x-amz-date", amzDate)

	// Calculate hash of request body
	var bodyHash string
	if body == nil {
		bodyHash = emptyStringSHA256
	} else {
		h := sha256.New()
		if _, err := io.Copy(h, body); err != nil {
			return err
		}
		bodyHash = hex.EncodeToString(h.Sum(nil))

		// Reset the body reader so it can be read again
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	req.Header.Set("x-amz-content-sha256", bodyHash)

	// Create canonical URI
	canonicalURI := parsedURL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Create canonical query string
	canonicalQueryString := parsedURL.RawQuery

	// Create canonical headers
	canonicalHeaders := ""
	signedHeaders := ""

	// Get all headers
	headers := make(map[string][]string)
	for k, v := range req.Header {
		lowerK := strings.ToLower(k)
		headers[lowerK] = v
	}
	// Add host header
	headers["host"] = []string{parsedURL.Host}

	// Sort headers by key
	var headerKeys []string
	for k := range headers {
		headerKeys = append(headerKeys, k)
	}
	sort.Strings(headerKeys)

	// Build canonical headers and signed headers
	for i, k := range headerKeys {
		canonicalHeaders += k + ":" + strings.Join(headers[k], ",") + "\n"
		if i > 0 {
			signedHeaders += ";"
		}
		signedHeaders += k
	}

	// Create canonical request
	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		bodyHash)

	// Create string to sign
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, c.region)

	h := sha256.New()
	h.Write([]byte(canonicalRequest))
	canonicalRequestHash := hex.EncodeToString(h.Sum(nil))

	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s",
		algorithm,
		amzDate,
		credentialScope,
		canonicalRequestHash)

	// Calculate signature
	kDate := hmacSHA256([]byte("AWS4"+c.secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(c.region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	// Add Authorization header
	authHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm,
		c.accessKey,
		credentialScope,
		signedHeaders,
		signature)

	req.Header.Set("Authorization", authHeader)

	return nil
}

const emptyStringSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func getMD5Sum(data []byte) string {
	h := md5.New()
	h.Write(data)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
