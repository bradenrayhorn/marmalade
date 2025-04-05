package fakes3

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"
)

func (s *FakeS3) handlePutObjectRetention(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusBadRequest)
		return
	}

	// Parse the retention XML
	var retentionReq struct {
		XMLName         xml.Name  `xml:"Retention"`
		Mode            string    `xml:"Mode"`
		RetainUntilDate time.Time `xml:"RetainUntilDate"`
	}

	if err := xml.Unmarshal(body, &retentionReq); err != nil {
		http.Error(w, fmt.Sprintf("Error parsing XML: %v", err), http.StatusBadRequest)
		return
	}

	if retentionReq.RetainUntilDate.Before(s.now) {
		http.Error(w, "RetainUntil must be after now", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	versions, exists := s.objects[key]
	if !exists {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}

	versionID := r.URL.Query().Get("versionId")

	var obj *ObjectVersion
	if versionID != "" {
		obj = versions[versionID]
	} else {
		// Get latest non-delete-marker version
		var latest *ObjectVersion
		var latestTime time.Time
		for _, v := range versions {
			if !v.DeleteMarker && (latest == nil || v.LastModified.After(latestTime)) {
				latestTime = v.LastModified
				latest = v
			}
		}
		obj = latest
	}

	if obj == nil {
		http.Error(w, "NoSuchVersion", http.StatusNotFound)
		return
	}

	obj.Retention = &ObjectLockRetention{
		Mode:  retentionReq.Mode,
		Until: retentionReq.RetainUntilDate,
	}
}
