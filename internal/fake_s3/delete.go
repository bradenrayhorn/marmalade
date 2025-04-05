package fakes3

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
)

type deleteVersionsResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Xmlns   string          `xml:"xmlns,attr"`
	Deleted []deletedObject `xml:"Deleted,omitempty"`
	Error   []deletedError  `xml:"Error,omitempty"`
}

type deletedObject struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

type deletedError struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
}

func (s *FakeS3) handleDeleteObjects(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusBadRequest)
		return
	}

	var deleteReq struct {
		XMLName xml.Name `xml:"Delete"`
		Quiet   bool     `xml:"Quiet"`
		Object  []struct {
			Key       string `xml:"Key"`
			VersionID string `xml:"VersionId,omitempty"`
		} `xml:"Object"`
	}

	if err := xml.Unmarshal(body, &deleteReq); err != nil {
		http.Error(w, fmt.Sprintf("Error parsing XML: %v", err), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	result := deleteVersionsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
	}

	for _, obj := range deleteReq.Object {
		key := obj.Key
		versionID := obj.VersionID

		if versions, exists := s.objects[key]; exists {
			if versionID != "" {
				// Delete specific version
				if version, versionExists := versions[versionID]; versionExists {
					if version.Retention != nil && version.Retention.Until.After(s.now) {
						result.Error = append(result.Error, deletedError{
							Key:       key,
							VersionID: versionID,
							Code:      "ObjectLocked",
							Message:   "Object is locked",
						})
						continue
					}

					delete(versions, versionID)
					if !deleteReq.Quiet {
						result.Deleted = append(result.Deleted, deletedObject{
							Key:       key,
							VersionID: versionID,
						})
					}
				}
			} else {
				// Version not specified, create delete marker
				deleteMarker := &ObjectVersion{
					Key:          key,
					VersionID:    s.generateVersionID(),
					LastModified: s.now,
					DeleteMarker: true,
				}
				versions[deleteMarker.VersionID] = deleteMarker

				if !deleteReq.Quiet {
					result.Deleted = append(result.Deleted, deletedObject{
						Key:       key,
						VersionID: deleteMarker.VersionID,
					})
				}
			}

			if len(s.objects[key]) == 0 {
				delete(s.objects, key)
			}
		}

	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	if err := encoder.Encode(result); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding XML: %v", err), http.StatusInternalServerError)
		return
	}
}
