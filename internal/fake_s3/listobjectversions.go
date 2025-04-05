package fakes3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

type listObjectVersionsResponse struct {
	XMLName             xml.Name        `xml:"ListVersionsResult"`
	Xmlns               string          `xml:"xmlns,attr"`
	Name                string          `xml:"Name"`
	Prefix              string          `xml:"Prefix"`
	KeyMarker           string          `xml:"KeyMarker"`
	VersionIdMarker     string          `xml:"VersionIdMarker"`
	MaxKeys             int             `xml:"MaxKeys"`
	IsTruncated         bool            `xml:"IsTruncated"`
	NextKeyMarker       string          `xml:"NextKeyMarker,omitempty"`
	NextVersionIdMarker string          `xml:"NextVersionIdMarker,omitempty"`
	Versions            []objectVersion `xml:"Version"`
	DeleteMarker        []deleteMarker  `xml:"DeleteMarker"`
}

type objectVersion struct {
	Key          string      `xml:"Key"`
	VersionId    string      `xml:"VersionId"`
	IsLatest     bool        `xml:"IsLatest"`
	LastModified time.Time   `xml:"LastModified"`
	ETag         string      `xml:"ETag"`
	Size         int64       `xml:"Size"`
	StorageClass string      `xml:"StorageClass"`
	Owner        objectOwner `xml:"Owner"`
}

type deleteMarker struct {
	Key          string      `xml:"Key"`
	VersionId    string      `xml:"VersionId"`
	IsLatest     bool        `xml:"IsLatest"`
	LastModified time.Time   `xml:"LastModified"`
	Owner        objectOwner `xml:"Owner"`
}

type objectOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

func (s *FakeS3) handleListObjectVersions(w http.ResponseWriter, r *http.Request, bucket string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Parse query parameters
	keyMarker := r.URL.Query().Get("key-marker")
	versionIdMarker := r.URL.Query().Get("version-id-marker")
	maxKeysStr := r.URL.Query().Get("max-keys")

	maxKeys := 1000 // Default value
	if maxKeysStr != "" {
		fmt.Sscanf(maxKeysStr, "%d", &maxKeys)
	}

	result := listObjectVersionsResponse{
		Xmlns:           "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:            bucket,
		Prefix:          "",
		KeyMarker:       keyMarker,
		VersionIdMarker: versionIdMarker,
		MaxKeys:         maxKeys,
	}

	type versionWithLatest struct {
		version  *ObjectVersion
		isLatest bool
	}

	allVersions := make(map[string]map[string]versionWithLatest)
	latestVersions := make(map[string]*ObjectVersion)

	// First, determine the latest version for each key
	for key, versions := range s.objects {
		var latest *ObjectVersion
		var latestTime time.Time

		for _, v := range versions {
			if latest == nil || (v.LastModified.Equal(latestTime) && v.VersionID > latest.VersionID) || v.LastModified.After(latestTime) {
				latest = v
				latestTime = v.LastModified
			}
		}

		if latest != nil {
			latestVersions[key] = latest
		}

		// Initialize the versions map for this key
		if _, exists := allVersions[key]; !exists {
			allVersions[key] = make(map[string]versionWithLatest)
		}

		// Add all versions with isLatest flag
		for versionID, v := range versions {
			allVersions[key][versionID] = versionWithLatest{
				version:  v,
				isLatest: v == latest,
			}
		}
	}

	// Convert to a flat list for sorting
	var flatVersions []versionWithLatest
	for _, versionMap := range allVersions {
		for _, v := range versionMap {
			flatVersions = append(flatVersions, v)
		}
	}

	// Sort by key then by lastModified (descending)
	sort.Slice(flatVersions, func(i, j int) bool {
		if flatVersions[i].version.Key == flatVersions[j].version.Key {
			a := flatVersions[i].version
			b := flatVersions[j].version

			if a.LastModified.Equal(b.LastModified) {
				return strings.Compare(a.VersionID, b.VersionID) > 0
			}

			return a.LastModified.After(b.LastModified)
		}
		return flatVersions[i].version.Key < flatVersions[j].version.Key
	})

	// Apply markers
	startIdx := 0
	if keyMarker != "" {
		for i, v := range flatVersions {
			if v.version.Key > keyMarker || (v.version.Key == keyMarker && v.version.VersionID > versionIdMarker) {
				startIdx = i
				break
			}
		}
	}

	// Apply max keys
	endIdx := startIdx + maxKeys
	if endIdx > len(flatVersions) {
		endIdx = len(flatVersions)
	}

	// Check if truncated
	isTruncated := endIdx < len(flatVersions)
	nextKeyMarker := ""
	nextVersionIdMarker := ""
	if isTruncated {
		nextKeyMarker = flatVersions[endIdx-1].version.Key
		nextVersionIdMarker = flatVersions[endIdx-1].version.VersionID
	}

	// Add selected versions to response
	for _, v := range flatVersions[startIdx:endIdx] {
		obj := v.version

		if obj.DeleteMarker {
			result.DeleteMarker = append(result.DeleteMarker, deleteMarker{
				Key:          obj.Key,
				VersionId:    obj.VersionID,
				IsLatest:     v.isLatest,
				LastModified: obj.LastModified,
				Owner:        objectOwner{ID: ownerID, DisplayName: ownerName},
			})
		} else {
			result.Versions = append(result.Versions, objectVersion{
				Key:          obj.Key,
				VersionId:    obj.VersionID,
				IsLatest:     v.isLatest,
				LastModified: obj.LastModified,
				ETag:         "",
				Size:         int64(len(obj.Content)),
				StorageClass: obj.StorageClass,
				Owner:        objectOwner{ID: ownerID, DisplayName: ownerName},
			})
		}
	}

	result.IsTruncated = isTruncated
	if isTruncated {
		result.NextKeyMarker = nextKeyMarker
		result.NextVersionIdMarker = nextVersionIdMarker
	}

	// Write XML response
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	if err := encoder.Encode(result); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding XML: %v", err), http.StatusInternalServerError)
		return
	}
}
