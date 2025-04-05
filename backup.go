package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/bradenrayhorn/marmalade/s3"
)

func backup(client *s3.Client, file string, now time.Time, schedule retentionSchedule) error {
	bytes, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	hash := sha256.Sum256(bytes)

	pathParts := strings.Split(path.Base(file), ".")
	backupFileName := fmt.Sprintf("%s.%s", now.Format("2006-01-02"), strings.Join(pathParts[1:], "."))

	// Get all objectVersions out of the bucket and check retention.
	objectVersions, err := client.ListObjectVersions("", "", "", 500)
	if err != nil {
		return fmt.Errorf("list object versions: %w", err)
	}

	backups := map[string]struct{}{}
	for _, object := range objectVersions.Versions {
		if !object.IsLatest { // Only consider latest version of files
			continue
		}

		// Remove sha256 hash files
		if !strings.HasSuffix(object.Key, ".sha256") {
			backups[object.Key] = struct{}{}
		}
	}

	oldRetained := calculateRetention(slices.Collect(maps.Keys(backups)), schedule)
	retained := calculateRetention(append(slices.Collect(maps.Keys(backups)), backupFileName), schedule)

	// Upload file if it will be retained AND it has not been uploaded already.
	if _, ok := backups[backupFileName]; !ok && slices.Contains(retained.All(), backupFileName) {
		slog.Info(fmt.Sprintf("Uploading %s", backupFileName))

		var lockHours int

		if slices.Contains(retained.yearly, backupFileName) {
			lockHours = schedule.yearlyLock.lockHours
		}
		if slices.Contains(retained.monthly, backupFileName) {
			lockHours = schedule.monthlyLock.lockHours
		}
		if slices.Contains(retained.daily, backupFileName) {
			lockHours = schedule.dailyLock.lockHours
		}

		var retention *s3.ObjectLockRetention
		if lockHours > 0 {
			retention = &s3.ObjectLockRetention{
				Mode:  "COMPLIANCE",
				Until: now.Add(time.Hour * time.Duration(lockHours)),
			}
		}

		if err := client.PutObject(backupFileName+".sha256", []byte(hex.EncodeToString(hash[:])), retention); err != nil {
			return fmt.Errorf("put object hash: %w", err)
		}
		if err := client.PutObject(backupFileName, bytes, retention); err != nil {
			return fmt.Errorf("put object: %w", err)
		}
	} else {
		slog.Info(fmt.Sprintf("skipping upload, %s will not be retained", backupFileName))
	}

	// Update object lock retention.
	if schedule.dailyLock.lockHours > 0 {
		until := now.Add(time.Hour * time.Duration(schedule.dailyLock.lockHours))
		retention := &s3.ObjectLockRetention{Mode: "COMPLIANCE", Until: until}

		for _, file := range retained.daily {
			if file == backupFileName {
				continue
			}

			if schedule.dailyLock.lockType == lockTypeRolling || !slices.Contains(oldRetained.daily, file) {
				slog.Info(fmt.Sprintf("extending lock for %s", file), "period", "daily")

				err := client.PutObjectRetention(file, retention)
				if err != nil {
					return fmt.Errorf("set retention %s: %w", file, err)
				}
				err = client.PutObjectRetention(file+".sha256", retention)
				if err != nil {
					return fmt.Errorf("set retention %s.sha256: %w", file, err)
				}
			}
		}
	}

	if schedule.monthlyLock.lockHours > 0 {
		until := now.Add(time.Hour * time.Duration(schedule.monthlyLock.lockHours))
		retention := &s3.ObjectLockRetention{Mode: "COMPLIANCE", Until: until}

		for _, file := range retained.monthly {
			if file == backupFileName {
				continue
			}

			if schedule.monthlyLock.lockType == lockTypeRolling || !slices.Contains(oldRetained.monthly, file) {
				slog.Info(fmt.Sprintf("extending lock for %s", file), "period", "monthly")

				err := client.PutObjectRetention(file, retention)
				if err != nil {
					return fmt.Errorf("set retention %s: %w", file, err)
				}
				err = client.PutObjectRetention(file+".sha256", retention)
				if err != nil {
					return fmt.Errorf("set retention %s.sha256: %w", file, err)
				}
			}
		}
	}

	if schedule.yearlyLock.lockHours > 0 {
		until := now.Add(time.Hour * time.Duration(schedule.yearlyLock.lockHours))
		retention := &s3.ObjectLockRetention{Mode: "COMPLIANCE", Until: until}

		for _, file := range retained.yearly {
			if file == backupFileName {
				continue
			}

			if schedule.yearlyLock.lockType == lockTypeRolling || !slices.Contains(oldRetained.yearly, file) {
				slog.Info(fmt.Sprintf("extending lock for %s", file), "period", "yearly")

				err := client.PutObjectRetention(file, retention)
				if err != nil {
					return fmt.Errorf("set retention %s: %w", file, err)
				}
				err = client.PutObjectRetention(file+".sha256", retention)
				if err != nil {
					return fmt.Errorf("set retention %s.sha256: %w", file, err)
				}
			}
		}
	}

	// Delete non-retained files.
	allRetained := retained.All()

	toDelete := []s3.ObjectIdentifier{}
	for _, object := range objectVersions.Versions {
		key := strings.TrimSuffix(object.Key, ".sha256") // remove hash suffix if it exists

		if !slices.Contains(allRetained, key) {
			toDelete = append(toDelete, s3.ObjectIdentifier{Key: object.Key, VersionID: object.VersionId})
			slog.Info(fmt.Sprintf("%s::%s not retained, deleting", object.Key, object.VersionId))
		}
	}

	for _, object := range objectVersions.DeleteMarkers {
		key := strings.TrimSuffix(object.Key, ".sha256") // remove hash suffix if it exists

		if !slices.Contains(allRetained, key) {
			toDelete = append(toDelete, s3.ObjectIdentifier{Key: object.Key, VersionID: object.VersionId})
			slog.Info(fmt.Sprintf("%s::%s not retained, deleting", object.Key, object.VersionId))
		}
	}

	if len(toDelete) > 0 {
		result, err := client.DeleteObjects(toDelete)
		if err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
		if len(result.Error) > 0 {
			for _, deleteError := range result.Error {
				slog.Warn("could not delete file", "key", deleteError.Key, "version", deleteError.VersionID, "message", deleteError.Message)
			}
		}
	}

	return nil
}
