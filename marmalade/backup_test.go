package marmalade

import (
	"os"
	"testing"
	"time"

	fakes3 "github.com/bradenrayhorn/marmalade/internal/fake_s3"
	"github.com/bradenrayhorn/marmalade/internal/testutils/assert"
	"github.com/bradenrayhorn/marmalade/s3"
)

func setupTest(t *testing.T) (*s3.Client, *fakes3.FakeS3, string) {
	sv := fakes3.NewFakeS3("my-bucket")

	sv.StartServer()
	t.Cleanup(func() { sv.StopServer() })
	url := sv.GetEndpoint()

	client := s3.NewClient(s3.Config{
		URL:       url,
		Region:    "my-region",
		KeyID:     "keyid",
		KeySecret: "shh",
		Bucket:    "my-bucket",
		Insecure:  true,
	})

	file, err := os.CreateTemp("", "*.txt")
	assert.NoErr(t, err)
	t.Cleanup(func() { _ = os.Remove(file.Name()) })

	return client, sv, file.Name()
}

var schedule = RetentionSchedule{
	daily:     3,
	dailyLock: lockSchedule{lockType: lockTypeSimple, lockHours: 2},
}

func TestCanBackup(t *testing.T) {
	client, fs3, file := setupTest(t)
	now := time.Date(2025, time.March, 5, 3, 0, 0, 0, time.UTC)

	err := Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), now.Add(time.Hour*2))

	// try again, expect no changes - should never upload duplicate files
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), now.Add(time.Hour*2))
}

func TestSkipsUploadIfNotRetaining(t *testing.T) {
	schedule := RetentionSchedule{daily: 0}

	client, fs3, file := setupTest(t)
	now := time.Date(2025, time.March, 5, 3, 0, 0, 0, time.UTC)

	err := Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.Equal(t, 0, len(fs3.GetVersions("2025-03-05.txt")))
	assert.Equal(t, 0, len(fs3.GetVersions("2025-03-05.txt.sha256")))
}

func TestCanBackupWithNoLock(t *testing.T) {
	schedule := RetentionSchedule{daily: 1}
	client, fs3, file := setupTest(t)
	now := time.Date(2025, time.March, 5, 3, 0, 0, 0, time.UTC)

	err := Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), time.Time{})
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), time.Time{})
}

func TestDeletesUnknownFiles(t *testing.T) {
	client, fs3, file := setupTest(t)
	now := time.Date(2025, time.March, 5, 3, 0, 0, 0, time.UTC)

	err := client.PutObject("randomfile.txt", []byte("abc"), nil)
	assert.NoErr(t, err)

	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.Equal(t, 0, len(fs3.GetVersions("randomfile.txt")))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), now.Add(time.Hour*2))
}

func TestPutsWithLockTime(t *testing.T) {
	client, fs3, file := setupTest(t)
	now := time.Date(2025, time.March, 5, 3, 0, 0, 0, time.UTC)

	// DAILY
	fs3.Reset()
	schedule := RetentionSchedule{daily: 1, dailyLock: lockSchedule{lockType: lockTypeRolling, lockHours: 2}}
	err := Backup(client, schedule, now, file)
	assert.NoErr(t, err)
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), now.Add(time.Hour*2))

	// MONTHLY
	fs3.Reset()
	schedule = RetentionSchedule{monthly: 1, monthlyLock: lockSchedule{lockType: lockTypeRolling, lockHours: 3}}
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), now.Add(time.Hour*3))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), now.Add(time.Hour*3))

	// YEARLY
	fs3.Reset()
	schedule = RetentionSchedule{yearly: 1, yearlyLock: lockSchedule{lockType: lockTypeRolling, lockHours: 4}}
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), now.Add(time.Hour*4))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), now.Add(time.Hour*4))
}

func TestUpdatesRollingRetention(t *testing.T) {
	schedule := RetentionSchedule{
		daily: 2, dailyLock: lockSchedule{lockType: lockTypeRolling, lockHours: 2},
		monthly: 2, monthlyLock: lockSchedule{lockType: lockTypeRolling, lockHours: 3},
		yearly: 2, yearlyLock: lockSchedule{lockType: lockTypeRolling, lockHours: 4},
	}
	client, fs3, file := setupTest(t)

	// backup March 5 2025, April 5 2026, May 2 2026
	now := time.Date(2025, time.March, 5, 3, 0, 0, 0, time.UTC)
	fs3.SetNow(now)
	err := Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	now = time.Date(2026, time.April, 5, 3, 0, 0, 0, time.UTC)
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	now = time.Date(2026, time.May, 2, 3, 0, 0, 0, time.UTC)
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	// do one more backup on May 3
	now = time.Date(2026, time.May, 3, 3, 0, 0, 0, time.UTC)
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	// check retentions have been extended
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), now.Add(time.Hour*4))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), now.Add(time.Hour*4))

	assert.HasOneVersion(t, fs3.GetVersions("2026-04-05.txt"), now.Add(time.Hour*3))
	assert.HasOneVersion(t, fs3.GetVersions("2026-04-05.txt.sha256"), now.Add(time.Hour*3))

	assert.HasOneVersion(t, fs3.GetVersions("2026-05-02.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-05-02.txt.sha256"), now.Add(time.Hour*2))

	assert.HasOneVersion(t, fs3.GetVersions("2026-05-03.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-05-03.txt.sha256"), now.Add(time.Hour*2))
}

func TestUpdatesSimpleRetentionAndDeletes(t *testing.T) {
	schedule := RetentionSchedule{
		daily: 2, dailyLock: lockSchedule{lockType: lockTypeSimple, lockHours: 2},
		monthly: 3, monthlyLock: lockSchedule{lockType: lockTypeSimple, lockHours: 3},
		yearly: 3, yearlyLock: lockSchedule{lockType: lockTypeSimple, lockHours: 4},
	}
	client, fs3, file := setupTest(t)

	// backup March 5 2025
	now := time.Date(2025, time.March, 5, 3, 0, 0, 0, time.UTC)
	mar5 := now
	fs3.SetNow(now)
	err := Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), now.Add(time.Hour*2))

	// backup March 6 2025
	now = time.Date(2025, time.March, 6, 3, 0, 0, 0, time.UTC)
	mar6 := now
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt"), mar5.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-05.txt.sha256"), mar5.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-06.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-06.txt.sha256"), now.Add(time.Hour*2))

	// backup April 1 2025
	now = time.Date(2025, time.April, 1, 3, 0, 0, 0, time.UTC)
	apr1 := now
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.Equal(t, 0, len(fs3.GetVersions("2025-03-05.txt")))
	assert.Equal(t, 0, len(fs3.GetVersions("2025-03-05.txt.sha256")))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-06.txt"), mar6.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-06.txt.sha256"), mar6.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-04-01.txt"), now.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-04-01.txt.sha256"), now.Add(time.Hour*2))

	// backup May 2 2025
	now = time.Date(2025, time.May, 2, 3, 0, 0, 0, time.UTC)
	may2 := now
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.HasOneVersion(t, fs3.GetVersions("2025-03-06.txt"), may2.Add(time.Hour*3)) // was upgraded to monthly
	assert.HasOneVersion(t, fs3.GetVersions("2025-03-06.txt.sha256"), may2.Add(time.Hour*3))
	assert.HasOneVersion(t, fs3.GetVersions("2025-04-01.txt"), apr1.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-04-01.txt.sha256"), apr1.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-05-02.txt"), may2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-05-02.txt.sha256"), may2.Add(time.Hour*2))

	// backup October 2 2026
	now = time.Date(2026, time.October, 2, 3, 0, 0, 0, time.UTC)
	oct2 := now
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.Equal(t, 0, len(fs3.GetVersions("2025-03-06.txt")))
	assert.Equal(t, 0, len(fs3.GetVersions("2025-03-06.txt.sha256")))
	assert.HasOneVersion(t, fs3.GetVersions("2025-04-01.txt"), oct2.Add(time.Hour*3)) // was upgrade to monthly
	assert.HasOneVersion(t, fs3.GetVersions("2025-04-01.txt.sha256"), oct2.Add(time.Hour*3))
	assert.HasOneVersion(t, fs3.GetVersions("2025-05-02.txt"), may2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2025-05-02.txt.sha256"), may2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-10-02.txt"), oct2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-10-02.txt.sha256"), oct2.Add(time.Hour*2))

	// backup November 2 2026
	now = time.Date(2026, time.November, 2, 3, 0, 0, 0, time.UTC)
	nov2 := now
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.Equal(t, 0, len(fs3.GetVersions("2025-04-01.txt")))
	assert.Equal(t, 0, len(fs3.GetVersions("2025-04-01.txt.sha256")))
	assert.HasOneVersion(t, fs3.GetVersions("2025-05-02.txt"), nov2.Add(time.Hour*3)) // was upgrade to monthly
	assert.HasOneVersion(t, fs3.GetVersions("2025-05-02.txt.sha256"), nov2.Add(time.Hour*3))
	assert.HasOneVersion(t, fs3.GetVersions("2026-10-02.txt"), oct2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-10-02.txt.sha256"), oct2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-11-02.txt"), nov2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-11-02.txt.sha256"), nov2.Add(time.Hour*2))

	// backup December 2 2026
	now = time.Date(2026, time.December, 2, 3, 0, 0, 0, time.UTC)
	dec2 := now
	fs3.SetNow(now)
	err = Backup(client, schedule, now, file)
	assert.NoErr(t, err)

	assert.HasOneVersion(t, fs3.GetVersions("2025-05-02.txt"), dec2.Add(time.Hour*4)) // was upgrade to yearly
	assert.HasOneVersion(t, fs3.GetVersions("2025-05-02.txt.sha256"), dec2.Add(time.Hour*4))
	assert.HasOneVersion(t, fs3.GetVersions("2026-10-02.txt"), dec2.Add(time.Hour*3)) // was upgrade to monthly
	assert.HasOneVersion(t, fs3.GetVersions("2026-10-02.txt.sha256"), dec2.Add(time.Hour*3))
	assert.HasOneVersion(t, fs3.GetVersions("2026-11-02.txt"), nov2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-11-02.txt.sha256"), nov2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-12-02.txt"), dec2.Add(time.Hour*2))
	assert.HasOneVersion(t, fs3.GetVersions("2026-12-02.txt.sha256"), dec2.Add(time.Hour*2))
}
