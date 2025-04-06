package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"
	"time"

	"filippo.io/age"
	fakes3 "github.com/bradenrayhorn/marmalade/internal/fake_s3"
	"github.com/bradenrayhorn/marmalade/internal/testutils/assert"
	"github.com/bradenrayhorn/marmalade/marmalade"
	"github.com/bradenrayhorn/marmalade/s3"
)

func TestBackup(t *testing.T) {
	sv := fakes3.NewFakeS3("my-bucket")

	sv.StartServer()
	t.Cleanup(func() { sv.StopServer() })
	url := sv.GetEndpoint()

	s3config := s3.Config{
		URL:       url,
		Region:    "my-region",
		KeyID:     "keyid",
		KeySecret: "shh",
		Bucket:    "my-bucket",
		Insecure:  true,
	}

	schedule, err := marmalade.ParseSchedule("1d")
	assert.NoErr(t, err)

	file, err := os.CreateTemp("", "*.txt")
	assert.NoErr(t, err)
	t.Cleanup(func() { _ = os.Remove(file.Name()) })

	_, err = file.Write([]byte("abc"))
	assert.NoErr(t, err)

	id, err := age.GenerateX25519Identity()
	assert.NoErr(t, err)

	// do backup
	err = encryptAndBackup(s3config, schedule, file.Name(), id.String())
	assert.NoErr(t, err)

	// get stored file
	fileName := time.Now().UTC().Format("2006-01-02") + ".txt.age"
	versions := sv.GetVersions(fileName)
	assert.Equal(t, 1, len(versions))

	storedData := versions[0].Content
	hash := sha256.Sum256(storedData)

	// validate can decrypt
	reader, err := age.Decrypt(bytes.NewReader(storedData), id)
	assert.NoErr(t, err)
	decrypted, err := io.ReadAll(reader)
	assert.NoErr(t, err)

	assert.Equal(t, "abc", string(decrypted))

	// check sha256 file matches data
	fileName = time.Now().UTC().Format("2006-01-02") + ".txt.age.sha256"
	versions = sv.GetVersions(fileName)
	assert.Equal(t, 1, len(versions))
	storedData = versions[0].Content

	assert.Equal(t, string(storedData), hex.EncodeToString(hash[:]))
}
