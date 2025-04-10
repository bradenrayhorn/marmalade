package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"filippo.io/age"
	"github.com/bradenrayhorn/marmalade/marmalade"
	"github.com/bradenrayhorn/marmalade/s3"
)

func encryptAndBackup(s3config s3.Config, schedule marmalade.RetentionSchedule, path, ageIdentity string) error {
	client := s3.NewClient(s3config)

	workingDir, err := os.MkdirTemp("", "marmalade-*")
	if err != nil {
		return fmt.Errorf("make working: %w", err)
	}
	defer func() { _ = os.RemoveAll(workingDir) }()

	encryptedArchive, err := encrypt(ageIdentity, path, workingDir)
	if err != nil {
		return err
	}

	err = marmalade.Backup(client, schedule, time.Now().UTC(), encryptedArchive)
	if err != nil {
		return fmt.Errorf("backup: %w", err)
	}

	return nil
}

func encrypt(ageIdentity, filePath, workingDir string) (string, error) {
	identity, err := age.ParseX25519Identity(ageIdentity)
	if err != nil {
		return "", fmt.Errorf("age identity: %w", err)
	}

	src, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", filePath, err)
	}

	archivePath := filepath.Join(workingDir, filepath.Base(filePath)+".age")
	archive, err := os.Create(archivePath)
	if err != nil {
		return "", fmt.Errorf("create %s: %w", archivePath, err)
	}
	defer func() { _ = archive.Close() }()

	w, err := age.Encrypt(archive, identity.Recipient())
	if err != nil {
		return "", fmt.Errorf("age encrypt: %w", err)
	}

	_, err = io.Copy(w, src)
	if err != nil {
		return "", fmt.Errorf("copy to age: %w", err)
	}

	if err := w.Close(); err != nil {
		return "", fmt.Errorf("close encrypted file: %w", err)
	}

	return archivePath, nil
}
