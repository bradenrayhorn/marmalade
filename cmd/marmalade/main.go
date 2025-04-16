package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/bradenrayhorn/marmalade/marmalade"
	"github.com/bradenrayhorn/marmalade/s3"
)

func main() {
	backupCmd := flag.NewFlagSet("backup", flag.ExitOnError)
	backupFile := backupCmd.String("f", "", "Path to back up")

	extendLocksCmd := flag.NewFlagSet("extend-locks", flag.ExitOnError)

	wouldRetainCmd := flag.NewFlagSet("would-retain", flag.ExitOnError)

	// Check if a command was provided
	if len(os.Args) < 2 {
		fmt.Println("Expected 'backup', 'extend-locks', or 'would-retain' command")
		os.Exit(1)
	}

	// Parse the command
	switch os.Args[1] {
	case "backup":
		backupCmd.Parse(os.Args[2:])
		if *backupFile == "" {
			fmt.Println("backup: -f flags are required")
			backupCmd.PrintDefaults()
			os.Exit(1)
		}

		schedule, err := marmalade.ParseSchedule(os.Getenv("MARMALADE_SCHEDULE"))
		if err != nil {
			fmt.Printf("parse schedule: %v\n", err)
			os.Exit(1)
		}

		err = encryptAndBackup(loadConfig(), schedule, *backupFile, os.Getenv("MARMALADE_AGE_PUBLIC_KEY"))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		return
	case "extend-locks":
		extendLocksCmd.Parse(os.Args[2:])
		//extendLocks()
	case "would-retain":
		wouldRetainCmd.Parse(os.Args[2:])
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		fmt.Println("Expected 'backup', 'extend-locks', or 'would-retain' command")
		os.Exit(1)
	}
}

func loadConfig() s3.Config {
	config := s3.Config{
		URL:          os.Getenv("MARMALADE_S3_URL"),
		Region:       os.Getenv("MARMALADE_S3_REGION"),
		KeyID:        os.Getenv("MARMALADE_S3_KEY_ID"),
		KeySecret:    os.Getenv("MARMALADE_S3_KEY_SECRET"),
		Bucket:       os.Getenv("MARMALADE_S3_BUCKET"),
		StorageClass: os.Getenv("MARMALADE_S3_STORAGE_CLASS"),
	}

	return config
}
