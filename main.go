package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bradenrayhorn/marmalade/s3"
)

func main() {
	backupCmd := flag.NewFlagSet("backup", flag.ExitOnError)
	backupPath := backupCmd.String("path", "", "Path to the file to back up (required)")

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
		if *backupPath == "" {
			fmt.Println("backup: -path flag is required")
			backupCmd.PrintDefaults()
			os.Exit(1)
		}

		config, err := loadConfig()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		client := s3.NewClient(
			config.S3URL,
			config.S3Region,
			config.S3KeyID,
			config.S3KeySecret,
			config.S3Bucket,
		)

		schedule, err := loadRetentionSchedule()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		err = backup(client, *backupPath, time.Now(), schedule)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

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

type backupConfig struct {
	S3URL       string
	S3Region    string
	S3KeyID     string
	S3KeySecret string
	S3Bucket    string
}

func loadConfig() (backupConfig, error) {
	config := backupConfig{
		S3URL:       os.Getenv("MARMALADE_S3_URL"),
		S3Region:    os.Getenv("MARMALADE_S3_REGION"),
		S3KeyID:     os.Getenv("MARMALADE_S3_KEY_ID"),
		S3KeySecret: os.Getenv("MARMALADE_S3_KEY_SECRET"),
		S3Bucket:    os.Getenv("MARMALADE_S3_BUCKET"),
	}

	return config, nil
}

func loadRetentionSchedule() (retentionSchedule, error) {
	schedule := retentionSchedule{}

	getRetention := func(period string) int {
		if val := os.Getenv("MARMALADE_RETAIN_" + period); val != "" {
			if intVal, err := strconv.Atoi(val); err == nil {
				return intVal
			}
		}
		return 0
	}

	getLockSchedule := func(period string) lockSchedule {
		schedule := lockSchedule{lockType: lockTypeSimple, lockHours: 0}
		if val := os.Getenv("MARMALADE_LOCK_HOURS_" + period); val != "" {
			if intVal, err := strconv.Atoi(val); err == nil {
				schedule.lockHours = intVal
			}
		}

		if lockType := os.Getenv("MARMALADE_LOCK_TYPE_" + period); lockType != "" {
			// Case insensitive comparison
			if strings.EqualFold(lockType, "rolling") {
				schedule.lockType = lockTypeRolling
			} else if strings.EqualFold(lockType, "simple") {
				schedule.lockType = lockTypeSimple
			}
		}

		return schedule
	}

	schedule.yearly = getRetention("YEARLY")
	schedule.monthly = getRetention("MONTHLY")
	schedule.daily = getRetention("DAILY")

	schedule.yearlyLock = getLockSchedule("YEARLY")
	schedule.monthlyLock = getLockSchedule("MONTHLY")
	schedule.dailyLock = getLockSchedule("DAILY")

	return schedule, nil
}
