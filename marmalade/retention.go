package marmalade

import (
	"slices"
	"strings"
	"time"
)

type retainedFiles struct {
	yearly  []string
	monthly []string
	daily   []string
}

func (r retainedFiles) All() []string {
	all := append(append(r.yearly, r.monthly...), r.daily...)
	slices.Sort(all)
	slices.Reverse(all)
	return all
}

func calculateRetention(files []string, schedule RetentionSchedule) retainedFiles {
	sortedFiles := make([]string, len(files))
	copy(sortedFiles, files)

	type fileBuckets struct {
		daily   []string
		monthly []string
		yearly  []string

		files map[string]string
	}

	buckets := fileBuckets{
		files: map[string]string{},
	}

	// Group files into buckets. The first file matching a bucket is put into the bucket. Further
	//  files matching the bucket are skipped for that bucket.
	//
	// Normally, descending order is used, meaning the newest file matching a bucket is used.
	// Inverted order can be applied, meaning the oldest file matching a bucket is used.
	slices.Sort(sortedFiles)
	if !schedule.inverted {
		slices.Reverse(sortedFiles)
	}

	for _, file := range sortedFiles {
		splitFile := strings.Split(file, ".")
		date := splitFile[0]
		time, err := time.Parse("2006-01-02", date)

		if err != nil {
			// Discard any files with unknown date
			continue
		}

		// put file into its buckets
		key := time.Format("2006-01-02")
		if !slices.Contains(buckets.daily, key) {
			buckets.daily = append(buckets.daily, key)
			buckets.files[key] = file
		}

		key = time.Format("2006-01")
		if !slices.Contains(buckets.monthly, key) {
			buckets.monthly = append(buckets.monthly, key)
			buckets.files[key] = file
		}

		key = time.Format("2006")
		if !slices.Contains(buckets.yearly, key) {
			buckets.yearly = append(buckets.yearly, key)
			buckets.files[key] = file
		}
	}

	// Sort every bucket descending so that we can easily retain newest N files.
	slices.Sort(buckets.daily)
	slices.Reverse(buckets.daily)
	slices.Sort(buckets.monthly)
	slices.Reverse(buckets.monthly)
	slices.Sort(buckets.yearly)
	slices.Reverse(buckets.yearly)

	// Retain newest N files, as specified by the schedule.
	toRetain := map[string]struct{}{}
	retained := retainedFiles{}
	for i := 0; i < min(schedule.daily, len(buckets.daily)); i++ {
		file := buckets.files[buckets.daily[i]]
		if _, ok := toRetain[file]; !ok {
			toRetain[file] = struct{}{}
			retained.daily = append(retained.daily, file)
		}
	}
	for i := 0; i < min(schedule.monthly, len(buckets.monthly)); i++ {
		file := buckets.files[buckets.monthly[i]]
		if _, ok := toRetain[file]; !ok {
			toRetain[file] = struct{}{}
			retained.monthly = append(retained.monthly, file)
		}
	}
	for i := 0; i < min(schedule.yearly, len(buckets.yearly)); i++ {
		file := buckets.files[buckets.yearly[i]]
		if _, ok := toRetain[file]; !ok {
			toRetain[file] = struct{}{}
			retained.yearly = append(retained.yearly, file)
		}
	}

	return retained
}
