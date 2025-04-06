package marmalade

import (
	"reflect"
	"testing"
)

func TestRetention(t *testing.T) {

	testCases := []struct {
		schedule RetentionSchedule
		input    []string
		expected retainedFiles
	}{
		{
			RetentionSchedule{
				daily:   3,
				monthly: 3,
				yearly:  4,
			},
			[]string{
				"2025-03-22",
				"2025-03-21",
				"2025-03-20",
				"2025-03-19",
				"2025-03-18",
				"2025-03-17",
				"2025-03-16",
				"2025-03-15",
				"2025-02-17",
				"2025-02-16",
				"2025-01-17",
				"2025-01-16",
				"2024-12-30",
				"2024-12-01",
				"2024-01-01",
				"2023-11-01",
				"2023-01-01",
				"2022-12-01",
				"2022-01-01",
				"2021-12-01",
				"2021-01-01",
			},
			retainedFiles{
				daily: []string{
					"2025-03-22",
					"2025-03-21",
					"2025-03-20",
				},
				monthly: []string{
					"2025-02-17",
					"2025-01-17",
				},
				yearly: []string{
					"2024-12-30",
					"2023-11-01",
					"2022-12-01",
				},
			},
		},

		{
			RetentionSchedule{
				daily:    0,
				monthly:  3,
				yearly:   0,
				inverted: true,
			},
			[]string{
				"2025-03-24",
				"2025-03-01",
				"2025-02-01",
				"2025-01-31",
				"2025-01-01",
				"2024-12-21",
			},
			retainedFiles{
				monthly: []string{
					"2025-03-01",
					"2025-02-01",
					"2025-01-01",
				},
			},
		},

		{
			RetentionSchedule{
				daily:   2,
				monthly: 3,
				yearly:  3,
			},
			[]string{
				"2026-11-02",
				"2026-10-02",
				"2025-05-02",
				"2025-04-01",
			},
			retainedFiles{
				daily: []string{
					"2026-11-02",
					"2026-10-02",
				},
				monthly: []string{
					"2025-05-02",
				},
			},
		},
	}

	for i, tc := range testCases {
		actual := calculateRetention(tc.input, tc.schedule)
		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf(
				"%d: expected=[\n%+v\n], actual=[\n%+v\n]",
				i,
				tc.expected,
				actual,
			)
		}
	}
}
