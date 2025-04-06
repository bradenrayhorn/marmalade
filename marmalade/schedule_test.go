package marmalade

import (
	"reflect"
	"testing"

	"github.com/bradenrayhorn/marmalade/internal/testutils/assert"
)

func TestParseSchedule(t *testing.T) {

	testCases := []struct {
		input    string
		expected RetentionSchedule
		error    string
	}{
		{
			input: "7d 12m 8y",
			expected: RetentionSchedule{
				daily:   7,
				monthly: 12,
				yearly:  8,
			},
		},
		{
			input: "12m",
			expected: RetentionSchedule{
				monthly: 12,
			},
		},
		{
			input: "- 12m",
			expected: RetentionSchedule{
				monthly:  12,
				inverted: true,
			},
		},
		{
			input: "12m/216h%",
			expected: RetentionSchedule{
				monthly:     12,
				monthlyLock: lockSchedule{lockTypeRolling, 216},
			},
		},
		{
			input: "7x",
			error: "unrecognized unit: x",
		},
		{
			input: "7xy",
			error: "unrecognized period: 7xy",
		},
		{
			input: "7d 0d",
			error: "period 0d duplicates unit d",
		},
		{
			input: "",
			error: "schedule is empty",
		},
	}

	for i, tc := range testCases {
		actual, err := ParseSchedule(tc.input)
		if err != nil {
			if tc.error != "" {
				assert.ErrContains(t, err, tc.error)
			} else {
				t.Errorf("%d: got error %s", i, err)
			}
		} else {
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
}
