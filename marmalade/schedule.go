package marmalade

import (
	"fmt"
	"slices"
	"strings"
)

type RetentionSchedule struct {
	yearly  int
	monthly int
	daily   int

	yearlyLock  lockSchedule
	monthlyLock lockSchedule
	dailyLock   lockSchedule

	inverted bool
}

type lockType int

const (
	lockTypeSimple  lockType = 0
	lockTypeRolling lockType = 1
)

type lockSchedule struct {
	lockType  lockType
	lockHours int
}

func ParseSchedule(scheduleString string) (RetentionSchedule, error) {
	// "- 7d 12m/2160h 7y/2160h%"
	scheduleString = strings.TrimSpace(scheduleString)

	inverted := false
	if strings.HasPrefix(scheduleString, "- ") {
		inverted = true
		scheduleString = strings.TrimPrefix(scheduleString, "- ")
	}

	schedule := RetentionSchedule{inverted: inverted}

	if scheduleString == "" {
		return schedule, fmt.Errorf("schedule is empty")
	}

	periods := strings.Split(scheduleString, " ")

	parsedUnits := []string{}
	for _, period := range periods {
		lockType := lockTypeSimple
		toParse := period
		if strings.HasSuffix(period, "%") {
			toParse = strings.TrimSuffix(period, "%")
			lockType = lockTypeRolling
		}

		var value int
		var unit string
		var hours int
		_, err := fmt.Sscanf(toParse, "%d%1s/%dh", &value, &unit, &hours)
		if err != nil || fmt.Sprintf("%d%s/%dh", value, unit, hours) != toParse {
			_, err = fmt.Sscanf(toParse, "%d%1s", &value, &unit)
			hours = 0
			if err != nil || fmt.Sprintf("%d%s", value, unit) != toParse {
				return schedule, fmt.Errorf("unrecognized period: %s", period)
			}
		}

		if slices.Contains(parsedUnits, unit) {
			return schedule, fmt.Errorf("period %s duplicates unit %s", period, unit)
		}

		parsedUnits = append(parsedUnits, unit)

		if unit == "d" {
			schedule.daily = value
			schedule.dailyLock = lockSchedule{lockType, hours}
		} else if unit == "m" {
			schedule.monthly = value
			schedule.monthlyLock = lockSchedule{lockType, hours}
		} else if unit == "y" {
			schedule.yearly = value
			schedule.yearlyLock = lockSchedule{lockType, hours}
		} else {
			return schedule, fmt.Errorf("unrecognized unit: %s", unit)
		}
	}

	return schedule, nil
}
