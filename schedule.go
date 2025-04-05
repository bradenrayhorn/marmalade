package main

type retentionSchedule struct {
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
	lockTypeRolling          = 1
)

type lockSchedule struct {
	lockType  lockType
	lockHours int
}
