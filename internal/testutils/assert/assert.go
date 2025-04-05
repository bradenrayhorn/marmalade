package assert

import (
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	fakes3 "github.com/bradenrayhorn/marmalade/internal/fake_s3"
)

func fatal(t testing.TB, msg string, args ...any) {
	pc, _, _, ok := runtime.Caller(2)
	f := runtime.FuncForPC(pc)
	if !ok {
		t.Fatal("assert: could not get caller")
	}

	file, line := f.FileLine(pc)

	finalArgs := []any{file, line}
	finalArgs = append(finalArgs, args...)

	t.Fatalf("\n%s:%d: "+msg, finalArgs...)
}

func True(t testing.TB, actual bool) {
	if !actual {
		fatal(t, "expected true, got false")
	}
}

func Equal[T comparable](t testing.TB, expected T, actual T) {
	if reflect.DeepEqual(expected, actual) {
		return
	}
	fatal(t, "expected %v, actual: %v", expected, actual)
}

func NotEqual[T comparable](t testing.TB, a T, b T) {
	if a == b {
		fatal(t, "expected %v to not equal %v", a, b)
	}
}

func NotZero[T comparable](t testing.TB, actual T) {
	if actual == *new(T) {
		fatal(t, "expected %v to not be empty", actual)
	}
}

func NoErr(t testing.TB, err error) {
	if err != nil {
		fatal(t, "expected no error, actual: %v", err)
	}
}

func ErrContains(t testing.TB, err error, msg string) {
	if err == nil {
		fatal(t, "got nil, expected err")
	}
	if !strings.Contains(err.Error(), msg) {
		fatal(t, "expected error '%v' to contain '%s'", err, msg)
	}
}

func ErrIs(t testing.TB, err error, target error) {
	if err == nil {
		fatal(t, "got nil, expected err")
	}
	if !errors.Is(err, target) {
		fatal(t, "expected error '%v' to be '%v'", err, target)
	}
}

func HasOneVersion(t *testing.T, versions []*fakes3.ObjectVersion, lockedUntil time.Time) {
	if len(versions) != 1 {
		fatal(t, "expected 1 version, found %d", len(versions))
	}

	version := versions[0]
	if lockedUntil.IsZero() {
		if version.Retention != nil {
			fatal(t, "expected no retention, found %v", version.Retention)
		}
	} else {
		if !lockedUntil.Equal(version.Retention.Until) {
			fatal(t, "expected retention to be %v, got %v", lockedUntil, version.Retention.Until)
		}
	}
}
