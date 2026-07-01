package controller

import (
	"testing"
	"time"
)

func TestCronScheduleUsesStandardLibraryParser(t *testing.T) {
	parsed, err := parseCronSchedule("@hourly")
	if err != nil {
		t.Fatalf("parseCronSchedule(@hourly) error = %v", err)
	}

	last := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 20, 12, 30, 0, 0, time.UTC)

	due, next := dueAndNext(parsed, last, now)
	if want := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC); !due.Equal(want) {
		t.Fatalf("due = %v, want %v", due, want)
	}
	if want := time.Date(2026, 6, 20, 13, 0, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("next = %v, want %v", next, want)
	}
}

func TestCronScheduleMalformedTimeZoneReturnsError(t *testing.T) {
	if _, err := parseCronSchedule("TZ=0"); err == nil {
		t.Fatal("parseCronSchedule(TZ=0) error = nil, want error")
	}
}
