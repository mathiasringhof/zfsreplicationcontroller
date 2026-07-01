package controller

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

func parseCronSchedule(expr string) (schedule cron.Schedule, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			schedule = nil
			err = fmt.Errorf("invalid schedule format: %v", recovered)
		}
	}()
	return cron.ParseStandard(expr)
}

func dueAndNext(s cron.Schedule, last, now time.Time) (time.Time, time.Time) {
	var due time.Time
	next := s.Next(last)
	for !next.IsZero() && !next.After(now) {
		due = next
		next = s.Next(next)
	}
	return due, next
}
