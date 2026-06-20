package controller

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type cronSchedule struct {
	minute     fieldMatcher
	hour       fieldMatcher
	dayOfMonth fieldMatcher
	month      fieldMatcher
	dayOfWeek  fieldMatcher
}

type fieldMatcher map[int]bool

func parseCronSchedule(expr string) (cronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return cronSchedule{}, fmt.Errorf("schedule must have five cron fields")
	}
	minute, err := parseCronField(fields[0], 0, 59)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("minute field: %w", err)
	}
	hour, err := parseCronField(fields[1], 0, 23)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("hour field: %w", err)
	}
	dayOfMonth, err := parseCronField(fields[2], 1, 31)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("day-of-month field: %w", err)
	}
	month, err := parseCronField(fields[3], 1, 12)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("month field: %w", err)
	}
	dayOfWeek, err := parseCronField(fields[4], 0, 7)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("day-of-week field: %w", err)
	}
	if dayOfWeek[7] {
		dayOfWeek[0] = true
		delete(dayOfWeek, 7)
	}
	return cronSchedule{
		minute:     minute,
		hour:       hour,
		dayOfMonth: dayOfMonth,
		month:      month,
		dayOfWeek:  dayOfWeek,
	}, nil
}

func parseCronField(field string, min, max int) (fieldMatcher, error) {
	values := fieldMatcher{}
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty field part")
		}
		base := part
		step := 1
		if before, after, ok := strings.Cut(part, "/"); ok {
			base = before
			parsedStep, err := strconv.Atoi(after)
			if err != nil || parsedStep <= 0 {
				return nil, fmt.Errorf("invalid step %q", after)
			}
			step = parsedStep
		}
		start, end, err := cronRange(base, min, max)
		if err != nil {
			return nil, err
		}
		for value := start; value <= end; value += step {
			values[value] = true
		}
	}
	return values, nil
}

func cronRange(base string, min, max int) (int, int, error) {
	if base == "*" {
		return min, max, nil
	}
	if left, right, ok := strings.Cut(base, "-"); ok {
		start, err := parseCronNumber(left, min, max)
		if err != nil {
			return 0, 0, err
		}
		end, err := parseCronNumber(right, min, max)
		if err != nil {
			return 0, 0, err
		}
		if start > end {
			return 0, 0, fmt.Errorf("range start %d is after end %d", start, end)
		}
		return start, end, nil
	}
	value, err := parseCronNumber(base, min, max)
	if err != nil {
		return 0, 0, err
	}
	return value, value, nil
}

func parseCronNumber(value string, min, max int) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", value)
	}
	if parsed < min || parsed > max {
		return 0, fmt.Errorf("value %d outside range %d-%d", parsed, min, max)
	}
	return parsed, nil
}

func (s cronSchedule) Next(after time.Time) time.Time {
	next := after.Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < 366*24*60; i++ {
		if s.matches(next) {
			return next
		}
		next = next.Add(time.Minute)
	}
	return time.Time{}
}

func (s cronSchedule) matches(t time.Time) bool {
	return s.minute[t.Minute()] &&
		s.hour[t.Hour()] &&
		s.dayOfMonth[t.Day()] &&
		s.month[int(t.Month())] &&
		s.dayOfWeek[int(t.Weekday())]
}

func dueAndNext(s cronSchedule, last, now time.Time) (time.Time, time.Time) {
	var due time.Time
	next := s.Next(last)
	for !next.IsZero() && !next.After(now) {
		due = next
		next = s.Next(next)
	}
	return due, next
}
