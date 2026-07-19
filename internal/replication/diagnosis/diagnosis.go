package diagnosis

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxBytes = 4096

const outputTailBytes = 64 * 1024

// Stream identifies the command-output stream for a sanitized live line.
type Stream string

const (
	// Stdout identifies sanitized standard-output lines.
	Stdout Stream = "stdout"
	// Stderr identifies sanitized standard-error lines.
	Stderr Stream = "stderr"
)

// LineLogger receives sanitized live output one bounded line at a time.
type LineLogger func(Stream, string)

// Capture owns bounded command output until it selects a Failure Diagnosis.
type Capture struct {
	stdout *streamCapture
	stderr *streamCapture
}

// NewCapture creates a bounded capture that emits only sanitized live lines.
func NewCapture(log LineLogger) *Capture {
	return &Capture{
		stdout: &streamCapture{
			stream:  Stdout,
			log:     log,
			tail:    outputTail{limit: outputTailBytes},
			pending: make([]byte, 0, outputTailBytes),
		},
		stderr: &streamCapture{
			stream:  Stderr,
			log:     log,
			tail:    outputTail{limit: outputTailBytes},
			pending: make([]byte, 0, outputTailBytes),
		},
	}
}

// Stdout returns the standard-output capture writer.
func (c *Capture) Stdout() io.Writer { return c.stdout }

// Stderr returns the standard-error capture writer.
func (c *Capture) Stderr() io.Writer { return c.stderr }

type streamCapture struct {
	stream     Stream
	log        LineLogger
	tail       outputTail
	pending    []byte
	discarding bool
}

func (s *streamCapture) Write(p []byte) (int, error) {
	written := len(p)
	for len(p) > 0 {
		if s.discarding {
			i := bytes.IndexAny(p, "\r\n")
			if i < 0 {
				break
			}
			s.discarding = false
			p = p[i+1:]
			continue
		}
		if i := bytes.IndexAny(p, "\r\n"); i >= 0 {
			if s.appendFragment(p[:i]) {
				s.flushLine()
			}
			p = p[i+1:]
			continue
		}
		if !s.appendFragment(p) {
			s.discarding = true
		}
		break
	}
	return written, nil
}

func (s *streamCapture) appendFragment(fragment []byte) bool {
	if len(fragment) > outputTailBytes-len(s.pending) {
		s.omitLine()
		return false
	}
	s.pending = append(s.pending, fragment...)
	return true
}

func (s *streamCapture) flushLine() {
	line := strings.TrimSpace(redactSensitiveText(string(s.pending)))
	s.pending = s.pending[:0]
	if line == "" {
		return
	}
	s.recordLine(line)
}

func (s *streamCapture) omitLine() {
	s.pending = s.pending[:0]
	s.recordLine("<output line omitted: exceeded 65536 bytes>")
}

func (s *streamCapture) recordLine(line string) {
	if len(s.tail.data) > 0 {
		s.tail.append([]byte{'\n'})
	}
	s.tail.append([]byte(line))
	if s.log != nil {
		s.log(s.stream, line)
	}
}

type outputTail struct {
	limit int
	data  []byte
}

func (t *outputTail) append(p []byte) {
	if len(p) >= t.limit {
		t.data = append(t.data[:0], p[len(p)-t.limit:]...)
		return
	}
	overflow := len(t.data) + len(p) - t.limit
	if overflow > 0 {
		copy(t.data, t.data[overflow:])
		t.data = t.data[:len(t.data)-overflow]
	}
	t.data = append(t.data, p...)
}

func (t *outputTail) String() string { return string(t.data) }

// Diagnosis is opaque, sanitized operator evidence rather than a stable category.
type Diagnosis struct {
	text string
}

// String returns the sanitized, single-line diagnosis.
func (d Diagnosis) String() string { return d.text }

// Sanitize constructs a bounded, valid UTF-8 Failure Diagnosis.
func Sanitize(value string) Diagnosis {
	value = strings.ToValidUTF8(value, "�")
	value = redactSensitiveText(value)
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if len(value) > maxBytes {
		value = value[:maxBytes]
		for !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
		value = strings.TrimSpace(value)
	}
	return Diagnosis{text: value}
}

// Failure carries only a Failure Diagnosis and extracted exit metadata.
type Failure struct {
	diagnosis Diagnosis
	exitCode  int
}

// Error returns the safe Failure Diagnosis without exposing captured output.
func (f Failure) Error() string { return f.diagnosis.String() }

// Diagnosis returns the safe Failure Diagnosis.
func (f Failure) Diagnosis() Diagnosis { return f.diagnosis }

// ExitCode returns the command exit code, or -1 when it is unavailable.
func (f Failure) ExitCode() int { return f.exitCode }

// Failure selects the primary cause and discards unsafe command evidence.
func (c *Capture) Failure(err error) Failure {
	c.Flush()
	return Failure{
		diagnosis: Sanitize(primaryCause(c.stdout.tail.String(), c.stderr.tail.String(), redactError(err))),
		exitCode:  commandExitCode(err),
	}
}

// Flush emits any final unterminated sanitized live lines.
func (c *Capture) Flush() {
	c.stdout.flushLine()
	c.stderr.flushLine()
}

func redactError(err error) error {
	if err == nil {
		return nil
	}
	return safeError(redactSensitiveText(err.Error()))
}

type safeError string

func (e safeError) Error() string { return string(e) }

func redactSensitiveText(value string) string {
	value = redactOptionValue(value, "--sshkey=", "--sshkey=<redacted>")
	value = redactSeparatedOptionValue(value, "-i", "-i <redacted>")
	value = redactSeparatedOptionValue(value, "-S", "-S <redacted>")
	return strings.ReplaceAll(value, "/var/run/zfsrep/ssh/id_rsa", "<redacted>")
}

func redactOptionValue(value, option, replacement string) string {
	for searchStart := 0; searchStart < len(value); {
		relative := strings.Index(value[searchStart:], option)
		if relative < 0 {
			return value
		}
		start := searchStart + relative
		end := tokenEnd(value, start+len(option))
		value = value[:start] + replacement + value[end:]
		searchStart = start + len(replacement)
	}
	return value
}

func redactSeparatedOptionValue(value, option, replacement string) string {
	for searchStart := 0; searchStart < len(value); {
		relative := strings.Index(value[searchStart:], option)
		if relative < 0 {
			return value
		}
		start := searchStart + relative
		after := start + len(option)
		if (start == 0 || isSpace(value[start-1])) && after < len(value) && isSpace(value[after]) {
			for after < len(value) && isSpace(value[after]) {
				after++
			}
			if after < len(value) {
				end := tokenEnd(value, after)
				value = value[:start] + replacement + value[end:]
				searchStart = start + len(replacement)
				continue
			}
		}
		searchStart = start + len(option)
	}
	return value
}

func tokenEnd(value string, start int) int {
	if start >= len(value) {
		return start
	}
	if quote, consumed, ok := escapedQuoteAt(value, start); ok {
		return quotedTokenEnd(value, start+consumed, quote, true, start)
	}
	if isQuote(value[start]) {
		return quotedTokenEnd(value, start+1, value[start], false, start)
	}
	return unquotedTokenEnd(value, start)
}

func quotedTokenEnd(value string, start int, quote byte, escaped bool, fallbackStart int) int {
	for i := start; i < len(value); i++ {
		if escaped {
			if foundQuote, consumed, ok := escapedQuoteAt(value, i); ok && foundQuote == quote {
				return i + consumed
			}
		} else if value[i] == '\\' && i+1 < len(value) {
			i++
			continue
		}
		if value[i] == quote {
			return i + 1
		}
	}
	return unquotedTokenEnd(value, fallbackStart)
}

func unquotedTokenEnd(value string, start int) int {
	for i := start; i < len(value); i++ {
		if isSpace(value[i]) {
			return i
		}
	}
	return len(value)
}

func escapedQuoteAt(value string, pos int) (byte, int, bool) {
	backslashes := 0
	for pos+backslashes < len(value) && value[pos+backslashes] == '\\' {
		backslashes++
	}
	if backslashes == 0 || pos+backslashes >= len(value) || !isQuote(value[pos+backslashes]) {
		return 0, 0, false
	}
	return value[pos+backslashes], backslashes + 1, true
}

func isQuote(ch byte) bool {
	return ch == '"' || ch == '\''
}

func isSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func primaryCause(stdout, stderr string, err error) string {
	for _, output := range []string{stderr, stdout} {
		if line := firstLineContaining(output, "CRITICAL ERROR:"); line != "" {
			return conciseCause(line)
		}
	}
	for _, output := range []string{stderr, stdout} {
		for _, line := range strings.Split(output, "\n") {
			line = strings.TrimSpace(line)
			if strings.Contains(line, "cannot receive") || strings.Contains(line, "cannot open") {
				return line
			}
		}
	}
	if err != nil {
		return strings.TrimSpace(err.Error())
	}
	if line := lastLine(stderr); line != "" {
		return line
	}
	if line := lastLine(stdout); line != "" {
		return line
	}
	return "sender failed"
}

func firstLineContaining(value, needle string) string {
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}

func conciseCause(value string) string {
	idx := strings.Index(value, "CRITICAL ERROR:")
	if idx < 0 {
		return value
	}
	critical := strings.TrimSpace(value[idx:])
	if !strings.Contains(critical, "zfs send") && !strings.Contains(critical, "zfs receive") && !strings.Contains(critical, "|") {
		return critical
	}
	if target := criticalPipelineReceiveTarget(critical); target != "" {
		return "CRITICAL ERROR: syncoid command failed target=" + target
	}
	return "CRITICAL ERROR: syncoid command failed"
}

func criticalPipelineReceiveTarget(value string) string {
	idx := strings.LastIndex(value, " zfs receive")
	if idx < 0 {
		return ""
	}
	fields := strings.Fields(value[idx+len(" zfs receive"):])
	for i := 0; i < len(fields); i++ {
		field := strings.Trim(fields[i], `"'.,;()[]{}<>`)
		if field == "" || strings.Contains(field, ">&") || strings.HasPrefix(field, ">") || strings.HasPrefix(field, "2>") {
			continue
		}
		if field == "failed" || field == "failed:" {
			return ""
		}
		if strings.HasPrefix(field, "-") {
			if field == "-o" || field == "-x" {
				i++
			}
			continue
		}
		if field != "sh" {
			return field
		}
	}
	return ""
}

func lastLine(value string) string {
	lines := strings.Split(value, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); line != "" {
			return line
		}
	}
	return ""
}

type exitCoder interface {
	ExitCode() int
}

func commandExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr exitCoder
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}
