package receiverauthorization

import (
	"fmt"
	"strings"
	"unicode"
)

func tokenizeReceiverCommand(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("command is empty")
	}
	var tokens []string
	var current strings.Builder
	var quote rune
	flush := func() {
		if current.Len() == 0 {
			return
		}
		tokens = append(tokens, current.String())
		current.Reset()
	}
	for _, r := range raw {
		if quote != 0 {
			if r == quote {
				quote = 0
				continue
			}
			if r == '\n' || r == '\r' || r == '`' || r == '$' {
				return nil, fmt.Errorf("unsupported quoted character %q", r)
			}
			current.WriteRune(r)
			continue
		}
		switch {
		case unicode.IsSpace(r):
			flush()
		case r == '\'' || r == '"':
			quote = r
		case r == '|':
			flush()
			tokens = append(tokens, "|")
		case r == '\n' || r == '\r' || r == ';' || r == '`' || r == '$' || r == '(' || r == ')' || r == '<':
			return nil, fmt.Errorf("unsupported shell character %q", r)
		default:
			current.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	flush()
	for _, token := range tokens {
		if token == "||" || token == "&&" {
			return nil, fmt.Errorf("unsupported shell operator %q", token)
		}
		if strings.Contains(token, "&") && token != "2>&1" {
			return nil, fmt.Errorf("unsupported shell operator in %q", token)
		}
		if strings.Contains(token, ">") && token != ">/dev/null" && token != "2>/dev/null" && token != "2>&1" {
			return nil, fmt.Errorf("unsupported redirection %q", token)
		}
	}
	return tokens, nil
}

func parseReceiverCommandSteps(tokens []string) ([]receiverCommandStep, error) {
	var steps []receiverCommandStep
	var part []string
	for _, token := range tokens {
		if token != "|" {
			part = append(part, token)
			continue
		}
		step, err := parseReceiverCommandStep(part)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
		part = nil
	}
	step, err := parseReceiverCommandStep(part)
	if err != nil {
		return nil, err
	}
	steps = append(steps, step)
	return steps, nil
}

func parseReceiverCommandStep(tokens []string) (receiverCommandStep, error) {
	if len(tokens) == 0 {
		return receiverCommandStep{}, fmt.Errorf("empty command in pipeline")
	}
	var args []string
	var step receiverCommandStep
	seenRedirect := false
	for _, token := range tokens {
		switch token {
		case ">/dev/null":
			seenRedirect = true
			step.StdoutNull = true
		case "2>/dev/null":
			seenRedirect = true
			step.StderrNull = true
		case "2>&1":
			seenRedirect = true
			step.StderrToStdout = true
		default:
			if seenRedirect {
				return receiverCommandStep{}, fmt.Errorf("command arguments after redirection are not supported")
			}
			args = append(args, token)
		}
	}
	if len(args) == 0 {
		return receiverCommandStep{}, fmt.Errorf("empty command")
	}
	step.Name = args[0]
	step.Args = args[1:]
	return step, nil
}
