package parser

import (
	"fmt"
	"runtime"
	"strings"
)

type stack *[]uintptr

// getCurrentStack creates a new stack without the last three frames, because they are from the internal calls (e.g. to
// this function) and therefore irrelevant to the function creating the error.
func getCurrentStack() stack {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	var st = pcs[0:n]
	return &st
}

func getPrintableStackTrace(stack stack) string {
	var sb strings.Builder

	for _, pc := range *stack {
		f := runtime.FuncForPC(pc)
		file, line := f.FileLine(pc)
		sb.WriteString(fmt.Sprintf("%s\n\t%s:%d\n", f.Name(), file, line))
	}

	return sb.String()
}

// ParsingExpectedButFoundError models a typical "Expected foo but found bar" kind of error.
type ParsingExpectedButFoundError struct {
	Position        int       `json:"position"`
	CurrentLexeme   string    `json:"current-lexeme"`
	CurrentKind     TokenKind `json:"current-kind"`
	ExpectedMessage string    `json:"expected-message"`
	stack           stack
}

func ParsingErrorExpectedButFound(expectedMessage string, position int, currentLexeme string, currentKind TokenKind) *ParsingExpectedButFoundError {
	return &ParsingExpectedButFoundError{
		Position:        position,
		CurrentLexeme:   currentLexeme,
		CurrentKind:     currentKind,
		ExpectedMessage: expectedMessage,
		stack:           getCurrentStack(),
	}
}

func (e *ParsingExpectedButFoundError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		fmt.Fprintf(s, "%s\n%s", e.Error(), getPrintableStackTrace(e.stack))
	case 's':
		fmt.Fprintf(s, "%s", e.Error())
	}
}

func (e *ParsingExpectedButFoundError) Error() string {
	return fmt.Sprintf("Parsing error: Expected %s at position %d but found '%s' of kind %s.", e.ExpectedMessage, e.Position, e.CurrentLexeme, e.CurrentKind.String())
}

// ParsingExpectedTokenKindError models a typical "Expected '(' but found ..." kind of error for a specific wanted token kind.
type ParsingExpectedTokenKindError struct {
	Position      int       `json:"position"`
	CurrentLexeme string    `json:"current-lexeme"`
	CurrentKind   TokenKind `json:"current-kind"`
	ExpectedKind  TokenKind `json:"expected-kind"`
	stack         stack
}

func ParsingErrorExpectedTokenKind(position int, currentLexeme string, currentKind TokenKind, expectedKind TokenKind) *ParsingExpectedTokenKindError {
	return &ParsingExpectedTokenKindError{
		Position:      position,
		CurrentLexeme: currentLexeme,
		CurrentKind:   currentKind,
		ExpectedKind:  expectedKind,
		stack:         getCurrentStack(),
	}
}

func (e *ParsingExpectedTokenKindError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		fmt.Fprintf(s, "%s\n%s", e.Error(), getPrintableStackTrace(e.stack))
	case 's':
		fmt.Fprintf(s, "%s", e.Error())
	}
}

func (e *ParsingExpectedTokenKindError) Error() string {
	return fmt.Sprintf("Parsing error: Expected '%s' (%s) at position %d but found '%s' of kind %s.", e.ExpectedKind.Lexeme(), e.ExpectedKind.String(), e.Position, e.CurrentLexeme, e.CurrentKind.String())
}

// ParsingTokenStreamEndedError models a typical "Expected foo but found bar" kind of error.
type ParsingTokenStreamEndedError struct {
	Position        int    `json:"position"`
	ExpectedMessage string `json:"expected-message"`
	stack           stack
}

func (e *ParsingTokenStreamEndedError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		fmt.Fprintf(s, "%s\n%s", e.Error(), getPrintableStackTrace(e.stack))
	case 's':
		fmt.Fprintf(s, "%s", e.Error())
	}
}

func ParsingTokenStreamEndAtPosition(currentPosition int, expectedMessage string) *ParsingTokenStreamEndedError {
	return &ParsingTokenStreamEndedError{
		Position:        currentPosition,
		ExpectedMessage: expectedMessage,
		stack:           getCurrentStack(),
	}
}

func (e *ParsingTokenStreamEndedError) Error() string {
	return fmt.Sprintf("Parsing error: Token stream ended at position %d, epxected %s.", e.Position, e.ExpectedMessage)
}