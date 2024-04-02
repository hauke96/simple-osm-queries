package query

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/pkg/errors"
	"soq/util"
	"unicode"
)

type Lexer struct {
	input []rune
	index int // Position in input.
}

var (
	keywordChars = []rune{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
		'_', ':', '@'}
	numberChars = []rune{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.'}
)

// char returns the rune at the current location or the number 0 if there is no next char.
func (l *Lexer) char() rune {
	if l.index >= len(l.input) {
		return 0
	}
	return l.input[l.index]
}

// nextChar returns the next rune, so the one after the rune char() returns, or the number 0 if there is no next char.
func (l *Lexer) nextChar() rune {
	if l.index+1 >= len(l.input) {
		return 0
	}
	return l.input[l.index+1]
}

func (l *Lexer) read() ([]*Token, error) {
	var tokens []*Token
	for l.index < len(l.input) {
		token, err := l.nextToken()
		if err != nil {
			return nil, err
		}
		l.tracef("Found token kind=%d, pos=%d, lexeme=\"%s\"", token.kind, token.startPosition, token.lexeme)
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func (l *Lexer) nextToken() (*Token, error) {
	/*
		Approach:

		Look at the current character l.char() and create token where possible. Everything else is a keyword. Each token
		creation has to take care of the index, so that we don't end up in an endless loop because the index wasn't
		incremented.
	*/

	for ; l.index < len(l.input); l.index++ {
		char := l.char()
		l.tracef("Process next char")

		// Ignore whitespace outside of string literals
		if unicode.IsSpace(char) || char == ',' {
			continue
		}

		// Ignore comments until next linebreak
		if char == '/' {
			err := l.skipComment()
			if err != nil {
				return nil, err
			}
			continue
		}

		// Single-char token
		switch char {
		case '(':
			return l.currentSingleCharToken(OpeningParenthesis), nil
		case ')':
			return l.currentSingleCharToken(ClosingParenthesis), nil
		case '{':
			return l.currentSingleCharToken(OpeningBraces), nil
		case '}':
			return l.currentSingleCharToken(ClosingBraces), nil
		case '.':
			return l.currentSingleCharToken(ExpressionSeparator), nil
			// TODO I think this token kind is not necessary:
			//case ',':
			//	return l.currentSingleCharToken(ParameterSeparator), nil
		}

		// Keywords and identifier (i.e. token consisting of multi-char words)
		if util.Contains(keywordChars, char) {
			return l.currentKeyword(), nil
		}

		// Numbers
		if util.Contains(numberChars, char) {
			return l.currentNumber(), nil
		}

		// Operators
		switch char {
		case '=':
			return l.currentSingleCharToken(OperatorEqual), nil
		case '<':
			if l.nextChar() == '=' {
				l.index++
				return l.currentSingleCharToken(OperatorLowerEqual), nil
			}
			return l.currentSingleCharToken(OperatorLower), nil
		case '>':
			if l.nextChar() == '=' {
				l.index++
				return l.currentSingleCharToken(OperatorGreaterEqual), nil
			}
			return l.currentSingleCharToken(OperatorGreater), nil
		case '!':
			if l.nextChar() == '=' {
				l.index++
				return l.currentSingleCharToken(OperatorNotEqual), nil
			}
			return l.currentSingleCharToken(OperatorNot), nil
		}

		return nil, errors.Errorf("Unexpected character '%c' at index %d", char, l.index)
	}

	return nil, errors.New("No token found")
}

func (l *Lexer) skipComment() error {
	l.tracef("Potential comment start")
	l.index++
	if l.index >= len(l.input) || l.char() != '/' {
		// Text ended or next rune is not '/'
		return errors.Errorf("Unexpected '%c' at index %d", l.char(), l.index-1)
	}
	l.tracef("Found comment start")

	for ; l.index < len(l.input); l.index++ {
		if l.char() == '\n' || l.char() == '\n' {
			return nil
		}
		l.tracef("Skip in comment")
	}

	l.tracef("Done parsing comment")
	return nil
}

func (l *Lexer) currentSingleCharToken(tokenKind TokenKind) *Token {
	token := &Token{
		kind:          tokenKind,
		lexeme:        string(l.char()),
		startPosition: l.index,
	}
	l.index++
	return token
}

func (l *Lexer) currentKeyword() *Token {
	lexeme := ""
	startIndex := l.index

	// Collect lexeme until end of character (e.g. when "{" or a newline comes)
	for ; l.index < len(l.input) && util.Contains(keywordChars, l.char()); l.index++ {
		lexeme += string(l.char())
	}

	return &Token{
		kind:          Keyword,
		lexeme:        lexeme,
		startPosition: startIndex,
	}
}

func (l *Lexer) currentNumber() *Token {
	lexeme := ""
	startIndex := l.index

	// Collect lexeme until end of character (e.g. when ")" or a newline comes)
	for ; l.index < len(l.input) && util.Contains(numberChars, l.char()); l.index++ {
		lexeme += string(l.char())
	}

	return &Token{
		kind:          Number,
		lexeme:        lexeme,
		startPosition: startIndex,
	}
}

func (l *Lexer) tracef(format string, args ...any) {
	formattedMessage := format
	if args != nil && len(args) > 0 {
		formattedMessage = fmt.Sprintf(format, args...)
	}
	sigolo.Traceb(1, "[%d, %q] %s", l.index, l.char(), formattedMessage)
}
