package parser

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

// char returns the rune at the current location or the rune '-1' if there is no next char.
func (l *Lexer) char() rune {
	if l.index >= len(l.input) {
		return -1
	}
	return l.input[l.index]
}

// nextChar returns the next rune, so the one after the rune char() returns, or the rune '-1' if there is no next char.
func (l *Lexer) nextChar() rune {
	if l.index+1 >= len(l.input) {
		return -1
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
		if token != nil {
			l.tracef("Found token kind=%d, pos=%d, lexeme=\"%s\"", token.kind, token.startPosition, token.lexeme)
			tokens = append(tokens, token)
		} else {
			l.tracef("Did not found a next token. This happens when a comment is at the end of the text. If this is not the case, than this might indicate a bug.")
		}
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
			return nil, nil
		}

		// Single-char token
		switch char {
		case '(':
			return l.currentSingleCharToken(TokenKindOpeningParenthesis), nil
		case ')':
			return l.currentSingleCharToken(TokenKindClosingParenthesis), nil
		case '{':
			return l.currentSingleCharToken(TokenKindOpeningBraces), nil
		case '}':
			return l.currentSingleCharToken(TokenKindClosingBraces), nil
		case '.':
			return l.currentSingleCharToken(TokenKindExpressionSeparator), nil
		case '*':
			return l.currentSingleCharToken(TokenKindWildcard), nil
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
		case '!':
			if l.nextChar() == '=' {
				token := l.currentMultiCharToken(TokenKindOperator, 2)
				return token, nil
			}
			return l.currentSingleCharToken(TokenKindOperator), nil
		case '<':
			if l.nextChar() == '=' {
				token := l.currentMultiCharToken(TokenKindOperator, 2)
				return token, nil
			}
			return l.currentSingleCharToken(TokenKindOperator), nil
		case '>':
			if l.nextChar() == '=' {
				token := l.currentMultiCharToken(TokenKindOperator, 2)
				return token, nil
			}
			return l.currentSingleCharToken(TokenKindOperator), nil
		case '=':
			return l.currentSingleCharToken(TokenKindOperator), nil
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
		l.index--
		return errors.Errorf("Unexpected '%c' at index %d", l.char(), l.index)
	}
	l.tracef("Found comment start")

	for ; l.index < len(l.input); l.index++ {
		if l.char() == '\n' || l.char() == '\r' {
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

func (l *Lexer) currentMultiCharToken(tokenKind TokenKind, chars int) *Token {
	token := &Token{
		kind:          tokenKind,
		lexeme:        string(l.input[l.index : l.index+chars]),
		startPosition: l.index,
	}
	l.index += chars
	return token
}

// currentKeyword returns the keyword starting at the current index.
func (l *Lexer) currentKeyword() *Token {
	lexeme := ""
	startIndex := l.index

	// Collect lexeme until end of character (e.g. when "{" or a newline comes)
	for ; l.index < len(l.input) && util.Contains(keywordChars, l.char()); l.index++ {
		lexeme += string(l.char())
	}

	return &Token{
		kind:          TokenKindKeyword,
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
		kind:          TokenKindNumber,
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
