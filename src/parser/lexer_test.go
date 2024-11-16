package parser

import (
	"github.com/hauke96/sigolo/v2"
	"soq/common"
	"testing"
)

func TestLexer_currentAndNextChar(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("012345"),
		index: 0,
	}

	// Act & Assert
	common.AssertEqual(t, '0', l.char())
	common.AssertEqual(t, '1', l.nextChar())

	l.index = 3
	common.AssertEqual(t, '3', l.char())
	common.AssertEqual(t, '4', l.nextChar())

	l.index = 5
	common.AssertEqual(t, '5', l.char())
	common.AssertEqual(t, rune(-1), l.nextChar())

	l.index = 6
	common.AssertEqual(t, rune(-1), l.char())
	common.AssertEqual(t, rune(-1), l.nextChar())
}

func TestLexer_skipComment(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("// ignore this\nbbox(1,2,3,4)"),
		index: 0,
	}

	// Act
	err := l.skipComment()

	// Assert
	common.AssertNil(t, err)
	common.AssertEqual(t, 14, l.index)
}

func TestLexer_skipComment_noCommentLineReturnsError(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("bbox(1,2,3,4)"),
		index: 0,
	}

	// Act
	err := l.skipComment()

	// Assert
	common.AssertNotNil(t, err)
	common.AssertEqual(t, 0, l.index)
}

func TestLexer_currentKeyword(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("bbox(1,2,3,4)"),
		index: 0,
	}

	// Act
	token := l.currentKeyword()

	// Assert
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindKeyword, token.kind)
	common.AssertEqual(t, "bbox", token.lexeme)
	common.AssertEqual(t, 0, token.startPosition)
	common.AssertEqual(t, 4, l.index)
}

func TestLexer_currentNumber(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("123 abc"),
		index: 0,
	}

	// Act
	token := l.currentNumber()

	// Assert
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindNumber, token.kind)
	common.AssertEqual(t, "123", token.lexeme)
	common.AssertEqual(t, 0, token.startPosition)
	common.AssertEqual(t, 3, l.index)
}

func TestLexer_nextToken(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("// skip this\nbbox(1,2,3,4)"),
		index: 0,
	}

	// Act & Assert
	token, err := l.nextToken()
	common.AssertNil(t, err)
	common.AssertNil(t, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindKeyword, token.kind)
	common.AssertEqual(t, "bbox", token.lexeme)
	common.AssertEqual(t, 13, token.startPosition)
	common.AssertEqual(t, 17, l.index)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindOpeningParenthesis, token.kind)
	common.AssertEqual(t, "(", token.lexeme)
	common.AssertEqual(t, 17, token.startPosition)
	common.AssertEqual(t, 18, l.index)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindNumber, token.kind)
	common.AssertEqual(t, "1", token.lexeme)
	common.AssertEqual(t, 18, token.startPosition)
	common.AssertEqual(t, 19, l.index)
}

func TestLexer_nextToken_singleCharTokens(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("(){}*."),
		index: 0,
	}

	// Act & Assert
	token, err := l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindOpeningParenthesis, token.kind)
	common.AssertEqual(t, "(", token.lexeme)
	common.AssertEqual(t, 0, token.startPosition)
	common.AssertEqual(t, 1, l.index)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindClosingParenthesis, token.kind)
	common.AssertEqual(t, ")", token.lexeme)
	common.AssertEqual(t, 1, token.startPosition)
	common.AssertEqual(t, 2, l.index)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindOpeningBraces, token.kind)
	common.AssertEqual(t, "{", token.lexeme)
	common.AssertEqual(t, 2, token.startPosition)
	common.AssertEqual(t, 3, l.index)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindClosingBraces, token.kind)
	common.AssertEqual(t, "}", token.lexeme)
	common.AssertEqual(t, 3, token.startPosition)
	common.AssertEqual(t, 4, l.index)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindWildcard, token.kind)
	common.AssertEqual(t, "*", token.lexeme)
	common.AssertEqual(t, 4, token.startPosition)
	common.AssertEqual(t, 5, l.index)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertNotNil(t, token)
	common.AssertEqual(t, TokenKindExpressionSeparator, token.kind)
	common.AssertEqual(t, ".", token.lexeme)
	common.AssertEqual(t, 5, token.startPosition)
	common.AssertEqual(t, 6, l.index)
}

func TestLexer_nextToken_unexpectedCharacter(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("#%&"),
		index: 0,
	}

	// Act & Assert
	token, err := l.nextToken()
	common.AssertNotNil(t, err)
	common.AssertNil(t, token)

	// Act & Assert
	token, err = l.nextToken()
	common.AssertNotNil(t, err)
	common.AssertNil(t, token)

	// Act & Assert
	token, err = l.nextToken()
	common.AssertNotNil(t, err)
	common.AssertNil(t, token)
}

func TestLexer_nextToken_operators(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("=!=>>=<<=!<"), //  =  !=  >  >=  <  <=  !  <
		index: 0,
	}

	// Act & Assert
	token, err := l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: "=", startPosition: 0}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: "!=", startPosition: 1}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: ">", startPosition: 3}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: ">=", startPosition: 4}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: "<", startPosition: 6}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: "<=", startPosition: 7}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: "!", startPosition: 9}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: "<", startPosition: 10}, token)
}

func TestLexer_nextToken_wildcardOperator(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("a=*"),
		index: 0,
	}

	// Act & Assert
	token, err := l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindKeyword, lexeme: "a", startPosition: 0}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindOperator, lexeme: "=", startPosition: 1}, token)

	token, err = l.nextToken()
	common.AssertNil(t, err)
	common.AssertEqual(t, &Token{kind: TokenKindWildcard, lexeme: "*", startPosition: 2}, token)
}

func TestLexer_read_simple(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("//skip this\nbbox(1,2,3,4.56)\n//also skip this"),
		index: 0,
	}

	// Act
	tokens, err := l.read()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, tokens)
	common.AssertEqual(t, 7, len(tokens))

	common.AssertEqual(t, &Token{kind: TokenKindKeyword, lexeme: "bbox", startPosition: 12}, tokens[0])
	common.AssertEqual(t, &Token{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 16}, tokens[1])
	common.AssertEqual(t, &Token{kind: TokenKindNumber, lexeme: "1", startPosition: 17}, tokens[2])
	common.AssertEqual(t, &Token{kind: TokenKindNumber, lexeme: "2", startPosition: 19}, tokens[3])
	common.AssertEqual(t, &Token{kind: TokenKindNumber, lexeme: "3", startPosition: 21}, tokens[4])
	common.AssertEqual(t, &Token{kind: TokenKindNumber, lexeme: "4.56", startPosition: 23}, tokens[5])
	common.AssertEqual(t, &Token{kind: TokenKindClosingParenthesis, lexeme: ")", startPosition: 27}, tokens[6])
}

func TestLexer_read_commentAfterToken(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("123 // skip this\n234"),
		index: 0,
	}

	// Act
	tokens, err := l.read()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, tokens)
	common.AssertEqual(t, 2, len(tokens))

	common.AssertEqual(t, &Token{kind: TokenKindNumber, lexeme: "123", startPosition: 0}, tokens[0])
	common.AssertEqual(t, &Token{kind: TokenKindNumber, lexeme: "234", startPosition: 17}, tokens[1])
}

func TestLexer_read_commentAfterClosingBlock(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)
	l := &Lexer{
		input: []rune("{ 123 }\n// skip this"),
		index: 0,
	}

	// Act
	tokens, err := l.read()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, tokens)
	common.AssertEqual(t, 3, len(tokens))

	common.AssertEqual(t, &Token{kind: TokenKindOpeningBraces, lexeme: "{", startPosition: 0}, tokens[0])
	common.AssertEqual(t, &Token{kind: TokenKindNumber, lexeme: "123", startPosition: 2}, tokens[1])
	common.AssertEqual(t, &Token{kind: TokenKindClosingBraces, lexeme: "}", startPosition: 6}, tokens[2])
}
