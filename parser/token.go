package parser

type TokenKind int

const (
	TokenKindKeyword TokenKind = iota
	TokenKindNumber
	TokenKindString // TODO
	TokenKindWildcard

	TokenKindExpressionSeparator

	TokenKindOpeningParenthesis
	TokenKindClosingParenthesis
	TokenKindOpeningBraces
	TokenKindClosingBraces

	TokenKindOperator
)

type Token struct {
	kind          TokenKind
	lexeme        string
	startPosition int // TODO remove if not needed
}
