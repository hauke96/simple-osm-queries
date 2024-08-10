package parser

import (
	"fmt"
)

type TokenKind int

const (
	TokenKindUnknown TokenKind = iota

	TokenKindKeyword
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

func (k TokenKind) String() string {
	switch k {
	case TokenKindUnknown:
		return "TokenKindUnknown"
	case TokenKindKeyword:
		return "TokenKindKeyword"
	case TokenKindNumber:
		return "TokenKindNumber"
	case TokenKindString:
		return "TokenKindString"
	case TokenKindWildcard:
		return "TokenKindWildcard"
	case TokenKindExpressionSeparator:
		return "TokenKindExpressionSeparator"
	case TokenKindOpeningParenthesis:
		return "TokenKindOpeningParenthesis"
	case TokenKindClosingParenthesis:
		return "TokenKindClosingParenthesis"
	case TokenKindOpeningBraces:
		return "TokenKindOpeningBraces"
	case TokenKindClosingBraces:
		return "TokenKindClosingBraces"
	case TokenKindOperator:
		return "TokenKindOperator"
	}
	return fmt.Sprintf("!! INVALID TOKEN KIND %d !!", k)
}

func (k TokenKind) Lexeme() string {
	switch k {
	case TokenKindUnknown:
		return "UNKNOWN"
	case TokenKindKeyword:
		return "keyword"
	case TokenKindNumber:
		return "number"
	case TokenKindString:
		return "string"
	case TokenKindWildcard:
		return "*"
	case TokenKindExpressionSeparator:
		return "."
	case TokenKindOpeningParenthesis:
		return "("
	case TokenKindClosingParenthesis:
		return ")"
	case TokenKindOpeningBraces:
		return "{"
	case TokenKindClosingBraces:
		return "}"
	case TokenKindOperator:
		return "binary operator"
	}
	return fmt.Sprintf("!! INVALID TOKEN KIND %d !!", k)
}

type Token struct {
	kind          TokenKind
	lexeme        string
	startPosition int // TODO remove if not needed
}
