package query

type TokenKind int

const (
	TokenKindKeyword TokenKind = iota
	TokenKindNumber
	TokenKindString // TODO

	TokenKindExpressionSeparator
	//ParameterSeparator

	TokenKindOpeningParenthesis
	TokenKindClosingParenthesis
	TokenKindOpeningBraces
	TokenKindClosingBraces

	TokenKindOperator // TODO Use this instead of the fine-grained operators. The parser will figure out what concrete operator this is. No need to do this twice
)

type Token struct {
	kind          TokenKind
	lexeme        string
	startPosition int // TODO remove if not needed
}
