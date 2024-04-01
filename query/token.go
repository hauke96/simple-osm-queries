package query

type TokenKind int

const (
	Keyword TokenKind = iota
	Number

	//ParameterSeparator

	OpeningParenthesis
	ClosingParenthesis
	OpeningBraces
	ClosingBraces

	OperatorEqual
	OperatorNotEqual
	OperatorNot
	OperatorGreater
	OperatorGreaterEqual
	OperatorLower
	OperatorLowerEqual
)

type Token struct {
	kind          TokenKind
	lexeme        string
	startPosition int // TODO remove if not needed
}
