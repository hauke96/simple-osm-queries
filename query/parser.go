package query

import "github.com/hauke96/sigolo/v2"

type Parser struct {
}

func ParseQueryString(queryString string) (*Query, error) {
	runes := []rune(queryString)
	lexer := Lexer{
		input: runes,
		index: 0,
	}

	token, err := lexer.read()
	if err != nil {
		return nil, err
	}

	sigolo.Tracef("Found %d token", len(token))
	for _, t := range token {
		sigolo.Tracef("  kind=%d, pos=%d : %s", t.kind, t.startPosition, t.lexeme)
	}

	return nil, nil // TODO
}
