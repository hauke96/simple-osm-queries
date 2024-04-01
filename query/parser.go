package query

import (
	"github.com/hauke96/sigolo/v2"
	"github.com/pkg/errors"
	"soq/util"
	"strconv"
	"strings"
)

var (
	bboxLocationExpression = "bbox"
	locationExpressions    = []string{bboxLocationExpression}

	objectTypeNodeExpression = "nodes"
	objectTypeExpressions    = []string{objectTypeNodeExpression}
)

type Parser struct {
	token []*Token
	index int
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

	parser := Parser{
		token: token,
		index: 0,
	}
	return parser.parse()
}

func (p *Parser) moveToNextToken() *Token {
	p.index++
	return p.currentToken()
}

func (p *Parser) peekNextToken() *Token {
	if p.index+1 >= len(p.token) {
		return nil
	}
	return p.token[p.index+1]
}

func (p *Parser) currentToken() *Token {
	if p.index >= len(p.token) {
		return nil
	}
	return p.token[p.index]
}

func (p *Parser) parse() (*Query, error) {
	var topLevelStatements []Statement

	for p.index < len(p.token) {
		statement, err := p.parseStatement()
		if err != nil {
			return nil, err
		}

		topLevelStatements = append(topLevelStatements, *statement)
	}

	return &Query{topLevelStatements: topLevelStatements}, nil
}

func (p *Parser) parseStatement() (*Statement, error) {
	// We start with a fresh baseExpression, so the first thing we expect is a location expression
	locationExpression, err := p.parseLocationExpression()
	// TODO make this expression optional. In "this.ways{...}" there's no location. Or rather: The location is "everywhere within the current context".
	if err != nil {
		return nil, err
	}

	// Then a '.'
	token := p.moveToNextToken()
	if token.kind != ExpressionSeparator {
		return nil, errors.Errorf("Expected '(' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	// Then object type
	objectType, err := p.parseObjectType()
	if err != nil {
		return nil, err
	}

	// Then a filter expression
	filterExpression, err := p.parseFilterExpression()
	if err != nil {
		return nil, err
	}

	// Then '}'
	token = p.moveToNextToken()
	if token.kind != ClosingBraces {
		return nil, errors.Errorf("Expected '}' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	return &Statement{
		location:   locationExpression,
		objectType: objectType,
		filter:     filterExpression,
	}, nil
}

func (p *Parser) parseLocationExpression() (LocationExpression, error) {
	token := p.currentToken()

	if token.kind != Keyword || util.Contains(locationExpressions, token.lexeme) {
		return nil, errors.Errorf("Expected location expression at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	// Then a "(" is expected
	token = p.moveToNextToken()
	if token.kind != OpeningParenthesis {
		return nil, errors.Errorf("Expected '(' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	var locationExpression LocationExpression
	var err error

	switch token.lexeme {
	case bboxLocationExpression:
		locationExpression, err = p.parseBboxLocationExpression()
	default:
		err = errors.Errorf("Expected location expression at position %d (one of: %s) but found kind=%d, lexeme=%s", token.startPosition, strings.Join(locationExpressions, ", "), token.kind, token.lexeme)
	}

	if err != nil {
		return nil, err
	}

	// Then a "(" is expected
	token = p.moveToNextToken()
	if token.kind != ClosingParenthesis {
		return nil, errors.Errorf("Expected ')' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	return locationExpression, nil
}

func (p *Parser) parseBboxLocationExpression() (LocationExpression, error) {
	// Expect four numbers for the BBOX
	var coordinates = [4]int{}
	for i := 0; i < 4; i++ {
		token := p.moveToNextToken()
		value, err := strconv.Atoi(token.lexeme)
		if token.kind != Number || err != nil {
			return nil, errors.Errorf("Expected number as argument %d but found kind=%d, lexeme=%s", i+1, token.kind, token.lexeme)
		}
		coordinates[i] = value
	}

	return &BboxLocationExpression{coordinates: coordinates}, nil
}

func (p *Parser) parseObjectType() (ObjectType, error) {
	token := p.moveToNextToken()
	if token.kind != Keyword {
		return -1, errors.Errorf("Expected object type at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	switch token.lexeme {
	case objectTypeNodeExpression:
		return Node, nil
	}

	return -1, errors.Errorf("Expected object type at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
}

func (p *Parser) parseFilterExpression() (FilterExpression, error) {
	for {
		if p.peekNextToken() == nil {
			return nil, errors.Errorf("Token stream ended unexpectedly while parsing filter expression. Missing '}'?")
		}
		if p.peekNextToken().kind == ClosingBraces {
			// No handling of "}" here
			// TODO Is this correct?
			return nil, nil
		}

		token := p.moveToNextToken()
		switch token.kind {
		case OpeningParenthesis:
			// TODO Recursively call this function correctly
		case OperatorNot:
			negationPosition := token.startPosition

			token = p.moveToNextToken()
			if token == nil {
				return nil, errors.Errorf("Expected start of new expression after '!' (at position %d) but token stream ended", negationPosition)
			}

			// TODO opening parenthesis or location filter are allowed to follow a "!"

			// TODO We need general filter expression here (since a normal filter expression as well as a statement might come after "!"). Enable this function to detect and handle statements as well.
			innerStatement, err := p.parseFilterExpression()
			if err != nil {
				return nil, err
			}

			return &NegatedStatement{
				baseExpression: innerStatement,
				operator:       Not,
			}, nil
		case Keyword:
			if token.lexeme == "this" {
				// Some function call like "this.foo()" -> new statement starts

				thisPosition := token.startPosition
				token = p.moveToNextToken()
				if token == nil {
					return nil, errors.Errorf("Expected '.' after 'this' (at position %d) but token stream ended", thisPosition)
				}
				if token.kind == ExpressionSeparator {
					return nil, errors.Errorf("Expected '.' after 'this' (at position %d) but found kind=%d, lexeme=%s", thisPosition, token.kind, token.lexeme)
				}

				return p.parseStatement()
			} else {
				// General keyword, meaning a new expression starts, such as "highway=primary".

				// Parse key (e.g. "highway" in "highway=primary")
				key := token.lexeme
				keyPos := token.startPosition

				// Parse operator (e.g. "=" in "highway=primary")
				binaryOperator, err := p.parseBinaryOperator(token, key, keyPos)
				if err != nil {
					return nil, err
				}
				binaryOperatorLexeme := token.lexeme

				// Parse value (e.g. "primary" in "highway=primary")
				token = p.moveToNextToken()
				if token == nil {
					return nil, errors.Errorf("Expected value after key '%s' but token stream ended", key+binaryOperatorLexeme)
				}
				if token.kind == Keyword {
					return nil, errors.Errorf("Expected value after key '%s' but found kind=%d, lexeme=%s", key+binaryOperatorLexeme, token.kind, token.lexeme)
				}
				value := token.lexeme

				return &TagFilterExpression{
					key:      0, // TODO convert key to int representation
					value:    0, // TODO convert value to int representation
					operator: binaryOperator,
				}, nil
			}
		}
	}

	// TODO
	return nil, nil
}

func (p *Parser) parseBinaryOperator(token *Token, previousLexeme string, previousLexemePos int) (BinaryOperator, error) {
	token = p.moveToNextToken()
	if token == nil {
		return -1, errors.Errorf("Expected binary operator after '%s' (position %d) but token stream ended", previousLexeme, previousLexemePos)
	}

	switch token.kind {
	case OperatorEqual:
		return Equal, nil
	case OperatorNotEqual:
		return NotEqual, nil
	case OperatorGreater:
		return Greater, nil
	case OperatorGreaterEqual:
		return GreaterEqual, nil
	case OperatorLower:
		return Lower, nil
	case OperatorLowerEqual:
		return LowerEqual, nil
	default:
		return -1, errors.Errorf("Expected binary operator (e.g. '>=') after '%s' (position %d) but found kind=%d, lexeme=%s", previousLexeme, previousLexemePos, token.kind, token.lexeme)
	}
}
