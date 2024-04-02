package query

import (
	"github.com/hauke96/sigolo/v2"
	"github.com/pkg/errors"
	"soq/index"
	"soq/util"
	"strconv"
	"strings"
)

var (
	bboxLocationExpression = "bbox"
	locationExpressions    = []string{bboxLocationExpression}

	objectTypeNodeExpression = "nodes"
)

type Parser struct {
	token    []*Token
	index    int
	tagIndex *index.TagIndex
}

func ParseQueryString(queryString string, tagIndex *index.TagIndex) (*Query, error) {
	runes := []rune(strings.Trim(queryString, "\n\r\t "))
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
		token:    token,
		index:    0,
		tagIndex: tagIndex,
	}
	return parser.parse()
}

func (p *Parser) moveToNextToken() *Token {
	p.index++
	sigolo.Debugb(1, "Moved to next token: %+v", p.currentToken())
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

	for p.peekNextToken() != nil {
		statement, err := p.parseStatement()
		if err != nil {
			return nil, err
		}

		statement.Print(0)

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
	previousToken := p.currentToken()
	token := p.moveToNextToken()
	if token == nil {
		return nil, errors.Errorf("Expected '.' after token '%s' (at position %d) but token stream ended", previousToken.lexeme, previousToken.startPosition)
	}
	if token.kind != ExpressionSeparator {
		return nil, errors.Errorf("Expected '.' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	// Then object type
	objectType, err := p.parseObjectType()
	if err != nil {
		return nil, err
	}

	// Expect "{"
	previousToken = p.currentToken()
	token = p.moveToNextToken()
	if token == nil {
		return nil, errors.Errorf("Expected '{' after token '%s' (at position %d) but token stream ended", previousToken.lexeme, previousToken.startPosition)
	}
	if token.kind != OpeningBraces {
		return nil, errors.Errorf("Expected '{' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	// Then a filter expression
	filterExpression, err := p.parseFilterExpressions()
	if err != nil {
		return nil, err
	}

	// Expect "}"
	previousToken = p.currentToken()
	token = p.moveToNextToken()
	if token == nil {
		return nil, errors.Errorf("Expected '}' after token '%s' (at position %d) but token stream ended", previousToken.lexeme, previousToken.startPosition)
	}
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
	if token == nil {
		return nil, errors.Errorf("Expected keyword to parse location expression but token stream ended")
	}
	if token.kind != Keyword || !util.Contains(locationExpressions, token.lexeme) {
		return nil, errors.Errorf("Expected location expression at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	// Then a "(" is expected
	expressionStartPosition := token.startPosition
	parenthesisToken := p.moveToNextToken()
	if parenthesisToken == nil {
		return nil, errors.Errorf("Expected '(' at index %d but token stream ended", expressionStartPosition)
	}
	if parenthesisToken.kind != OpeningParenthesis {
		return nil, errors.Errorf("Expected '(' at index %d but found kind=%d with lexeme=%s", parenthesisToken.startPosition, parenthesisToken.kind, parenthesisToken.lexeme)
	}

	var locationExpression LocationExpression
	var err error

	switch token.lexeme {
	case bboxLocationExpression:
		locationExpression, err = p.parseBboxLocationExpression()
	default:
		err = errors.Errorf("Expected location expression at position %d (one of: %s) but found kind=%d with lexeme=%s", token.startPosition, strings.Join(locationExpressions, ", "), token.kind, token.lexeme)
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
			return nil, errors.Errorf("Expected number as argument %d but found kind=%d with lexeme=%s", i+1, token.kind, token.lexeme)
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

func (p *Parser) parseFilterExpressions() (FilterExpression, error) {
	expression, err := p.parseNextExpression()
	if err != nil {
		return nil, err
	}

	for {
		//var nextExpression FilterExpression

		lastToken := p.currentToken()
		token := p.peekNextToken()
		if token == nil {
			return nil, errors.Errorf("Token stream ended unexpectedly after token '%s' at position %d while parsing filter expression. Missing '}'?", lastToken.lexeme, lastToken.startPosition)
		}

		// Closing parentheses and braces are handles by calling functions
		if token.kind == ClosingBraces {
			//expression = nextExpression
			//p.moveToNextToken()
			break
		} else if token.kind == ClosingParenthesis {
			//p.moveToNextToken()
			break
		}

		// nil check happened already above.
		token = p.moveToNextToken()

		// Expect AND, OR or '}' after expression
		if token.kind == Keyword {
			// Handle conjunction/disjunction
			switch token.lexeme {
			case "AND":
				// Exit recursion to create correct hierarchy of AND/OR operators
				var secondExpression FilterExpression
				secondExpression, err = p.parseNextExpression()
				if err != nil {
					return nil, err
				}

				expression = &LogicalFilterExpression{
					statementA: expression,
					statementB: secondExpression,
					operator:   And,
				}
			case "OR":
				// Enter recursion to create correct hierarchy of AND/OR operators
				var secondExpression FilterExpression
				secondExpression, err = p.parseFilterExpressions()
				if err != nil {
					return nil, err
				}

				expression = &LogicalFilterExpression{
					statementA: expression,
					statementB: secondExpression,
					operator:   Or,
				}
			default:
				return nil, errors.Errorf("Unexpected keyword '%s' at position %d, expected 'AND' or 'OR'", token.lexeme, token.startPosition)
			}
		} else {
			return nil, errors.Errorf("Unexpected keyword '%s' at position %d, expected '}', ')', 'AND' or 'OR'", token.lexeme, token.startPosition)
		}
	}

	return expression, nil
}

func (p *Parser) parseNextExpression() (FilterExpression, error) {
	var expression FilterExpression
	var err error
	token := p.moveToNextToken()
	switch token.kind {
	case OpeningParenthesis:
		expression, err = p.parseFilterExpressions()
		if err != nil {
			return nil, err
		}

		// Then a ")" is expected
		token = p.moveToNextToken()
		if token.kind != ClosingParenthesis {
			return nil, errors.Errorf("Expected ')' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
		}
	case OperatorNot:
		negationPosition := token.startPosition

		token = p.moveToNextToken()
		if token == nil {
			return nil, errors.Errorf("Expected start of new expression after '!' (at position %d) but token stream ended", negationPosition)
		}

		nextToken := p.peekNextToken()
		if nextToken == nil {
			return nil, errors.Errorf("Unexpected end of token stream after '%s' (at position %d), expected '(' or call of filter function", token.lexeme, token.startPosition)
		}
		if nextToken.kind != OpeningParenthesis {
			// TODO Add "this" keyword here, which is another possible token after "!"
			return nil, errors.Errorf("Expected '(' after '!' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
		}

		expression, err = p.parseNextExpression()
		if err != nil {
			return nil, err
		}

		expression = &NegatedStatement{
			baseExpression: expression,
			operator:       Not,
		}
	case Keyword:
		if token.lexeme == "this" {
			// Some function call like "this.foo()" -> new statement starts

			thisPosition := token.startPosition
			token = p.moveToNextToken()
			if token == nil {
				return nil, errors.Errorf("Expected '.' after 'this' (at position %d) but token stream ended", thisPosition)
			}
			if token.kind == ExpressionSeparator {
				return nil, errors.Errorf("Expected '.' after 'this' (at position %d) but found kind=%d with lexeme=%s", thisPosition, token.kind, token.lexeme)
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
			token = p.currentToken()
			binaryOperatorLexeme := token.lexeme

			// Parse value (e.g. "primary" in "highway=primary")
			token = p.moveToNextToken()
			if token == nil {
				return nil, errors.Errorf("Expected value after key '%s' but token stream ended", key+binaryOperatorLexeme)
			}
			if token.kind != Keyword && token.kind != Number && token.kind != String {
				return nil, errors.Errorf("Expected value after key '%s' but found kind=%d with lexeme=%s", key+binaryOperatorLexeme, token.kind, token.lexeme)
			}
			value := token.lexeme

			expression = &TagFilterExpression{
				key:      p.tagIndex.GetKeyIndexFromKeyString(key),
				value:    p.tagIndex.GetValueIndexFromKeyValueStrings(key, value),
				operator: binaryOperator,
			}
		}
	}

	return expression, nil
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
		return -1, errors.Errorf("Expected binary operator (e.g. '>=') after '%s' (position %d) but found kind=%d with lexeme=%s", previousLexeme, previousLexemePos, token.kind, token.lexeme)
	}
}
