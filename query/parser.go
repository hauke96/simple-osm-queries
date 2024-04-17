package query

import (
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
	"soq/feature"
	"soq/index"
	"soq/util"
	"strconv"
	"strings"
)

var (
	bboxLocationExpression = "bbox"
	locationExpressions    = []string{bboxLocationExpression}

	objectTypeNodeExpression      = "nodes"
	objectTypeWaysExpression      = "ways"
	objectTypeRelationsExpression = "relations"
)

type Parser struct {
	token         []*Token
	index         int
	tagIndex      *index.TagIndex
	geometryIndex index.GeometryIndex
}

func ParseQueryString(queryString string, tagIndex *index.TagIndex, geometryIndex index.GeometryIndex) (*Query, error) {
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
		token:         token,
		index:         0,
		tagIndex:      tagIndex,
		geometryIndex: geometryIndex,
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

		topLevelStatements = append(topLevelStatements, *statement)
	}

	return &Query{topLevelStatements: topLevelStatements}, nil
}

func (p *Parser) parseStatement() (*Statement, error) {
	var err error

	// Parse location expression, such as "bbox(...)" but also context aware expressions like "this.ways"
	var locationExpression LocationExpression
	token := p.currentToken()
	if token.kind == TokenKindKeyword && token.lexeme == "this" {
		thisPosition := token.startPosition
		token = p.moveToNextToken()
		if token == nil {
			return nil, errors.Errorf("Expected '.' after 'this' (at position %d) but token stream ended", thisPosition)
		}
		if token.kind != TokenKindExpressionSeparator {
			return nil, errors.Errorf("Expected '.' after 'this' (at position %d) but found kind=%d with lexeme=%s", thisPosition, token.kind, token.lexeme)
		}

		locationExpression = &ContextAwareLocationExpression{}
	} else {
		// We start with a fresh baseExpression, so the first thing we expect is a location expression
		locationExpression, err = p.parseLocationExpression()
		if err != nil {
			return nil, err
		}

		// Then a '.'
		previousToken := token
		token = p.moveToNextToken()
		if token == nil {
			return nil, errors.Errorf("Expected '.' after token '%s' (at position %d) but token stream ended", previousToken.lexeme, previousToken.startPosition)
		}
		if token.kind != TokenKindExpressionSeparator {
			return nil, errors.Errorf("Expected '.' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
		}
	}

	// Then object type
	p.moveToNextToken()
	objectType, err := p.parseOsmObjectType()
	if err != nil {
		return nil, err
	}

	// Expect "{"
	previousToken := p.currentToken()
	token = p.moveToNextToken()
	if token == nil {
		return nil, errors.Errorf("Expected '{' after token '%s' (at position %d) but token stream ended", previousToken.lexeme, previousToken.startPosition)
	}
	if token.kind != TokenKindOpeningBraces {
		return nil, errors.Errorf("Expected '{' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	// Then a filter expression
	filterExpression, err := p.parseNextFilterExpressions()
	if err != nil {
		return nil, err
	}

	// Expect "}"
	previousToken = p.currentToken()
	token = p.moveToNextToken()
	if token == nil {
		return nil, errors.Errorf("Expected '}' after token '%s' (at position %d) but token stream ended", previousToken.lexeme, previousToken.startPosition)
	}
	if token.kind != TokenKindClosingBraces {
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
	if token.kind != TokenKindKeyword || !util.Contains(locationExpressions, token.lexeme) {
		return nil, errors.Errorf("Expected location expression at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
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

	return locationExpression, nil
}

func (p *Parser) parseBboxLocationExpression() (*BboxLocationExpression, error) {
	token := p.currentToken()
	if token.kind != TokenKindKeyword && token.lexeme != "bbox" {
		return nil, errors.Errorf("Error parsing BBOX-Expression: Expected start at bbox-token at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	// Then a "(" is expected
	expressionStartPosition := token.startPosition
	parenthesisToken := p.moveToNextToken()
	if parenthesisToken == nil {
		return nil, errors.Errorf("Expected '(' at index %d but token stream ended", expressionStartPosition)
	}
	if parenthesisToken.kind != TokenKindOpeningParenthesis {
		return nil, errors.Errorf("Expected '(' at index %d but found kind=%d with lexeme=%s", parenthesisToken.startPosition, parenthesisToken.kind, parenthesisToken.lexeme)
	}

	// Expect four numbers for the BBOX
	var coordinates = [4]float64{}
	for i := 0; i < 4; i++ {
		token = p.moveToNextToken()
		value, err := strconv.ParseFloat(token.lexeme, 64)
		if token.kind != TokenKindNumber || err != nil {
			return nil, errors.Errorf("Expected number as argument %d but found kind=%d with lexeme=%s", i+1, token.kind, token.lexeme)
		}
		coordinates[i] = value
	}

	// Then a "(" is expected
	token = p.moveToNextToken()
	if token.kind != TokenKindClosingParenthesis {
		return nil, errors.Errorf("Expected ')' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	return &BboxLocationExpression{bbox: &orb.Bound{
		Min: orb.Point{coordinates[0], coordinates[1]},
		Max: orb.Point{coordinates[2], coordinates[3]},
	}}, nil
}

func (p *Parser) parseContextAwareLocationExpression() (*ContextAwareLocationExpression, error) {
	token := p.currentToken()
	notAContextAwareSpecifier := token.lexeme != objectTypeNodeExpression && token.lexeme != objectTypeWaysExpression && token.lexeme != objectTypeRelationsExpression
	if token.kind != TokenKindKeyword || notAContextAwareSpecifier {
		return nil, errors.Errorf("Error parsing BBOX-Expression: Expected start at bbox-token at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	return &ContextAwareLocationExpression{}, nil
}

func (p *Parser) parseOsmObjectType() (feature.OsmObjectType, error) {
	token := p.currentToken()
	if token.kind != TokenKindKeyword {
		return -1, errors.Errorf("Expected object type at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	switch token.lexeme {
	case objectTypeNodeExpression:
		return feature.OsmObjNode, nil
	case objectTypeWaysExpression:
		return feature.OsmObjWay, nil
	case objectTypeRelationsExpression:
		return feature.OsmObjRelation, nil
	}

	return -1, errors.Errorf("Expected object type at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
}

func (p *Parser) parseNextFilterExpressions() (FilterExpression, error) {
	expression, err := p.parseNextExpression()
	if err != nil {
		return nil, err
	}

	for {
		lastToken := p.currentToken()
		token := p.peekNextToken()
		if token == nil {
			return nil, errors.Errorf("Token stream ended unexpectedly after token '%s' at position %d while parsing filter expression. Missing '}'?", lastToken.lexeme, lastToken.startPosition)
		}

		// Closing parentheses and braces are handles by calling functions
		if token.kind == TokenKindClosingBraces {
			break
		} else if token.kind == TokenKindClosingParenthesis {
			break
		}

		// nil check happened already above.
		token = p.moveToNextToken()

		// Expect AND, OR or '}' after expression
		if token.kind == TokenKindKeyword {
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
					operator:   LogicOpAnd,
				}
			case "OR":
				// Enter recursion to create correct hierarchy of AND/OR operators
				var secondExpression FilterExpression
				secondExpression, err = p.parseNextFilterExpressions()
				if err != nil {
					return nil, err
				}

				expression = &LogicalFilterExpression{
					statementA: expression,
					statementB: secondExpression,
					operator:   LogicOpOr,
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
	case TokenKindOpeningParenthesis:
		expression, err = p.parseNextFilterExpressions()
		if err != nil {
			return nil, err
		}

		// Then a ")" is expected
		token = p.moveToNextToken()
		if token.kind != TokenKindClosingParenthesis {
			return nil, errors.Errorf("Expected ')' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
		}
	case TokenKindOperator:
		if token.lexeme != "!" {
			return nil, errors.Errorf("Expected '!' to start a new expression (at position %d) but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
		}

		expression, err = p.parseNegatedExpression(token, expression, err)
		if err != nil {
			return nil, err
		}
	case TokenKindKeyword:
		if token.lexeme == "this" {
			// Some function call like "this.foo()" -> new statement starts
			var statement *Statement
			statement, err = p.parseStatement()
			if err != nil {
				return nil, err
			}
			return &SubStatementFilterExpression{
				statement:   statement,
				cachedCells: []index.CellIndex{},
				idCache:     make(map[uint64]uint64),
			}, err
		} else {
			// General keyword, meaning a new expression starts, such as "highway=primary".

			expression, err = p.parseNormalExpression(token)
			if err != nil {
				return nil, err
			}
		}
	}

	return expression, nil
}

func (p *Parser) parseNegatedExpression(token *Token, expression FilterExpression, err error) (FilterExpression, error) {
	negationPosition := token.startPosition

	token = p.peekNextToken()
	if token == nil {
		return nil, errors.Errorf("Expected start of new expression after '!' (at position %d) but token stream ended", negationPosition)
	}

	if token.kind != TokenKindOpeningParenthesis && !(token.kind == TokenKindKeyword && token.lexeme == "this") {
		// TODO Add "this" keyword here, which is another possible token after "!"
		return nil, errors.Errorf("Expected '(' after '!' at index %d but found kind=%d with lexeme=%s", token.startPosition, token.kind, token.lexeme)
	}

	expression, err = p.parseNextExpression()
	if err != nil {
		return nil, err
	}

	expression = &NegatedFilterExpression{
		baseExpression: expression,
	}
	return expression, nil
}

func (p *Parser) parseNormalExpression(token *Token) (FilterExpression, error) {
	// We're on the key (e.g. "highway" in "highway=primary")
	key := token.lexeme
	keyPos := token.startPosition
	keyIndex := p.tagIndex.GetKeyIndexFromKeyString(key)

	// Parse operator (e.g. "=" in "highway=primary")
	p.moveToNextToken()
	binaryOperator, err := p.parseBinaryOperator(key, keyPos)
	if err != nil {
		return nil, err
	}
	binaryOperatorToken := p.currentToken()

	// Parse value (e.g. "primary" in "highway=primary")
	valueToken := p.moveToNextToken()
	if valueToken == nil {
		return nil, errors.Errorf("Expected value after key '%s' but token stream ended", key+binaryOperatorToken.lexeme)
	}
	if valueToken.kind != TokenKindKeyword && valueToken.kind != TokenKindNumber && valueToken.kind != TokenKindString && valueToken.kind != TokenKindWildcard {
		return nil, errors.Errorf("Expected value after key '%s' but found kind=%d with lexeme=%s at pos=%d", key+binaryOperatorToken.lexeme, valueToken.kind, valueToken.lexeme, valueToken.startPosition)
	}

	if valueToken.kind == TokenKindWildcard {
		if binaryOperator != BinOpEqual && binaryOperator != BinOpNotEqual {
			return nil, errors.Errorf("Expected '=' or '!=' operator when using wildcard but found kind=%d with lexeme=%s at pos=%d", valueToken.kind, valueToken.lexeme, valueToken.startPosition)
		}

		return &KeyFilterExpression{
			key:         keyIndex,
			shouldBeSet: binaryOperator == BinOpEqual,
		}, nil
	} else {
		_, valueIndex := p.tagIndex.GetIndicesFromKeyValueStrings(key, valueToken.lexeme)

		if valueIndex == index.NotFound && binaryOperator.isComparisonOperator() {
			// Search for next smaller value and adjust binary operator. It can happen that we search for e.g.
			// "width>=2.5" but the exact value "2.5" doesn't exist. Then we have to adjust the expression to
			// "width>2" in case "2" is the next lower existing value for "2.5".
			valueIndex, _ = p.tagIndex.GetNextLowerValueIndexForKey(keyIndex, valueToken.lexeme)

			if valueIndex == index.NotFound {
				// There is no lower value, the valueToken already contains a value lower than the lowest value
				// in the tag index.
				valueIndex = 0
				if binaryOperator == BinOpGreater {
					// Example: "width>-1"  ->  "width>=0"
					binaryOperator = BinOpGreaterEqual
				} else if binaryOperator == BinOpLowerEqual {
					// Example: "width<=-1"  ->  "width<0"
					binaryOperator = BinOpLower
				}
				// All other operators are ok, they do not distort/falsify the result of the expression.
			} else {
				// We found the next lower value for the given valueToken. We now might have to adjust the
				// binary operator so that the meaning of the expression is still correct.
				if binaryOperator == BinOpGreaterEqual {
					// Example: "width>=2.5"  ->  "width>2"
					binaryOperator = BinOpGreater
				} else if binaryOperator == BinOpLower {
					// Example: "width<2.5"  ->  "width<=2"
					binaryOperator = BinOpLowerEqual
				}
				// All other operators are ok, they do not distort/falsify the result of the expression.
			}
		}

		return &TagFilterExpression{
			key:      keyIndex,
			value:    valueIndex,
			operator: binaryOperator,
		}, nil
	}
}

func (p *Parser) parseBinaryOperator(previousLexeme string, previousLexemePos int) (BinaryOperator, error) {
	token := p.currentToken()
	if token == nil || token.kind != TokenKindOperator {
		return BinOpInvalid, errors.Errorf("Expected binary operator after '%s' (position %d) but token stream ended", previousLexeme, previousLexemePos)
	}

	switch token.lexeme {
	case "=":
		return BinOpEqual, nil
	case "!=":
		return BinOpNotEqual, nil
	case ">":
		return BinOpGreater, nil
	case ">=":
		return BinOpGreaterEqual, nil
	case "<":
		return BinOpLower, nil
	case "<=":
		return BinOpLowerEqual, nil
	default:
		return BinOpInvalid, errors.Errorf("Expected binary operator (e.g. '>=') after '%s' (position %d) but found kind=%d with lexeme=%s", previousLexeme, previousLexemePos, token.kind, token.lexeme)
	}
}
