package parser

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
	"soq/common"
	"soq/index"
	"soq/osm"
	"soq/query"
	"strconv"
	"strings"
)

var (
	bboxLocationExpression         = "bbox"
	contextAwareLocationExpression = "this"
	locationExpressions            = []string{bboxLocationExpression}

	objectTypeNodeExpression           = "nodes"
	objectTypeWaysExpression           = "ways"
	objectTypeRelationsExpression      = "relations"
	objectTypeChildRelationsExpression = "child_relations"
)

type Parser struct {
	token         []*Token
	index         int
	tagIndex      *index.TagIndex
	geometryIndex index.GeometryIndex
}

func ParseQueryString(queryString string, tagIndex *index.TagIndex, geometryIndex index.GeometryIndex) (*query.Query, error) {
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

func (p *Parser) hasNextToken() bool {
	return p.peekNextToken() != nil
}

func (p *Parser) getNextTokenStartPosition() int {
	if p.hasNextToken() {
		return p.peekNextToken().startPosition
	} else if p.currentToken() != nil {
		// No next token, so the start position of this hypothetical next token is right behind the current one.
		return p.currentToken().startPosition + len(p.currentToken().lexeme)
	}
	return -1
}

func (p *Parser) currentToken() *Token {
	if p.index >= len(p.token) {
		return nil
	}
	return p.token[p.index]
}

func (p *Parser) parse() (*query.Query, error) {
	var topLevelStatements []query.Statement

	for p.peekNextToken() != nil {
		statement, err := p.parseStatement()
		if err != nil {
			return nil, err
		}

		topLevelStatements = append(topLevelStatements, *statement)
	}

	return query.NewQuery(topLevelStatements), nil
}

func (p *Parser) parseStatement() (*query.Statement, error) {
	var err error

	// Parse location expression, such as "bbox(...)" but also context aware expressions like "this.ways"
	var locationExpression query.LocationExpression
	if !p.hasNextToken() {
		return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected location expression")
	}
	token := p.currentToken()
	if token.kind != TokenKindKeyword {
		return nil, ParsingErrorExpectedButFound("location expression keyword", token.startPosition, token.lexeme, token.kind)
	}

	// We start with a fresh baseExpression, so the first thing we expect is a location expression (e.g. "bbox")
	locationExpression, err = p.parseLocationExpression()
	if err != nil {
		return nil, err
	}
	isContextAwareStatement := token.lexeme == contextAwareLocationExpression

	// Then a '.'
	if !p.hasNextToken() {
		return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected '.'")
	}
	token = p.moveToNextToken()
	if token.kind != TokenKindExpressionSeparator {
		return nil, ParsingErrorExpectedTokenKind(token.startPosition, token.lexeme, token.kind, TokenKindExpressionSeparator)
	}

	// Then object type (e.g. "nodes")
	p.moveToNextToken()
	queryType, err := p.parseOsmQueryType(isContextAwareStatement)
	if err != nil {
		return nil, err
	}

	// Then "{"
	if !p.hasNextToken() {
		return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected '{'")
	}
	token = p.moveToNextToken()
	if token.kind != TokenKindOpeningBraces {
		return nil, ParsingErrorExpectedTokenKind(token.startPosition, token.lexeme, token.kind, TokenKindOpeningBraces)
	}

	// Then a filter expression
	filterExpression, err := p.parseNextFilterExpressions()
	if err != nil {
		return nil, err
	}

	// Then finally "}"
	if !p.hasNextToken() {
		return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected '}'")
	}
	token = p.moveToNextToken()
	if token.kind != TokenKindClosingBraces {
		return nil, ParsingErrorExpectedTokenKind(token.startPosition, token.lexeme, token.kind, TokenKindClosingBraces)
	}

	return query.NewStatement(locationExpression, queryType, filterExpression), nil
}

func (p *Parser) parseLocationExpression() (query.LocationExpression, error) {
	if !p.hasNextToken() {
		return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected keyword for location expression")
	}
	token := p.currentToken()
	if token.kind != TokenKindKeyword || !common.Contains(locationExpressions, token.lexeme) && token.lexeme != contextAwareLocationExpression {
		return nil, ParsingErrorExpectedButFound("location expression", token.startPosition, token.lexeme, token.kind)
	}

	var locationExpression query.LocationExpression
	var err error

	switch token.lexeme {
	case bboxLocationExpression:
		locationExpression, err = p.parseBboxLocationExpression()
	case contextAwareLocationExpression:
		locationExpression, err = query.NewContextAwareLocationExpression(), nil
	default:
		return nil, ParsingErrorExpectedButFound(fmt.Sprintf("location expression (one of: %s)", strings.Join(locationExpressions, ", ")), token.startPosition, token.lexeme, token.kind)
	}

	if err != nil {
		return nil, err
	}

	return locationExpression, nil
}

func (p *Parser) parseBboxLocationExpression() (*query.BboxLocationExpression, error) {
	token := p.currentToken()
	if token.kind != TokenKindKeyword && token.lexeme != "bbox" {
		return nil, ParsingErrorExpectedButFound("start of BBOX-Expression with 'bbox' keyword", token.startPosition, token.lexeme, token.kind)
	}

	// Then a "(" is expected
	if !p.hasNextToken() {
		return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected '('")
	}
	parenthesisToken := p.moveToNextToken()
	if parenthesisToken.kind != TokenKindOpeningParenthesis {
		return nil, ParsingErrorExpectedTokenKind(token.startPosition, token.lexeme, token.kind, TokenKindOpeningParenthesis)
	}

	// Expect four numbers for the BBOX
	var coordinates = [4]float64{}
	for i := 0; i < 4; i++ {
		token = p.moveToNextToken()
		value, err := strconv.ParseFloat(token.lexeme, 64)
		if token.kind != TokenKindNumber || err != nil {
			return nil, ParsingErrorExpectedButFound("number as argument in BBOX-expression", token.startPosition, token.lexeme, token.kind)
		}
		coordinates[i] = value
	}

	// Then a "(" is expected
	token = p.moveToNextToken()
	if token.kind != TokenKindClosingParenthesis {
		return nil, ParsingErrorExpectedTokenKind(token.startPosition, token.lexeme, token.kind, TokenKindClosingParenthesis)
	}

	return query.NewBboxLocationExpression(&orb.Bound{
		Min: orb.Point{coordinates[0], coordinates[1]},
		Max: orb.Point{coordinates[2], coordinates[3]},
	}), nil
}

func (p *Parser) parseOsmQueryType(isContextAwareStatement bool) (osm.OsmQueryType, error) {
	token := p.currentToken()
	if token.kind != TokenKindKeyword {
		return -1, ParsingErrorExpectedButFound(fmt.Sprintf("OSM object type (%s, %s or %s)", objectTypeNodeExpression, objectTypeWaysExpression, objectTypeRelationsExpression), token.startPosition, token.lexeme, token.kind)
	}

	switch token.lexeme {
	case objectTypeNodeExpression:
		return osm.OsmQueryNode, nil
	case objectTypeWaysExpression:
		return osm.OsmQueryWay, nil
	case objectTypeRelationsExpression:
		return osm.OsmQueryRelation, nil
	case objectTypeChildRelationsExpression:
		if !isContextAwareStatement {
			return -1, ParsingErrorExpectedButFound(fmt.Sprintf("OSM object type (%s, %s or %s)", objectTypeNodeExpression, objectTypeWaysExpression, objectTypeRelationsExpression), token.startPosition, token.lexeme, token.kind)
		}
		return osm.OsmQueryChildRelation, nil
	}

	return -1, ParsingErrorExpectedButFound(fmt.Sprintf("OSM object type (%s, %s or %s)", objectTypeNodeExpression, objectTypeWaysExpression, objectTypeRelationsExpression), token.startPosition, token.lexeme, token.kind)
}

func (p *Parser) parseNextFilterExpressions() (query.FilterExpression, error) {
	expression, err := p.parseNextExpression()
	if err != nil {
		return nil, err
	}

	for {
		if !p.hasNextToken() {
			return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected filter expression or '}'")
		}

		// Closing parentheses and braces are handles by calling functions
		token := p.peekNextToken()
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
				var secondExpression query.FilterExpression
				secondExpression, err = p.parseNextExpression()
				if err != nil {
					return nil, err
				}

				expression = query.NewLogicalFilterExpression(expression, secondExpression, query.LogicOpAnd)
			case "OR":
				// Enter recursion to create correct hierarchy of AND/OR operators
				var secondExpression query.FilterExpression
				secondExpression, err = p.parseNextFilterExpressions()
				if err != nil {
					return nil, err
				}

				expression = query.NewLogicalFilterExpression(expression, secondExpression, query.LogicOpOr)
			default:
				return nil, ParsingErrorExpectedButFound("'AND' or 'OR'", token.startPosition, token.lexeme, token.kind)
			}
		} else {
			return nil, ParsingErrorExpectedButFound("'}', ')', 'AND' or 'OR'", token.startPosition, token.lexeme, token.kind)
		}
	}

	return expression, nil
}

func (p *Parser) parseNextExpression() (query.FilterExpression, error) {
	var expression query.FilterExpression
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
			return nil, ParsingErrorExpectedTokenKind(token.startPosition, token.lexeme, token.kind, TokenKindClosingParenthesis)
		}
	case TokenKindOperator:
		if token.lexeme != "!" {
			return nil, ParsingErrorExpectedButFound("'!' to start a new expression", token.startPosition, token.lexeme, token.kind)
		}

		expression, err = p.parseNegatedExpression(token, expression, err)
		if err != nil {
			return nil, err
		}
	case TokenKindKeyword:
		if token.lexeme == contextAwareLocationExpression {
			// Some function call like "this.foo()" -> new statement starts
			var statement *query.Statement
			statement, err = p.parseStatement()
			if err != nil {
				return nil, err
			}
			return query.NewSubStatementFilterExpression(statement), err
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

func (p *Parser) parseNegatedExpression(token *Token, expression query.FilterExpression, err error) (query.FilterExpression, error) {
	if !p.hasNextToken() {
		return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected start of new expression after '!'")
	}

	token = p.peekNextToken()
	if token.kind != TokenKindOpeningParenthesis && !(token.kind == TokenKindKeyword && token.lexeme == contextAwareLocationExpression) {
		// TODO Add "this" keyword here, which is another possible token after "!"
		return nil, ParsingErrorExpectedButFound("'(' after '!'", token.startPosition, token.lexeme, token.kind)
	}

	expression, err = p.parseNextExpression()
	if err != nil {
		return nil, err
	}

	return query.NewNegatedFilterExpression(expression), nil
}

func (p *Parser) parseNormalExpression(token *Token) (query.FilterExpression, error) {
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
	if !p.hasNextToken() {
		return nil, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected value after key "+key+binaryOperatorToken.lexeme)
	}
	valueToken := p.moveToNextToken()
	if valueToken.kind != TokenKindKeyword && valueToken.kind != TokenKindNumber && valueToken.kind != TokenKindString && valueToken.kind != TokenKindWildcard {
		return nil, ParsingErrorExpectedButFound("value after key "+key+binaryOperatorToken.lexeme, valueToken.startPosition, valueToken.lexeme, valueToken.kind)
	}

	if valueToken.kind == TokenKindWildcard {
		if binaryOperator != query.BinOpEqual && binaryOperator != query.BinOpNotEqual {
			return nil, ParsingErrorExpectedButFound("'=' or '!=' operator when using wildcard", token.startPosition, token.lexeme, token.kind)
		}

		return query.NewKeyFilterExpression(keyIndex, binaryOperator == query.BinOpEqual), nil
	} else {
		_, valueIndex := p.tagIndex.GetIndicesFromKeyValueStrings(key, valueToken.lexeme)

		if valueIndex == index.NotFound && binaryOperator.IsComparisonOperator() {
			// Search for next smaller value and adjust binary operator. It can happen that we search for e.g.
			// "width>=2.5" but the exact value "2.5" doesn't exist. Then we have to adjust the expression to
			// "width>2" in case "2" is the next lower existing value for "2.5".
			valueIndex, _ = p.tagIndex.GetNextLowerValueIndexForKey(keyIndex, valueToken.lexeme)

			if valueIndex == index.NotFound {
				// There is no lower value, the valueToken already contains a value lower than the lowest value
				// in the tag index.
				valueIndex = 0
				if binaryOperator == query.BinOpGreater {
					// Example: "width>-1"  ->  "width>=0"
					binaryOperator = query.BinOpGreaterEqual
				} else if binaryOperator == query.BinOpLowerEqual {
					// Example: "width<=-1"  ->  "width<0"
					binaryOperator = query.BinOpLower
				}
				// All other operators are ok, they do not distort/falsify the result of the expression.
			} else {
				// We found the next lower value for the given valueToken. We now might have to adjust the
				// binary operator so that the meaning of the expression is still correct.
				if binaryOperator == query.BinOpGreaterEqual {
					// Example: "width>=2.5"  ->  "width>2"
					binaryOperator = query.BinOpGreater
				} else if binaryOperator == query.BinOpLower {
					// Example: "width<2.5"  ->  "width<=2"
					binaryOperator = query.BinOpLowerEqual
				}
				// All other operators are ok, they do not distort/falsify the result of the expression.
			}
		}

		return query.NewTagFilterExpression(keyIndex, valueIndex, binaryOperator), nil
	}
}

func (p *Parser) parseBinaryOperator(previousLexeme string, previousLexemePos int) (query.BinaryOperator, error) {
	if !p.hasNextToken() {
		return query.BinOpInvalid, ParsingTokenStreamEndAtPosition(p.getNextTokenStartPosition(), "Expected binary operator")
	}
	token := p.currentToken()
	if token.kind != TokenKindOperator {
		return query.BinOpInvalid, ParsingErrorExpectedButFound("Expected binary operator", token.startPosition, token.lexeme, token.kind)
	}

	switch token.lexeme {
	case "=":
		return query.BinOpEqual, nil
	case "!=":
		return query.BinOpNotEqual, nil
	case ">":
		return query.BinOpGreater, nil
	case ">=":
		return query.BinOpGreaterEqual, nil
	case "<":
		return query.BinOpLower, nil
	case "<=":
		return query.BinOpLowerEqual, nil
	default:
		return query.BinOpInvalid, errors.Errorf("Expected binary operator (e.g. '>=') after '%s' (position %d) but found kind=%d with lexeme=%s", previousLexeme, previousLexemePos, token.kind, token.lexeme)
	}
}
