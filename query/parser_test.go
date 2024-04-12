package query

import (
	"github.com/paulmach/orb"
	"soq/index"
	"soq/util"
	"testing"
)

func TestParser_currentAndNextToken(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "bbox", startPosition: 0},
			{kind: TokenKindNumber, lexeme: "123", startPosition: 4},
		},
		index: 0,
	}

	// Act & Assert
	token := parser.currentToken()
	util.AssertEqual(t, parser.token[0], token)

	token = parser.peekNextToken()
	util.AssertEqual(t, parser.token[1], token)
	token = parser.moveToNextToken()
	util.AssertEqual(t, parser.token[1], token)

	token = parser.currentToken()
	util.AssertEqual(t, parser.token[1], token)
	token = parser.peekNextToken()
	util.AssertNil(t, token)
	token = parser.moveToNextToken()
	util.AssertNil(t, token)
}

func TestParser_parseBboxLocationExpression(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "bbox", startPosition: 0},
			{kind: TokenKindNumber, lexeme: "1.1", startPosition: 4},
			{kind: TokenKindNumber, lexeme: "2.2", startPosition: 8},
			{kind: TokenKindNumber, lexeme: "3", startPosition: 12},
			{kind: TokenKindNumber, lexeme: "4.567", startPosition: 14},
			{kind: TokenKindKeyword, lexeme: "foobar", startPosition: 20},
		},
		index: 0,
	}

	// Act
	expression, err := parser.parseBboxLocationExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)
	expectedBbox := &orb.Bound{
		Min: orb.Point{1.1, 2.2},
		Max: orb.Point{3, 4.567},
	}
	util.AssertEqual(t, expectedBbox, expression.bbox)
	util.AssertEqual(t, 4, parser.index)
}

func TestParser_parseBboxLocationExpression_invalidNumberTokens(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "bbox", startPosition: 0},
			{kind: TokenKindNumber, lexeme: "1.1", startPosition: 4},
			{kind: TokenKindNumber, lexeme: "2.2", startPosition: 8},
			{kind: TokenKindNumber, lexeme: "3", startPosition: 12},
			{kind: TokenKindKeyword, lexeme: "foobar", startPosition: 14},
		},
		index: 0,
	}

	// Act
	expression, err := parser.parseBboxLocationExpression()

	// Assert
	util.AssertNotNil(t, err)
	util.AssertNil(t, expression)
	util.AssertEqual(t, 4, parser.index)
}

func TestParser_parseBboxLocationExpression_notStartingAtBboxToken(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "bbox", startPosition: 0},
			{kind: TokenKindNumber, lexeme: "1.1", startPosition: 4},
			{kind: TokenKindNumber, lexeme: "2.2", startPosition: 8},
			{kind: TokenKindNumber, lexeme: "3", startPosition: 12},
			{kind: TokenKindNumber, lexeme: "4.567", startPosition: 14},
			{kind: TokenKindKeyword, lexeme: "foobar", startPosition: 20},
		},
		index: 0,
	}
	parser.index = 1 // Skip "bbox" token

	// Act
	expression, err := parser.parseBboxLocationExpression()

	// Assert
	util.AssertNotNil(t, err)
	util.AssertNil(t, expression)
	util.AssertEqual(t, 1, parser.index)
}

func TestParser_parseOsmObjectType(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "nodes", startPosition: 0},
			{kind: TokenKindKeyword, lexeme: "foo", startPosition: 0},
		},
		index: 0,
	}

	// Act
	objectType, err := parser.parseOsmObjectType()

	// Assert
	util.AssertNil(t, err)
	util.AssertEqual(t, OsmObjNode, objectType)
	util.AssertEqual(t, 0, parser.index)
}

func TestParser_parseBinaryOperator(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: OperatorEqual, lexeme: "=", startPosition: 0},
			{kind: OperatorNotEqual, lexeme: "!=", startPosition: 1},
			{kind: OperatorGreater, lexeme: ">", startPosition: 3},
			{kind: OperatorGreaterEqual, lexeme: ">=", startPosition: 4},
			{kind: OperatorLower, lexeme: "<", startPosition: 6},
			{kind: OperatorLowerEqual, lexeme: "<=", startPosition: 7},
		},
		index: 0,
	}

	// Act & Assert
	operator, err := parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, BinOpEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, BinOpNotEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, BinOpGreater, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, BinOpGreaterEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, BinOpLower, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, BinOpLowerEqual, operator)
}

func TestParser_parseNextExpression_simpleTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 0},
			{kind: OperatorEqual, lexeme: "=", startPosition: 1},
			{kind: TokenKindKeyword, lexeme: "b", startPosition: 2},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"foo", "a"},
			[][]string{{"bar"}, {"blubb", "b"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 1, tagFilterExpression.key)
	util.AssertEqual(t, 1, tagFilterExpression.value)
	util.AssertEqual(t, BinOpEqual, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_invalidTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 0},
			{kind: OperatorEqual, lexeme: "=", startPosition: 1},
			{kind: OperatorLower, lexeme: "<", startPosition: 2},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"foo", "a"},
			[][]string{{"bar"}, {"blubb", "b"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNotNil(t, err)
	util.AssertNil(t, expression)
}

func TestParser_parseNextExpression_negatedTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: OperatorNot, lexeme: "!", startPosition: 0},
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 1},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 2},
			{kind: OperatorEqual, lexeme: "=", startPosition: 3},
			{kind: TokenKindKeyword, lexeme: "b", startPosition: 4},
			{kind: TokenKindClosingParenthesis, lexeme: ")", startPosition: 5},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"foo", "a"},
			[][]string{{"bar"}, {"blubb", "b"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)

	negatedTagFilterExpression, isNegatedTagFilterExpression := expression.(*NegatedFilterExpression)
	util.AssertTrue(t, isNegatedTagFilterExpression)
	util.AssertNotNil(t, negatedTagFilterExpression.baseExpression)

	tagFilterExpression, isTagFilterExpression := negatedTagFilterExpression.baseExpression.(*TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 1, tagFilterExpression.key)
	util.AssertEqual(t, 1, tagFilterExpression.value)
	util.AssertEqual(t, BinOpEqual, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_invalidNegatedTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: OperatorNot, lexeme: "!", startPosition: 0},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 1},
			{kind: OperatorEqual, lexeme: "=", startPosition: 2},
			{kind: TokenKindKeyword, lexeme: "b", startPosition: 3},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"foo", "a"},
			[][]string{{"bar"}, {"blubb", "b"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNotNil(t, err)
	util.AssertNil(t, expression)
}

func TestParser_parseNextExpression_invalidExpressionInsideNegatedTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: OperatorNot, lexeme: "!", startPosition: 0},
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 1},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 2},
			{kind: TokenKindKeyword, lexeme: "b", startPosition: 3},
			{kind: TokenKindClosingParenthesis, lexeme: ")", startPosition: 4},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"foo", "a"},
			[][]string{{"bar"}, {"blubb", "b"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNotNil(t, err)
	util.AssertNil(t, expression)
}

func TestParser_parseNextExpression_expressionInsideParentheses(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 0},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 1},
			{kind: OperatorEqual, lexeme: "=", startPosition: 2},
			{kind: TokenKindKeyword, lexeme: "b", startPosition: 3},
			{kind: TokenKindClosingParenthesis, lexeme: ")", startPosition: 4},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"foo", "a"},
			[][]string{{"bar"}, {"blubb", "b"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 1, tagFilterExpression.key)
	util.AssertEqual(t, 1, tagFilterExpression.value)
	util.AssertEqual(t, BinOpEqual, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_expressionInsideParenthesesMissinClose(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 0},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 1},
			{kind: OperatorEqual, lexeme: "=", startPosition: 2},
			{kind: TokenKindKeyword, lexeme: "b", startPosition: 3},
			{kind: TokenKindKeyword, lexeme: "foo", startPosition: 4},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"foo", "a"},
			[][]string{{"bar"}, {"blubb", "b"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNotNil(t, err)
	util.AssertNil(t, expression)
}

func TestParser_parseBinaryOperator_invalidAndNotExistingToken(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: OperatorNot, lexeme: "!", startPosition: 0},
		},
		index: 0,
	}

	// Act & Assert
	operator, err := parser.parseBinaryOperator("previous", -123)
	util.AssertNotNil(t, err)
	util.AssertEqual(t, BinOpInvalid, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNotNil(t, err)
	util.AssertEqual(t, BinOpInvalid, operator)
}
