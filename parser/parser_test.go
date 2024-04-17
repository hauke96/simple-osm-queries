package parser

import (
	"github.com/paulmach/orb"
	"soq/feature"
	"soq/index"
	"soq/query"
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
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 4},
			{kind: TokenKindNumber, lexeme: "1.1", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "2.2", startPosition: 9},
			{kind: TokenKindNumber, lexeme: "3", startPosition: 13},
			{kind: TokenKindNumber, lexeme: "4.567", startPosition: 15},
			{kind: TokenKindClosingParenthesis, lexeme: ")", startPosition: 21},
			{kind: TokenKindKeyword, lexeme: "foobar", startPosition: 22},
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
	util.AssertEqual(t, 6, parser.index)
}

func TestParser_parseLocationExpression(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "bbox", startPosition: 0},
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 4},
			{kind: TokenKindNumber, lexeme: "1.1", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "2.2", startPosition: 9},
			{kind: TokenKindNumber, lexeme: "3", startPosition: 13},
			{kind: TokenKindNumber, lexeme: "4.567", startPosition: 15},
			{kind: TokenKindClosingParenthesis, lexeme: ")", startPosition: 21},
			{kind: TokenKindKeyword, lexeme: "foobar", startPosition: 22},
		},
		index: 0,
	}

	// Act
	expression, err := parser.parseLocationExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)
	bboxExpression, isBboxExpression := expression.(*query.BboxLocationExpression)
	util.AssertTrue(t, isBboxExpression)
	expectedBbox := &orb.Bound{
		Min: orb.Point{1.1, 2.2},
		Max: orb.Point{3, 4.567},
	}
	util.AssertEqual(t, expectedBbox, bboxExpression.bbox)
	util.AssertEqual(t, 6, parser.index)
}

func TestParser_parseBboxLocationExpression_invalidNumberTokens(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "bbox", startPosition: 0},
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 4},
			{kind: TokenKindNumber, lexeme: "1.1", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "2.2", startPosition: 9},
			{kind: TokenKindNumber, lexeme: "3", startPosition: 13},
			{kind: TokenKindClosingParenthesis, lexeme: ")", startPosition: 14},
			{kind: TokenKindKeyword, lexeme: "foobar", startPosition: 15},
		},
		index: 0,
	}

	// Act
	expression, err := parser.parseBboxLocationExpression()

	// Assert
	util.AssertNotNil(t, err)
	util.AssertNil(t, expression)
	util.AssertEqual(t, 5, parser.index)
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
	util.AssertEqual(t, feature.OsmObjNode, objectType)
	util.AssertEqual(t, 0, parser.index)
}

func TestParser_parseBinaryOperator(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindOperator, lexeme: "=", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "!=", startPosition: 1},
			{kind: TokenKindOperator, lexeme: ">", startPosition: 3},
			{kind: TokenKindOperator, lexeme: ">=", startPosition: 4},
			{kind: TokenKindOperator, lexeme: "<", startPosition: 6},
			{kind: TokenKindOperator, lexeme: "<=", startPosition: 7},
		},
		index: 0,
	}

	// Act & Assert
	operator, err := parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, query.BinOpEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, query.BinOpNotEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, query.BinOpGreater, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, query.BinOpGreaterEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, query.BinOpLower, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNil(t, err)
	util.AssertEqual(t, query.BinOpLowerEqual, operator)
}

func TestParser_parseNextExpression_simpleTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 1},
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
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 1, tagFilterExpression.key)
	util.AssertEqual(t, 1, tagFilterExpression.value)
	util.AssertEqual(t, query.BinOpEqual, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_simpleInnerStatement(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "this", startPosition: 0},
			{kind: TokenKindExpressionSeparator, lexeme: ".", startPosition: 4},
			{kind: TokenKindKeyword, lexeme: "ways", startPosition: 5},
			{kind: TokenKindOpeningBraces, lexeme: "{", startPosition: 9},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 10},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 11},
			{kind: TokenKindKeyword, lexeme: "b", startPosition: 12},
			{kind: TokenKindClosingBraces, lexeme: "}", startPosition: 13},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"a"},
			[][]string{{"b"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)

	subStatementExpression, isSubStatementExpression := expression.(*query.SubStatementFilterExpression)
	util.AssertTrue(t, isSubStatementExpression)
	util.AssertNotNil(t, subStatementExpression.statement)
	util.AssertNotNil(t, subStatementExpression.statement.filter)

	tagFilterExpression, isTagFilterExpression := subStatementExpression.statement.filter.(*query.TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 0, tagFilterExpression.key)
	util.AssertEqual(t, 0, tagFilterExpression.value)
	util.AssertEqual(t, query.BinOpEqual, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_simpleKeyFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 1},
			{kind: TokenKindWildcard, lexeme: "*", startPosition: 2},
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
	keyFilterExpression, isKeyFilterExpression := expression.(*query.KeyFilterExpression)
	util.AssertTrue(t, isKeyFilterExpression)
	util.AssertEqual(t, 1, keyFilterExpression.key)
	util.AssertEqual(t, true, keyFilterExpression.shouldBeSet)
}

func TestParser_parseNextExpression_invalidTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 1},
			{kind: TokenKindOperator, lexeme: "<", startPosition: 2},
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
			{kind: TokenKindOperator, lexeme: "!", startPosition: 0},
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 1},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 2},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 3},
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

	negatedTagFilterExpression, isNegatedTagFilterExpression := expression.(*query.NegatedFilterExpression)
	util.AssertTrue(t, isNegatedTagFilterExpression)
	util.AssertNotNil(t, negatedTagFilterExpression.baseExpression)

	tagFilterExpression, isTagFilterExpression := negatedTagFilterExpression.baseExpression.(*query.TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 1, tagFilterExpression.key)
	util.AssertEqual(t, 1, tagFilterExpression.value)
	util.AssertEqual(t, query.BinOpEqual, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_invalidNegatedTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindOperator, lexeme: "!", startPosition: 0},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 1},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 2},
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
			{kind: TokenKindOperator, lexeme: "!", startPosition: 0},
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
			{kind: TokenKindOperator, lexeme: "=", startPosition: 2},
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
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 1, tagFilterExpression.key)
	util.AssertEqual(t, 1, tagFilterExpression.value)
	util.AssertEqual(t, query.BinOpEqual, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_expressionInsideParenthesesMissinClose(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindOpeningParenthesis, lexeme: "(", startPosition: 0},
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 1},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 2},
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

func TestParser_parseNextExpression_determineNextSmallerValue_greaterThanOperator(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "width", startPosition: 0},
			{kind: TokenKindOperator, lexeme: ">=", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "2.5", startPosition: 7},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"width"},
			[][]string{{"2", "2.2", "2.5test", "3"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 0, tagFilterExpression.key)
	util.AssertEqual(t, 1, tagFilterExpression.value)
	util.AssertEqual(t, query.BinOpGreater, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_determineNextSmallerValue_lowerOperatorOnHugeValue(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "width", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "<", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "100", startPosition: 7},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"width"},
			[][]string{{"2", "2.2", "3", "50test"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 0, tagFilterExpression.key)
	util.AssertEqual(t, 3, tagFilterExpression.value)
	util.AssertEqual(t, query.BinOpLowerEqual, tagFilterExpression.operator)
}

func TestParser_parseNextExpression_determineNextSmallerValue_equalOperator(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "width", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "2.5", startPosition: 6},
		},
		index: -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex(
			[]string{"width"},
			[][]string{{"2", "2.2", "3"}},
		),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	util.AssertNil(t, err)
	util.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	util.AssertTrue(t, isTagFilterExpression)
	util.AssertEqual(t, 0, tagFilterExpression.key)
	util.AssertEqual(t, -1, tagFilterExpression.value)
	util.AssertEqual(t, query.BinOpEqual, tagFilterExpression.operator)
}

func TestParser_parseBinaryOperator_invalidAndNotExistingToken(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindOperator, lexeme: "!", startPosition: 0},
		},
		index: 0,
	}

	// Act & Assert
	operator, err := parser.parseBinaryOperator("previous", -123)
	util.AssertNotNil(t, err)
	util.AssertEqual(t, query.BinOpInvalid, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	util.AssertNotNil(t, err)
	util.AssertEqual(t, query.BinOpInvalid, operator)
}
