package parser

import (
	"github.com/paulmach/orb"
	"soq/common"
	"soq/feature"
	"soq/index"
	"soq/query"
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
	common.AssertEqual(t, parser.token[0], token)

	token = parser.peekNextToken()
	common.AssertEqual(t, parser.token[1], token)
	token = parser.moveToNextToken()
	common.AssertEqual(t, parser.token[1], token)

	token = parser.currentToken()
	common.AssertEqual(t, parser.token[1], token)
	token = parser.peekNextToken()
	common.AssertNil(t, token)
	token = parser.moveToNextToken()
	common.AssertNil(t, token)
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
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)
	expectedBbox := &orb.Bound{
		Min: orb.Point{1.1, 2.2},
		Max: orb.Point{3, 4.567},
	}
	common.AssertEqual(t, expectedBbox, expression.GetBbox())
	common.AssertEqual(t, 6, parser.index)
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
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)
	bboxExpression, isBboxExpression := expression.(*query.BboxLocationExpression)
	common.AssertTrue(t, isBboxExpression)
	expectedBbox := &orb.Bound{
		Min: orb.Point{1.1, 2.2},
		Max: orb.Point{3, 4.567},
	}
	common.AssertEqual(t, expectedBbox, bboxExpression.GetBbox())
	common.AssertEqual(t, 6, parser.index)
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
	common.AssertNotNil(t, err)
	common.AssertNil(t, expression)
	common.AssertEqual(t, 5, parser.index)
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
	common.AssertNotNil(t, err)
	common.AssertNil(t, expression)
	common.AssertEqual(t, 1, parser.index)
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
	queryType, err := parser.parseOsmQueryType(false)

	// Assert
	common.AssertNil(t, err)
	common.AssertEqual(t, feature.OsmQueryNode, queryType)
	common.AssertEqual(t, 0, parser.index)
}

func TestParser_parseOsmObjectType_butNotChildRelationsOnNormalExpression(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "child_relations", startPosition: 0},
			{kind: TokenKindKeyword, lexeme: "foo", startPosition: 0},
		},
		index: 0,
	}

	// Act
	queryType, err := parser.parseOsmQueryType(false)

	// Assert
	common.AssertNotNil(t, err)
	common.AssertEqual(t, feature.OsmQueryType(-1), queryType)
}

func TestParser_parseOsmObjectType_childRelations(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "child_relations", startPosition: 0},
			{kind: TokenKindKeyword, lexeme: "foo", startPosition: 0},
		},
		index: 0,
	}

	// Act
	queryType, err := parser.parseOsmQueryType(true)

	// Assert
	common.AssertNil(t, err)
	common.AssertEqual(t, feature.OsmQueryChildRelation, queryType)
	common.AssertEqual(t, 0, parser.index)
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
	common.AssertNil(t, err)
	common.AssertEqual(t, query.BinOpEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	common.AssertNil(t, err)
	common.AssertEqual(t, query.BinOpNotEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	common.AssertNil(t, err)
	common.AssertEqual(t, query.BinOpGreater, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	common.AssertNil(t, err)
	common.AssertEqual(t, query.BinOpGreaterEqual, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	common.AssertNil(t, err)
	common.AssertEqual(t, query.BinOpLower, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	common.AssertNil(t, err)
	common.AssertEqual(t, query.BinOpLowerEqual, operator)
}

func TestParser_parseNextExpression_simpleTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 1},
			{kind: TokenKindKeyword, lexeme: "b", startPosition: 2},
		},
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"foo", "a"}, [][]string{{"bar"}, {"blubb", "b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	common.AssertTrue(t, isTagFilterExpression)
	key, value, operator := tagFilterExpression.GetParameter()
	common.AssertEqual(t, 1, key)
	common.AssertEqual(t, 1, value)
	common.AssertEqual(t, query.BinOpEqual, operator)
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
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"a"}, [][]string{{"b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)

	subStatementExpression, isSubStatementExpression := expression.(*query.SubStatementFilterExpression)
	common.AssertTrue(t, isSubStatementExpression)
	common.AssertNotNil(t, subStatementExpression.GetStatement())
	common.AssertNotNil(t, subStatementExpression.GetStatement().GetFilterExpression())

	tagFilterExpression, isTagFilterExpression := subStatementExpression.GetStatement().GetFilterExpression().(*query.TagFilterExpression)
	common.AssertTrue(t, isTagFilterExpression)
	key, value, operator := tagFilterExpression.GetParameter()
	common.AssertEqual(t, 0, key)
	common.AssertEqual(t, 0, value)
	common.AssertEqual(t, query.BinOpEqual, operator)
}

func TestParser_parseNextExpression_simpleKeyFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 1},
			{kind: TokenKindWildcard, lexeme: "*", startPosition: 2},
		},
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"foo", "a"}, [][]string{{"bar"}, {"blubb", "b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)
	keyFilterExpression, isKeyFilterExpression := expression.(*query.KeyFilterExpression)
	common.AssertTrue(t, isKeyFilterExpression)
	key, shouldBeSet := keyFilterExpression.GetParameter()
	common.AssertEqual(t, 1, key)
	common.AssertEqual(t, true, shouldBeSet)
}

func TestParser_parseNextExpression_invalidTagFilter(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "a", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 1},
			{kind: TokenKindOperator, lexeme: "<", startPosition: 2},
		},
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"foo", "a"}, [][]string{{"bar"}, {"blubb", "b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNotNil(t, err)
	common.AssertNil(t, expression)
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
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"foo", "a"}, [][]string{{"bar"}, {"blubb", "b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)

	negatedTagFilterExpression, isNegatedTagFilterExpression := expression.(*query.NegatedFilterExpression)
	common.AssertTrue(t, isNegatedTagFilterExpression)
	common.AssertNotNil(t, negatedTagFilterExpression.GetBaseExpression())

	tagFilterExpression, isTagFilterExpression := negatedTagFilterExpression.GetBaseExpression().(*query.TagFilterExpression)
	common.AssertTrue(t, isTagFilterExpression)
	key, value, operator := tagFilterExpression.GetParameter()
	common.AssertEqual(t, 1, key)
	common.AssertEqual(t, 1, value)
	common.AssertEqual(t, query.BinOpEqual, operator)
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
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"foo", "a"}, [][]string{{"bar"}, {"blubb", "b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNotNil(t, err)
	common.AssertNil(t, expression)
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
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"foo", "a"}, [][]string{{"bar"}, {"blubb", "b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNotNil(t, err)
	common.AssertNil(t, expression)
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
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"foo", "a"}, [][]string{{"bar"}, {"blubb", "b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	common.AssertTrue(t, isTagFilterExpression)
	key, value, operator := tagFilterExpression.GetParameter()
	common.AssertEqual(t, 1, key)
	common.AssertEqual(t, 1, value)
	common.AssertEqual(t, query.BinOpEqual, operator)
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
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"foo", "a"}, [][]string{{"bar"}, {"blubb", "b"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNotNil(t, err)
	common.AssertNil(t, expression)
}

func TestParser_parseNextExpression_determineNextSmallerValue_greaterThanOperator(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "width", startPosition: 0},
			{kind: TokenKindOperator, lexeme: ">=", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "2.5", startPosition: 7},
		},
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"width"}, [][]string{{"2", "2.2", "2.5test", "3"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	common.AssertTrue(t, isTagFilterExpression)
	key, value, operator := tagFilterExpression.GetParameter()
	common.AssertEqual(t, 0, key)
	common.AssertEqual(t, 1, value)
	common.AssertEqual(t, query.BinOpGreater, operator)
}

func TestParser_parseNextExpression_determineNextSmallerValue_lowerOperatorOnHugeValue(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "width", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "<", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "100", startPosition: 7},
		},
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"width"}, [][]string{{"2", "2.2", "3", "50test"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	common.AssertTrue(t, isTagFilterExpression)
	key, value, operator := tagFilterExpression.GetParameter()
	common.AssertEqual(t, 0, key)
	common.AssertEqual(t, 3, value)
	common.AssertEqual(t, query.BinOpLowerEqual, operator)
}

func TestParser_parseNextExpression_determineNextSmallerValue_equalOperator(t *testing.T) {
	// Arrange
	parser := &Parser{
		token: []*Token{
			{kind: TokenKindKeyword, lexeme: "width", startPosition: 0},
			{kind: TokenKindOperator, lexeme: "=", startPosition: 5},
			{kind: TokenKindNumber, lexeme: "2.5", startPosition: 6},
		},
		index:    -1, // Because of "moveToNextToken()" call in parser function
		tagIndex: index.NewTagIndex([]string{"width"}, [][]string{{"2", "2.2", "3"}}),
	}

	// Act
	expression, err := parser.parseNextExpression()

	// Assert
	common.AssertNil(t, err)
	common.AssertNotNil(t, expression)
	tagFilterExpression, isTagFilterExpression := expression.(*query.TagFilterExpression)
	common.AssertTrue(t, isTagFilterExpression)
	key, value, operator := tagFilterExpression.GetParameter()
	common.AssertEqual(t, 0, key)
	common.AssertEqual(t, -1, value)
	common.AssertEqual(t, query.BinOpEqual, operator)
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
	common.AssertNotNil(t, err)
	common.AssertEqual(t, query.BinOpInvalid, operator)

	parser.moveToNextToken()
	operator, err = parser.parseBinaryOperator("previous", -123)
	common.AssertNotNil(t, err)
	common.AssertEqual(t, query.BinOpInvalid, operator)
}
