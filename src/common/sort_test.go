package common

import "testing"

func TestSort_onlyNumbers(t *testing.T) {
	// Arrange
	input := []string{"3", "2", "2.5", "1", "-1", "0"}

	// Act
	output := Sort(input)

	// Assert
	AssertEqual(t, []string{"-1", "0", "1", "2", "2.5", "3"}, output)
}

func TestSort_onlyNumbersWithStringSufix(t *testing.T) {
	// Arrange
	input := []string{"1a", "1b", "2c", "1", "2"}

	// Act
	output := Sort(input)

	// Assert
	AssertEqual(t, []string{"1", "1a", "1b", "2", "2c"}, output)
}

func TestSort_numbersAndStrings(t *testing.T) {
	// Arrange
	input := []string{"1a", "a", "b", "1", "2"}

	// Act
	output := Sort(input)

	// Assert
	AssertEqual(t, []string{"1", "1a", "2", "a", "b"}, output)
}

func TestSort_onlyStrings(t *testing.T) {
	// Arrange
	input := []string{"a", "foo", "bar", "b"}

	// Act
	output := Sort(input)

	// Assert
	AssertEqual(t, []string{"a", "b", "bar", "foo"}, output)
}

func TestSort_alphanumeric(t *testing.T) {
	// Arrange
	input := []string{
		"Reitplatz 1",
		"Portal 1",
		"RE1: Büchen => Hamburg",
		"S1: Airport (Flughafen)/Poppenbüttel <=> Wedel",
		"S1: Poppenbüttel => Wedel",
		"Reitroute P1 Haake",
		"S1: Wedel => Airport (Flughafen)",
		"RE1: Rostock => Hamburg",
	}

	// Act
	output := Sort(input)

	// Assert
	AssertEqual(t, []string{
		"Portal 1",
		"RE1: Büchen => Hamburg",
		"RE1: Rostock => Hamburg",
		"Reitplatz 1",
		"Reitroute P1 Haake",
		"S1: Airport (Flughafen)/Poppenbüttel <=> Wedel",
		"S1: Poppenbüttel => Wedel",
		"S1: Wedel => Airport (Flughafen)",
	}, output)
}
