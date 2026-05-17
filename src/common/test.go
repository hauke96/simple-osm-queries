package common

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/hauke96/sigolo/v2"
)

func AssertEqual(t *testing.T, expected any, actual any) {
	expectedIsString := false
	actualIsString := false

	switch expected.(type) {
	case string:
		expectedIsString = true
	}
	switch actual.(type) {
	case string:
		actualIsString = true
	}

	if !reflect.DeepEqual(expected, actual) {
		if expectedIsString && actualIsString {
			assertEqualStrings(t, expected.(string), actual.(string))
		} else {
			sigolo.Errorb(1, "Expect to be equal.\nExpected: %+v\n----------\nActual  : %+v\n", expected, actual)
			t.Fail()
		}
	}
}

func AssertApprox[T float32 | float64](t *testing.T, expected T, actual T, accuracy T) {
	if math.Abs(float64(expected-actual)) > float64(accuracy) {
		t.Fail()
	}
}

func assertEqualStrings(t *testing.T, expected string, actual string) {
	expected = strings.ReplaceAll(expected, "\n", "\\n\n")

	actual = strings.ReplaceAll(actual, "\n", "\\n\n")

	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")

	sigolo.Errorb(2, "Expect to be equal.\n|   | %-50s | %-50s |", "Expected", "Actual")
	fmt.Printf("|%s|\n", strings.Repeat("-", 109))

	for i, expectedLine := range expectedLines {
		actualLine := ""
		if len(actualLines) > i {
			actualLine = actualLines[i]
		}

		changeMark := " "
		if actualLine != expectedLine {
			changeMark = "*"
		}

		fmt.Printf("| %s | %-50s | %-50s |\n", changeMark, "\""+expectedLine+"\"", "\""+actualLine+"\"")
	}

	if len(actualLines) > len(expectedLines) {
		for i := len(expectedLines); i < len(actualLines); i++ {
			actualLine := actualLines[i]
			fmt.Printf("| * | %-50s | %-50s |\n", "", "\""+actualLine+"\"")
		}
	}

	t.Fail()
}

func AssertNil(t *testing.T, value any) {
	if value != nil && !reflect.ValueOf(value).IsNil() {
		sigolo.Errorb(1, "Expect to be 'nil' but was: %+v", value)
		t.Fail()
	}
}

func AssertNotNil(t *testing.T, value any) {
	if value == nil || reflect.ValueOf(value).IsNil() {
		sigolo.Errorb(1, "Expect NOT to be 'nil' but was: %+v", value)
		t.Fail()
	}
}

func AssertError(t *testing.T, expectedMessage string, err error) {
	if expectedMessage != err.Error() {
		sigolo.Errorb(1, "Expected message: %s\nActual error message: %s", expectedMessage, err.Error())
		t.Fail()
	}
}

func AssertEmptyString(t *testing.T, s string) {
	if "" != s {
		sigolo.Errorb(1, "Expected: empty string\nActual  : %s", s)
		t.Fail()
	}
}

func AssertTrue(t *testing.T, b bool) {
	if !b {
		sigolo.Errorb(1, "Expected true but got false")
		t.Fail()
	}
}

func AssertFalse(t *testing.T, b bool) {
	if b {
		sigolo.Errorb(1, "Expected false but got true")
		t.Fail()
	}
}

func AssertMatch(t *testing.T, regexString string, content string) {
	regex := regexp.MustCompile(regexString)
	if !regex.MatchString(content) {
		sigolo.Errorb(1, "Expected to match\nRegex: %s\nContent: %s", regexString, content)
		t.Fail()
	}
}

func AssertNoMatch(t *testing.T, regexString string, content string) {
	regex := regexp.MustCompile(regexString)
	if regex.MatchString(content) {
		sigolo.Errorb(1, "Expected NOT to match\nRegex: %s\nContent: %s", regexString, content)
		t.Fail()
	}
}

func AssertContainsExactlyInAnyOrder[V comparable](t *testing.T, expected []V, actual []V) {
	if len(expected) != len(actual) {
		sigolo.Errorb(1, "Expected lists to be of equal size but found %d actual elements while expecting %d", len(actual), len(expected))
		t.Fail()
	}

	var valueInActualListButNotExpected []V
	for _, actualValue := range actual {
		found := false
		for _, expectedValue := range expected {
			if reflect.DeepEqual(actualValue, expectedValue) {
				found = true
			}
		}
		if !found {
			valueInActualListButNotExpected = append(valueInActualListButNotExpected, actualValue)
		}
	}

	var valuesExpectedButNotInActualList []V
	for _, expectedValue := range expected {
		found := false
		for _, actualValue := range actual {
			if reflect.DeepEqual(expectedValue, actualValue) {
				found = true
			}
		}
		if !found {
			valuesExpectedButNotInActualList = append(valuesExpectedButNotInActualList, expectedValue)
		}
	}

	if len(valueInActualListButNotExpected) != 0 || len(valuesExpectedButNotInActualList) != 0 {
		foundButNotExpectedString := ""
		for _, v := range valueInActualListButNotExpected {
			foundButNotExpectedString += fmt.Sprintf("%+v\n", v)
		}

		expectedButNotFoundString := ""
		for _, v := range valuesExpectedButNotInActualList {
			expectedButNotFoundString += fmt.Sprintf("%+v\n", v)
		}

		sigolo.Errorb(1, `Expect to be equal.

Values found but not expected:
%s
Values expected but not found:
%s`, foundButNotExpectedString, expectedButNotFoundString)
		t.Fail()
	}
}
