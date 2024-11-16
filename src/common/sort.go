package common

import (
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type sortableString struct {
	value                string
	isNumber             bool    // True when item is a number without string text
	containsNumberPrefix bool    // Is true is the item is a number or has a number prefix (like in "12 ft")
	number               float64 // The numerical value of the prefix or number. Only contains a useful value when containsNumberPrefix is true.
}

func (this sortableString) isLessThan(other sortableString) bool {
	if this.containsNumberPrefix && other.containsNumberPrefix {
		if this.number == other.number {
			if this.isNumber {
				return true
			}
			return false
		}
		return this.number < other.number
	}

	return this.value < other.value
}

func toSortableString(s string) sortableString {
	sortableStringObj := sortableString{
		value: s,
	}

	numberPrefix := extractNumberPrefix(s)
	sortableStringObj.containsNumberPrefix = numberPrefix != ""
	sortableStringObj.isNumber = len(numberPrefix) == len(s)

	if sortableStringObj.containsNumberPrefix {
		sortableStringObj.number, _ = strconv.ParseFloat(numberPrefix, 64)
	}
	return sortableStringObj
}

func Sort(values []string) []string {
	var sortableStrings []sortableString
	for _, s := range values {
		s = strings.TrimSpace(s)

		sortableStringObj := toSortableString(s)

		sortableStrings = append(sortableStrings, sortableStringObj)
	}

	sort.Slice(sortableStrings, func(i, j int) bool {
		// A "less" function that returns "true" is item i is less than item j.
		s1 := sortableStrings[i]
		s2 := sortableStrings[j]
		return s1.isLessThan(s2)
	})

	sortedStrings := make([]string, len(sortableStrings))
	for i, s := range sortableStrings {
		sortedStrings[i] = s.value
	}

	return sortedStrings
}

// IsLessThan returns true if s1 is less than s2, i.e. if s1 appears before s2 in a sorted list.
func IsLessThan(s1, s2 string) bool {
	s1Sortable := toSortableString(s1)
	s2Sortable := toSortableString(s2)
	return s1Sortable.isLessThan(s2Sortable)
}

func extractNumberPrefix(s string) string {
	var prefix []rune

	for _, r := range []rune(s) {
		if r == '-' || r == '.' || unicode.IsDigit(r) {
			prefix = append(prefix, r)
		}
	}

	prefixString := string(prefix)
	if isNumber(prefixString) {
		return prefixString
	}

	return ""
}

func isNumber(s string) bool {
	containsDecimalPoint := false

	if len(s) == 0 {
		return false
	}

	for i, c := range s {
		if c == '-' && i != 0 {
			// A dash is only allowed at the beginning
			return false
		} else if c == '.' {
			if containsDecimalPoint {
				// Decimal point already found -> invalid since two decimal points do not make sense
				return false
			}
			containsDecimalPoint = true
		} else if !unicode.IsDigit(c) {
			return false
		}
	}

	return true
}
