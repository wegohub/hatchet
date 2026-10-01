package clickhouse

import (
	"regexp"
	"testing"
)

func TestLogSearchPattern(t *testing.T) {
	cases := []struct {
		query, message string
		want           bool
	}{
		{"error", "An ERROR occurred", true},
		{"a_b", "a猫b", true},
		{"a_b", "a猫猫b", false},
		{"a%b", "a\n猫b", true},
		{`a\%b`, "a%b", true},
		{`a\%b`, "a--b", false},
		{`a\_b`, "a_b", true},
		{`a\_b`, "axb", false},
		{"[bad]", "literal [bad] pattern", true},
		{"", "anything", true},
		{`tail\`, "tail%", true},
	}
	for _, tc := range cases {
		pattern, err := searchPattern(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		rx, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatal(err)
		}
		if got := rx.MatchString(tc.message); got != tc.want {
			t.Errorf("%q in %q: %v, want %v", tc.query, tc.message, got, tc.want)
		}
	}
}
