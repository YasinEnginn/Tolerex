package logger

import "testing"

func TestParseLevel(t *testing.T) {
	for _, test := range []struct {
		input string
		want  Level
	}{
		{input: "debug", want: DEBUG},
		{input: "INFO", want: INFO},
		{input: "warning", want: WARN},
		{input: "error", want: ERROR},
	} {
		got, ok := parseLevel(test.input)
		if !ok || got != test.want {
			t.Fatalf("parseLevel(%q)=(%v,%v), want (%v,true)", test.input, got, ok, test.want)
		}
	}
	if _, ok := parseLevel("trace"); ok {
		t.Fatal("unknown log level accepted")
	}
}

func TestSafeName(t *testing.T) {
	if got := safeName("member:5556/../x"); got != "member_5556____x" {
		t.Fatalf("unexpected safe name %q", got)
	}
}
