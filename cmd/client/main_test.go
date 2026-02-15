package main

import "testing"

func TestIsControlBannerLine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
		want bool
	}{
		{name: "banner separator", line: "--------------------------------------------------", want: true},
		{name: "banner title", line: "TOLEREX - Leader Node (Control Plane)", want: true},
		{name: "banner command line", line: "HELP                      Show this help", want: true},
		{name: "normal data begins with s", line: "Secret payload", want: false},
		{name: "normal data begins with g", line: "Get me back", want: false},
		{name: "normal data begins with h", line: "Hello world", want: false},
		{name: "normal data begins with q", line: "Quantum text", want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isControlBannerLine(tc.line)
			if got != tc.want {
				t.Fatalf("isControlBannerLine(%q)=%v, want %v", tc.line, got, tc.want)
			}
		})
	}
}
