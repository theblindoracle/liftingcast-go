package main

import (
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	current := "v0.1.0\nv0.2.0\nv0.3.0\n"
	tests := []struct {
		name   string
		tags   string
		labels []string
		want   string
	}{
		{"minor", current, []string{"release:minor"}, "v0.4.0"},
		{"patch", current, []string{"release:patch"}, "v0.3.1"},
		{"patch after patch", "v0.4.0\nv0.4.1\n", []string{"release:patch"}, "v0.4.2"},
		{"minor resets patch", "v0.4.2\n", []string{"release:minor"}, "v0.5.0"},
		{"other labels ignored", current, []string{"enhancement", "release:minor"}, "v0.4.0"},
		{"no release label", current, []string{"enhancement"}, ""},
		{"no labels", current, nil, ""},
		{"no tags", "", []string{"release:minor"}, "v0.1.0"},
		{"numeric not lexical order", "v0.9.0\nv0.10.0\nv0.2.0\n", []string{"release:patch"}, "v0.10.1"},
		{"major counts", "v0.9.0\nv1.0.0\n", []string{"release:minor"}, "v1.1.0"},
		{
			"other tag shapes ignored",
			"v0.3.0\nv0.9.0-rc1\n0.8.0\nv0.7\nlatest\nv00.8.0\n",
			[]string{"release:patch"},
			"v0.3.1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := run(strings.NewReader(tt.tags), tt.labels)
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if got != tt.want {
				t.Errorf("run = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRunBothLabels(t *testing.T) {
	got, err := run(strings.NewReader("v0.3.0\n"), []string{"release:patch", "release:minor"})
	if err == nil {
		t.Fatalf("run = %q, want an error", got)
	}
	if got != "" {
		t.Errorf("run = %q alongside error, want \"\"", got)
	}
}
