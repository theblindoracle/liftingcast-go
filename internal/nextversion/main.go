// Command nextversion works out the version a merged PR releases, from the
// PR's labels and the repo's existing tags. CI runs it on pushes to main to
// decide which tag to create; it never creates or pushes anything itself, so
// running it locally is a dry run.
//
// It reads tags from stdin, one per line, and takes the PR's labels as
// arguments. The current version is the highest vX.Y.Z tag (v0.0.0 if there
// is none); tags of any other shape are ignored.
//
//	release:minor  prints the next minor version: v0.3.0 -> v0.4.0
//	release:patch  prints the next patch version: v0.3.0 -> v0.3.1
//	neither        prints nothing
//	both           fails and prints nothing
//
// Usage:
//
//	git tag -l | go run ./internal/nextversion release:minor
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
)

const (
	labelMinor = "release:minor"
	labelPatch = "release:patch"
)

func main() {
	next, err := run(os.Stdin, os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "nextversion:", err)
		os.Exit(1)
	}
	if next != "" {
		fmt.Println(next)
	}
}

// run returns the version to release, or "" if the labels ask for no release.
func run(tags io.Reader, labels []string) (string, error) {
	var minor, patch bool
	for _, l := range labels {
		switch l {
		case labelMinor:
			minor = true
		case labelPatch:
			patch = true
		}
	}
	if minor && patch {
		return "", errors.New("the PR has both " + labelMinor + " and " + labelPatch + "; remove one")
	}
	if !minor && !patch {
		return "", nil
	}

	cur, err := highest(tags)
	if err != nil {
		return "", err
	}
	if minor {
		cur = version{cur.major, cur.minor + 1, 0}
	} else {
		cur.patch++
	}
	return cur.String(), nil
}

type version struct{ major, minor, patch int }

func (v version) String() string { return fmt.Sprintf("v%d.%d.%d", v.major, v.minor, v.patch) }

func (v version) less(w version) bool {
	if v.major != w.major {
		return v.major < w.major
	}
	if v.minor != w.minor {
		return v.minor < w.minor
	}
	return v.patch < w.patch
}

var tagPattern = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// highest returns the highest vX.Y.Z tag read from r, or v0.0.0 if there is none.
func highest(r io.Reader) (version, error) {
	var best version
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		m := tagPattern.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		var v version
		var err error
		if v.major, err = strconv.Atoi(m[1]); err != nil {
			continue
		}
		if v.minor, err = strconv.Atoi(m[2]); err != nil {
			continue
		}
		if v.patch, err = strconv.Atoi(m[3]); err != nil {
			continue
		}
		if best.less(v) {
			best = v
		}
	}
	return best, sc.Err()
}
