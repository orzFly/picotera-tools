package picotera

import (
	"encoding/json"
	"regexp"
	"strings"
)

// Mirrors pkg/server/project_extractor.go. PicoTera matches against the
// client's request body, before any rewrite.
var projectExtractRegexps = []*regexp.Regexp{
	regexp.MustCompile(`Workspace root folder: (.*?)(?:\\n|\n|$|")`),
	regexp.MustCompile(`Primary working directory: (.*?)(?:\\n|\n|$|")`),
	regexp.MustCompile(`Current working directory: (.*?)(?:\\n|\n|$|")`),
	regexp.MustCompile(`<cwd>(.*?)</cwd>`),
	regexp.MustCompile(`<env>(?:\\n|\n)Working directory: (.*?)(?:\\n|\n|$|")`),
	regexp.MustCompile(`Today is [0-9-]+, and the current working directory is '(.*?)'.(?:\\n|\n|$|")`),
	regexp.MustCompile(`current working directory: '(.*?)'.`),
}

const autoCreateMinPathLen = 5

// ProjectPaths returns the decoded workspace paths found in body, in regexp
// order, deduplicated.
func ProjectPaths(body []byte) []string {
	var out []string
	seen := map[string]bool{}
	for _, re := range projectExtractRegexps {
		for _, m := range re.FindAllSubmatch(body, -1) {
			if len(m) < 2 {
				continue
			}
			var s string
			if json.Unmarshal(append(append([]byte{'"'}, m[1]...), '"'), &s) != nil || s == "" || seen[s] {
				continue
			}
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func AncestorPaths(p string) []string {
	if p == "" {
		return nil
	}
	out := []string{p}
	cur := p
	for {
		i := strings.LastIndexAny(cur, `/\`)
		if i < 0 {
			break
		}
		if i == 0 {
			if len(cur) > 1 {
				out = append(out, cur[:1])
			}
			break
		}
		cur = cur[:i]
		out = append(out, cur)
	}
	return out
}

// AutoCreateSource picks the path PicoTera would seed an auto-created project
// with: the longest one over the minimum length. Empty means none qualifies.
func AutoCreateSource(paths []string) string {
	source := ""
	for _, p := range paths {
		if len(p) > autoCreateMinPathLen && len(p) > len(source) {
			source = p
		}
	}
	return source
}

func LastPathComponent(p string) string {
	p = strings.TrimRight(p, `/\`)
	if p == "" {
		return ""
	}
	if i := strings.LastIndexAny(p, `/\`); i >= 0 {
		return p[i+1:]
	}
	return p
}
