package axonimport

import (
	"regexp"
	"sort"
	"strings"
)

// Rules is how a workspace path maps to a project key. It implements the
// changes proposed for PicoTera in docs/picotera-project-matching.md, so the
// importer, reproject and merge-projects agree on one canonical form.
type Rules struct {
	Aliases []Alias
	// Blacklist paths (canonical) never seed a project and never match as an
	// ancestor; a project path equal to the workspace still matches.
	Blacklist map[string]bool
	// Roots are canonical directories whose subdirectories belong to them,
	// for repositories whose first request came from a subdirectory.
	Roots []string
}

type Alias struct {
	From *regexp.Regexp // anchored at the start, ends on a path boundary
	To   string         // canonical prefix, always '/'-separated
}

const MainRoot = "/home/alice/src"

func DefaultRules() *Rules {
	alias := func(re, to string) Alias { return Alias{From: regexp.MustCompile(re), To: to} }
	return &Rules{
		Aliases: []Alias{
			alias(`^/Users/alice/src(?:[\\/]|$)`, MainRoot),
			alias(`^(?i:D:\\Alice\\src)(?:\\|$)`, MainRoot),
			alias(`^/home/al/src(?:/|$)`, MainRoot),
			alias(`^/home/alice/sandbox/homes/bob/code(?:/|$)`, "/home/alice/code"),
		},
		Blacklist: map[string]bool{
			"/home/alice":     true,
			"/home/carol": true,
			"/home/al":   true,
			"/Users/alice":    true,
			"/root":          true,
			`D:\Alice`:        true,
			`C:\Users\alice`:  true,
		},
	}
}

var worktreeRe = regexp.MustCompile(`^(.+?)[\\/](?:\.claude[\\/]worktrees|\.worktrees)[\\/]([^\\/]+)`)

// CleanPath trims whitespace and trailing separators. One live path carried
// the newlines around a <cwd> block, which never matches anything.
func CleanPath(p string) string {
	p = strings.TrimSpace(p)
	for len(p) > 1 && (strings.HasSuffix(p, "/") || strings.HasSuffix(p, `\`)) {
		p = p[:len(p)-1]
	}
	return p
}

// RepoRoot maps a path inside a worktree to the same path under the
// repository root, keeping the original separators.
func RepoRoot(p string) string {
	c := CleanPath(p)
	m := worktreeRe.FindStringSubmatchIndex(c)
	if m == nil {
		return c
	}
	rest := c[m[1]:]
	if rest != "" && rest[0] != '/' && rest[0] != '\\' {
		return c // the worktree name only matched a prefix of a segment
	}
	return c[m[2]:m[3]] + rest
}

func WorktreeName(p string) string {
	c := CleanPath(p)
	m := worktreeRe.FindStringSubmatchIndex(c)
	if m == nil {
		return ""
	}
	if rest := c[m[1]:]; rest != "" && rest[0] != '/' && rest[0] != '\\' {
		return ""
	}
	return c[m[4]:m[5]]
}

// Canonical is the grouping key: repository root, with other machines'
// directories rewritten to their local equivalent.
func (r *Rules) Canonical(p string) string {
	root := RepoRoot(p)
	for _, a := range r.Aliases {
		loc := a.From.FindStringIndex(root)
		if loc == nil {
			continue
		}
		rest := strings.TrimLeft(root[loc[1]:], `\/`)
		rest = strings.ReplaceAll(rest, `\`, "/")
		root = a.To
		if rest != "" {
			root += "/" + rest
		}
		break
	}
	for _, r := range r.Roots {
		if root == r || strings.HasPrefix(root, r+"/") || strings.HasPrefix(root, r+`\`) {
			return r
		}
	}
	return root
}

func Basename(p string) string {
	c := CleanPath(p)
	if i := strings.LastIndexAny(c, `\/`); i >= 0 {
		return c[i+1:]
	}
	return c
}

// Workdir is one working-directory mention in a request body.
type Workdir struct {
	Path string
	Was  string // the previous directory of a Claude Code environment update
	pos  int
}

// Claude Code announces mid-session worktree switches as
// "Primary working directory: <new> (was <old>)"; PicoTera's pattern runs to
// the end of the line and swallows the suffix.
var workdirRegexps = []*regexp.Regexp{
	regexp.MustCompile(`Workspace root folder: (.*?)(?:\\n|\n|$|")`),
	regexp.MustCompile(`Primary working directory: (.*?)(?: \(was (.*?)\))?(?:\\n|\n|$|")`),
	regexp.MustCompile(`Current working directory: (.*?)(?: \(was (.*?)\))?(?:\\n|\n|$|")`),
	regexp.MustCompile(`<cwd>(.*?)</cwd>`),
	regexp.MustCompile(`<env>(?:\\n|\n)Working directory: (.*?)(?:\\n|\n|$|")`),
	regexp.MustCompile(`Today is [0-9-]+, and the current working directory is '(.*?)'.(?:\\n|\n|$|")`),
	regexp.MustCompile(`current working directory: '(.*?)'.`),
}

// Workdirs returns every mention in body order; the last one is where the
// client was when it sent the request.
func Workdirs(body []byte) []Workdir {
	var out []Workdir
	for _, re := range workdirRegexps {
		for _, m := range re.FindAllSubmatchIndex(body, -1) {
			w := Workdir{pos: m[0]}
			w.Path = decodeCapture(body, m[2], m[3])
			if len(m) >= 6 && m[4] >= 0 {
				w.Was = decodeCapture(body, m[4], m[5])
			}
			if w.Path != "" {
				out = append(out, w)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].pos < out[j].pos })
	return out
}

func decodeCapture(body []byte, start, end int) string {
	if start < 0 {
		return ""
	}
	raw := body[start:end]
	buf := make([]byte, 0, len(raw)+2)
	buf = append(append(append(buf, '"'), raw...), '"')
	var s string
	if err := jsonUnmarshalString(buf, &s); err != nil {
		return ""
	}
	return CleanPath(s)
}

// Ancestors lists p and its parent directories, longest first.
func Ancestors(p string) []string {
	out := []string{p}
	cur := p
	for {
		i := strings.LastIndexAny(cur, `/\`)
		if i < 0 {
			return out
		}
		if i == 0 {
			if len(cur) > 1 {
				out = append(out, cur[:1])
			}
			return out
		}
		cur = cur[:i]
		out = append(out, cur)
	}
}
