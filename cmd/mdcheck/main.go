// Command mdcheck verifies local Markdown links and synchronized Go snippets.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/text"
)

var (
	snippetReference = regexp.MustCompile(
		`^\s*<!--\s*go-source:\s+(\S+)\s+([A-Za-z][A-Za-z0-9_-]*)\s*-->\s*$`,
	)
	htmlAnchor = regexp.MustCompile(
		`(?i)<[a-z][^>]*\s(?:id|name)=["']([^"']+)["'][^>]*>`,
	)
)

type finding struct {
	path   string
	line   int
	reason string
}

type report struct {
	files         int
	localLinks    int
	externalLinks int
	snippets      int
	skipped       int
	findings      []finding
}

type document struct {
	path     string
	source   []byte
	root     ast.Node
	headings map[string]struct{}
}

type replacement struct {
	start int
	stop  int
	value []byte
}

type checker struct {
	repository   string
	write        bool
	markdown     goldmark.Markdown
	documents    map[string]*document
	snippetCache map[string]map[string][]byte
	replacements map[string][]replacement
	report       report
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("mdcheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	write := flags.Bool("write", false, "update Go fences from compile-checked sources")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() == 0 {
		fmt.Fprintln(stderr, "usage: mdcheck [-write] <file-or-directory> [...]")
		return 2
	}
	repository, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(stderr, "mdcheck: working directory: %v\n", err)
		return 2
	}
	result, err := audit(repository, flags.Args(), *write)
	if err != nil {
		fmt.Fprintf(stderr, "mdcheck: %v\n", err)
		return 2
	}
	for _, item := range result.findings {
		fmt.Fprintf(stdout, "%s:%d: %s\n", item.path, item.line, item.reason)
	}
	fmt.Fprintf(
		stdout,
		"mdcheck: %d files, %d local links, %d external URLs skipped, "+
			"%d Go snippets synchronized, %d generated or vendored files skipped\n",
		result.files,
		result.localLinks,
		result.externalLinks,
		result.snippets,
		result.skipped,
	)
	if len(result.findings) != 0 {
		return 1
	}
	return 0
}

func audit(repository string, paths []string, write bool) (report, error) {
	repository, err := filepath.Abs(repository)
	if err != nil {
		return report{}, err
	}
	files, skipped, err := markdownFiles(repository, paths)
	if err != nil {
		return report{}, err
	}
	checker := &checker{
		repository:   repository,
		write:        write,
		markdown:     goldmark.New(goldmark.WithExtensions(extension.GFM)),
		documents:    make(map[string]*document),
		snippetCache: make(map[string]map[string][]byte),
		replacements: make(map[string][]replacement),
		report:       report{skipped: skipped},
	}
	for _, path := range files {
		doc, err := checker.loadDocument(path)
		if err != nil {
			return report{}, err
		}
		checker.report.files++
		if err := checker.checkDocument(doc); err != nil {
			return report{}, err
		}
	}
	if write {
		if err := checker.applyReplacements(); err != nil {
			return report{}, err
		}
	}
	sort.Slice(checker.report.findings, func(i, j int) bool {
		left, right := checker.report.findings[i], checker.report.findings[j]
		if left.path != right.path {
			return left.path < right.path
		}
		if left.line != right.line {
			return left.line < right.line
		}
		return left.reason < right.reason
	})
	return checker.report, nil
}

func markdownFiles(repository string, paths []string) ([]string, int, error) {
	seen := make(map[string]struct{})
	skipped := 0
	for _, input := range paths {
		path := input
		if !filepath.IsAbs(path) {
			path = filepath.Join(repository, path)
		}
		err := filepath.WalkDir(path, func(current string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			relative, err := filepath.Rel(repository, current)
			if err != nil {
				return err
			}
			if outsideRepository(relative) {
				return fmt.Errorf("%s is outside repository root", input)
			}
			if entry.IsDir() {
				if current != path && excludedDirectory(entry.Name()) {
					skipped++
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.EqualFold(filepath.Ext(current), ".md") {
				return nil
			}
			source, err := os.ReadFile(current)
			if err != nil {
				return err
			}
			if generatedMarkdown(source) {
				skipped++
				return nil
			}
			seen[filepath.Clean(current)] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, 0, fmt.Errorf("scan %s: %w", input, err)
		}
	}
	files := make([]string, 0, len(seen))
	for path := range seen {
		files = append(files, path)
	}
	sort.Strings(files)
	return files, skipped, nil
}

func excludedDirectory(name string) bool {
	return name == "vendor" || name == "third_party" || name == ".git" || name == ".tools"
}

func generatedMarkdown(source []byte) bool {
	prefix := source
	if len(prefix) > 2048 {
		prefix = prefix[:2048]
	}
	text := strings.ToLower(string(prefix))
	return strings.Contains(text, "code generated") && strings.Contains(text, "do not edit")
}

func (c *checker) loadDocument(path string) (*document, error) {
	path = filepath.Clean(path)
	if doc := c.documents[path]; doc != nil {
		return doc, nil
	}
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", c.relative(path), err)
	}
	root := c.markdown.Parser().Parse(text.NewReader(source))
	doc := &document{
		path: path, source: source, root: root, headings: headingIDs(root, source),
	}
	for _, match := range htmlAnchor.FindAllSubmatch(source, -1) {
		doc.headings[string(match[1])] = struct{}{}
	}
	c.documents[path] = doc
	return doc, nil
}

func headingIDs(root ast.Node, source []byte) map[string]struct{} {
	result := make(map[string]struct{})
	counts := make(map[string]int)
	_ = ast.Walk(root, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		heading, ok := node.(*ast.Heading)
		if !entering || !ok {
			return ast.WalkContinue, nil
		}
		base := githubSlug(string(heading.Text(source)))
		id := base
		if duplicate := counts[base]; duplicate > 0 {
			id = fmt.Sprintf("%s-%d", base, duplicate)
		}
		counts[base]++
		result[id] = struct{}{}
		return ast.WalkContinue, nil
	})
	return result
}

func githubSlug(value string) string {
	var slug strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case unicode.IsSpace(r):
			slug.WriteByte('-')
		case unicode.IsLetter(r), unicode.IsNumber(r), unicode.IsSymbol(r), r == '-', r == '_':
			slug.WriteRune(r)
		}
	}
	return slug.String()
}

func (c *checker) checkDocument(doc *document) error {
	return ast.Walk(doc.root, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch value := node.(type) {
		case *ast.Link:
			c.checkLink(doc, node, string(value.Destination))
		case *ast.Image:
			c.checkLink(doc, node, string(value.Destination))
		case *ast.FencedCodeBlock:
			if strings.EqualFold(string(value.Language(doc.source)), "go") ||
				strings.EqualFold(string(value.Language(doc.source)), "golang") {
				if err := c.checkGoSnippet(doc, value); err != nil {
					return ast.WalkStop, err
				}
			}
		}
		return ast.WalkContinue, nil
	})
}

func (c *checker) checkLink(doc *document, node ast.Node, destination string) {
	line := nodeLine(doc.source, node)
	parsed, err := url.Parse(destination)
	if err != nil {
		c.addFinding(doc.path, line, fmt.Sprintf("invalid link %q: %v", destination, err))
		return
	}
	if parsed.IsAbs() || strings.HasPrefix(destination, "//") {
		c.report.externalLinks++
		return
	}
	c.report.localLinks++
	decodedPath, err := url.PathUnescape(parsed.Path)
	if err != nil {
		c.addFinding(doc.path, line, fmt.Sprintf("invalid escaped link path %q", destination))
		return
	}
	target := doc.path
	if decodedPath != "" {
		if filepath.IsAbs(decodedPath) {
			target = filepath.Join(c.repository, filepath.FromSlash(strings.TrimPrefix(decodedPath, "/")))
		} else {
			target = filepath.Join(filepath.Dir(doc.path), filepath.FromSlash(decodedPath))
		}
		target = filepath.Clean(target)
		relative, relErr := filepath.Rel(c.repository, target)
		if relErr != nil || outsideRepository(relative) {
			c.addFinding(doc.path, line, fmt.Sprintf("link escapes repository: %q", destination))
			return
		}
		if _, statErr := os.Stat(target); statErr != nil {
			c.addFinding(doc.path, line, fmt.Sprintf("link target does not exist: %q", destination))
			return
		}
	}
	if parsed.Fragment == "" {
		return
	}
	fragment, err := url.PathUnescape(parsed.Fragment)
	if err != nil {
		c.addFinding(doc.path, line, fmt.Sprintf("invalid escaped anchor %q", destination))
		return
	}
	if info, statErr := os.Stat(target); statErr == nil && info.IsDir() {
		target = filepath.Join(target, "README.md")
	}
	if !strings.EqualFold(filepath.Ext(target), ".md") {
		c.addFinding(doc.path, line, fmt.Sprintf("anchor target is not Markdown: %q", destination))
		return
	}
	targetDoc, err := c.loadDocument(target)
	if err != nil {
		c.addFinding(doc.path, line, fmt.Sprintf("cannot inspect anchor %q: %v", destination, err))
		return
	}
	if _, ok := targetDoc.headings[fragment]; !ok {
		c.addFinding(doc.path, line, fmt.Sprintf("anchor does not exist: %q", destination))
	}
}

func outsideRepository(relative string) bool {
	return relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func nodeLine(source []byte, node ast.Node) int {
	position := node.Pos()
	if position < 0 && node.Lines() != nil && node.Lines().Len() > 0 {
		position = node.Lines().At(0).Start
	}
	if position < 0 {
		for child := node.FirstChild(); child != nil; child = child.NextSibling() {
			if child.Pos() >= 0 {
				position = child.Pos()
				break
			}
		}
	}
	if position < 0 {
		return 1
	}
	return bytes.Count(source[:min(position, len(source))], []byte{'\n'}) + 1
}

func (c *checker) checkGoSnippet(doc *document, block *ast.FencedCodeBlock) error {
	line := nodeLine(doc.source, block)
	sourceLines := bytes.Split(doc.source, []byte{'\n'})
	markerIndex := line - 2
	if markerIndex < 0 || markerIndex >= len(sourceLines) {
		c.addFinding(doc.path, line, "Go block is missing an adjacent go-source marker")
		return nil
	}
	match := snippetReference.FindSubmatch(sourceLines[markerIndex])
	if match == nil {
		c.addFinding(doc.path, line, "Go block is missing an adjacent go-source marker")
		return nil
	}
	sourcePath := filepath.Clean(filepath.Join(c.repository, filepath.FromSlash(string(match[1]))))
	relative, err := filepath.Rel(c.repository, sourcePath)
	if err != nil || outsideRepository(relative) ||
		!strings.HasPrefix(filepath.ToSlash(relative), "internal/docexamples/") ||
		!strings.HasSuffix(relative, "_test.go") {
		c.addFinding(
			doc.path,
			line,
			fmt.Sprintf("Go source must be an internal/docexamples/*_test.go file: %q", match[1]),
		)
		return nil
	}
	snippet, err := c.loadSnippet(sourcePath, string(match[2]))
	if err != nil {
		c.addFinding(doc.path, line, err.Error())
		return nil
	}
	c.report.snippets++
	actual := normalizeSnippet(block.Text(doc.source))
	expected := normalizeSnippet(snippet)
	if bytes.Equal(actual, expected) {
		return nil
	}
	if !c.write {
		c.addFinding(
			doc.path,
			line,
			fmt.Sprintf("Go block differs from %s snippet %q", filepath.ToSlash(relative), match[2]),
		)
		return nil
	}
	start, stop, ok := blockContentRange(block)
	if !ok {
		c.addFinding(doc.path, line, "cannot update an empty Go block")
		return nil
	}
	c.replacements[doc.path] = append(c.replacements[doc.path], replacement{
		start: start, stop: stop, value: expected,
	})
	return nil
}

func (c *checker) loadSnippet(path, name string) ([]byte, error) {
	snippets := c.snippetCache[path]
	if snippets == nil {
		source, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read Go source %s: %w", c.relative(path), err)
		}
		snippets, err = parseSnippets(source)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.relative(path), err)
		}
		c.snippetCache[path] = snippets
	}
	value, ok := snippets[name]
	if !ok {
		return nil, fmt.Errorf("%s: snippet %q does not exist", c.relative(path), name)
	}
	return value, nil
}

func parseSnippets(source []byte) (map[string][]byte, error) {
	const (
		startPrefix = "// doc:snippet "
		endPrefix   = "// doc:snippet-end "
	)
	result := make(map[string][]byte)
	lines := bytes.Split(source, []byte{'\n'})
	for index := 0; index < len(lines); index++ {
		trimmed := strings.TrimSpace(string(lines[index]))
		if trimmed != strings.TrimSpace(startPrefix) && !strings.HasPrefix(trimmed, startPrefix) {
			continue
		}
		if trimmed == strings.TrimSpace(startPrefix) {
			return nil, fmt.Errorf("line %d: empty snippet name", index+1)
		}
		name := strings.TrimSpace(strings.TrimPrefix(trimmed, startPrefix))
		if _, exists := result[name]; exists {
			return nil, fmt.Errorf("line %d: duplicate snippet %q", index+1, name)
		}
		start := index + 1
		for index++; index < len(lines); index++ {
			end := strings.TrimSpace(string(lines[index]))
			if end == endPrefix+name {
				result[name] = dedent(lines[start:index])
				break
			}
		}
		if _, ok := result[name]; !ok {
			return nil, fmt.Errorf("snippet %q has no matching end marker", name)
		}
	}
	return result, nil
}

func dedent(lines [][]byte) []byte {
	indent := -1
	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		width := len(line) - len(bytes.TrimLeft(line, " \t"))
		if indent < 0 || width < indent {
			indent = width
		}
	}
	if indent < 0 {
		indent = 0
	}
	var output bytes.Buffer
	for _, line := range lines {
		if len(line) >= indent {
			line = line[indent:]
		}
		output.Write(line)
		output.WriteByte('\n')
	}
	return output.Bytes()
}

func normalizeSnippet(source []byte) []byte {
	source = bytes.ReplaceAll(source, []byte("\r\n"), []byte("\n"))
	return append(bytes.TrimRight(source, "\n"), '\n')
}

func blockContentRange(block *ast.FencedCodeBlock) (int, int, bool) {
	if block.Lines().Len() == 0 {
		return 0, 0, false
	}
	return block.Lines().At(0).Start, block.Lines().At(block.Lines().Len() - 1).Stop, true
}

func (c *checker) applyReplacements() error {
	for path, replacements := range c.replacements {
		doc := c.documents[path]
		sort.Slice(replacements, func(i, j int) bool {
			return replacements[i].start > replacements[j].start
		})
		source := append([]byte(nil), doc.source...)
		for _, change := range replacements {
			if change.start < 0 || change.stop < change.start || change.stop > len(source) {
				return errors.New("invalid snippet replacement range")
			}
			source = append(source[:change.start], append(change.value, source[change.stop:]...)...)
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, source, info.Mode().Perm()); err != nil {
			return fmt.Errorf("write %s: %w", c.relative(path), err)
		}
	}
	return nil
}

func (c *checker) addFinding(path string, line int, reason string) {
	c.report.findings = append(c.report.findings, finding{
		path: c.relative(path), line: line, reason: reason,
	})
}

func (c *checker) relative(path string) string {
	relative, err := filepath.Rel(c.repository, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(relative)
}
