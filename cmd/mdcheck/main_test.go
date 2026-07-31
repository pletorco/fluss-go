package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuditAcceptsLocalLinksAnchorsAndSynchronizedSnippets(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "internal/docexamples/snippets_test.go", `package docexamples_test

func example() {
	// doc:snippet hello
	println("hello")
	// doc:snippet-end hello
}
`)
	writeTestFile(t, root, "docs/target.md", "# Target heading\n\n## Repeated\n\n## Repeated\n")
	writeTestFile(t, root, "README.md", `# Project

[target](docs/target.md#target-heading)
[duplicate](docs/target.md#repeated-1)
[external](https://example.com)

<!-- go-source: internal/docexamples/snippets_test.go hello -->
`+"```go\nprintln(\"hello\")\n```\n")

	result, err := audit(root, []string{"README.md", "docs"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.findings) != 0 {
		t.Fatalf("findings = %#v", result.findings)
	}
	if result.files != 2 || result.localLinks != 2 || result.externalLinks != 1 || result.snippets != 1 {
		t.Fatalf("report = %#v", result)
	}
}

func TestAuditReportsBrokenLinksAnchorsAndEscapes(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "docs/target.md", "# Existing\n")
	writeTestFile(t, root, "docs/source.md", `# Source

[missing](missing.md)
[anchor](target.md#missing)
[escape](../../outside.md)
[bad escape](%zz)
`)

	result, err := audit(root, []string{"docs"}, false)
	if err != nil {
		t.Fatal(err)
	}
	assertReasons(t, result.findings,
		"link target does not exist",
		"anchor does not exist",
		"link escapes repository",
		"invalid link",
	)
}

func TestAuditReportsSnippetPolicyAndContentErrors(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "internal/docexamples/snippets_test.go", `package docexamples_test

func example() {
	// doc:snippet known
	println("expected")
	// doc:snippet-end known
}
`)
	writeTestFile(t, root, "outside_test.go", "package sample\n")
	writeTestFile(t, root, "docs/source.md", `# Source

`+"```go\nprintln(\"missing marker\")\n```\n\n"+
		"<!-- go-source: outside_test.go known -->\n```go\nprintln(\"outside\")\n```\n\n"+
		"<!-- go-source: internal/docexamples/snippets_test.go absent -->\n```go\nprintln(\"absent\")\n```\n\n"+
		"<!-- go-source: internal/docexamples/snippets_test.go known -->\n```go\nprintln(\"different\")\n```\n")

	result, err := audit(root, []string{"docs"}, false)
	if err != nil {
		t.Fatal(err)
	}
	assertReasons(t, result.findings,
		"missing an adjacent go-source marker",
		"must be an internal/docexamples",
		"snippet \"absent\" does not exist",
		"Go block differs",
	)
}

func TestAuditWritesSnippetUpdates(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "internal/docexamples/snippets_test.go", `package docexamples_test

func example() {
	// doc:snippet update
	println("new")
	// doc:snippet-end update
}
`)
	writeTestFile(t, root, "README.md", `# Project

<!-- go-source: internal/docexamples/snippets_test.go update -->
`+"```go\nprintln(\"old\")\n```\n")

	result, err := audit(root, []string{"README.md"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.findings) != 0 {
		t.Fatalf("findings = %#v", result.findings)
	}
	updated, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(updated, []byte("println(\"new\")")) ||
		bytes.Contains(updated, []byte("println(\"old\")")) {
		t.Fatalf("updated Markdown = %q", updated)
	}
}

func TestMarkdownFilesSkipsGeneratedVendoredAndHiddenContent(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "README.md", "# Included\n")
	writeTestFile(t, root, "vendor/dependency/README.md", "# Vendor\n")
	writeTestFile(t, root, "third_party/source/README.md", "# Third party\n")
	writeTestFile(t, root, ".tools/README.md", "# Tool cache\n")
	writeTestFile(t, root, "docs/generated.md", "<!-- Code generated. DO NOT EDIT. -->\n")

	files, skipped, err := markdownFiles(root, []string{"."})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || filepath.Base(files[0]) != "README.md" {
		t.Fatalf("files = %#v", files)
	}
	if skipped != 4 {
		t.Fatalf("skipped = %d, want 4", skipped)
	}
}

func TestParseSnippetsRejectsMalformedSources(t *testing.T) {
	tests := []struct {
		name   string
		source string
		reason string
	}{
		{name: "empty", source: "// doc:snippet \n", reason: "empty snippet"},
		{name: "unterminated", source: "// doc:snippet one\nx\n", reason: "no matching end"},
		{
			name: "duplicate",
			source: "// doc:snippet one\nx\n// doc:snippet-end one\n" +
				"// doc:snippet one\ny\n// doc:snippet-end one\n",
			reason: "duplicate snippet",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseSnippets([]byte(test.source))
			if err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("error = %v, want %q", err, test.reason)
			}
		})
	}
}

func TestRunUsageFindingsAndSuccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 2 {
		t.Fatalf("run(nil) = %d", code)
	}
	if !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("stderr = %q", stderr.String())
	}

	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeTestFile(t, root, "README.md", "# Project\n\n[missing](missing.md)\n")
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(workingDirectory); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"README.md"}, &stdout, &stderr); code != 1 {
		t.Fatalf("run(broken) = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
	}
	writeTestFile(t, root, "target.md", "# Target\n")
	writeTestFile(t, root, "README.md", "# Project\n\n[target](target.md)\n")
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"README.md"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(valid) = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
	}
}

func TestHelpers(t *testing.T) {
	if got := githubSlug("Hello, Go_1!"); got != "hello-go_1" {
		t.Fatalf("githubSlug() = %q", got)
	}
	if got := string(dedent([][]byte{[]byte("\tfirst"), []byte("\t\tsecond")})); got != "first\n\tsecond\n" {
		t.Fatalf("dedent() = %q", got)
	}
	if !generatedMarkdown([]byte("<!-- Code generated. DO NOT EDIT. -->")) {
		t.Fatal("generatedMarkdown() = false")
	}
	if generatedMarkdown([]byte("# handwritten")) {
		t.Fatal("generatedMarkdown() = true")
	}
}

func assertReasons(t *testing.T, findings []finding, reasons ...string) {
	t.Helper()
	combined := ""
	for _, item := range findings {
		combined += item.reason + "\n"
	}
	for _, reason := range reasons {
		if !strings.Contains(combined, reason) {
			t.Errorf("findings %q do not contain %q", combined, reason)
		}
	}
}

func writeTestFile(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
