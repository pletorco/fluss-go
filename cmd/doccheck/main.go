// Command doccheck verifies consumer-facing Go documentation.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type finding struct {
	path   string
	line   int
	symbol string
	reason string
}

type auditor struct {
	fset     *token.FileSet
	packages map[string]bool
	findings []finding
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: doccheck <file-or-directory> [...]")
		return 2
	}
	findings, err := audit(args)
	if err != nil {
		fmt.Fprintf(stderr, "doccheck: %v\n", err)
		return 2
	}
	for _, item := range findings {
		fmt.Fprintf(stdout, "%s:%d: %s: %s\n", item.path, item.line, item.symbol, item.reason)
	}
	if len(findings) != 0 {
		return 1
	}
	return 0
}

func audit(paths []string) ([]finding, error) {
	auditor := &auditor{
		fset:     token.NewFileSet(),
		packages: make(map[string]bool),
	}
	for _, root := range paths {
		if err := auditor.scan(root); err != nil {
			return nil, fmt.Errorf("scan %s: %w", root, err)
		}
	}
	auditor.addPackageFindings()
	sort.Slice(auditor.findings, func(i, j int) bool {
		return findingLess(auditor.findings[i], auditor.findings[j])
	})
	return auditor.findings, nil
}

func (a *auditor) scan(root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return directoryAction(root, path, entry.Name())
		}
		if !isSourceFile(path) {
			return nil
		}
		return a.auditFile(path)
	})
}

func directoryAction(root, path, name string) error {
	if path != root && strings.HasPrefix(name, ".") {
		return filepath.SkipDir
	}
	return nil
}

func isSourceFile(path string) bool {
	return strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")
}

func (a *auditor) auditFile(path string) error {
	file, err := parser.ParseFile(a.fset, path, nil, parser.ParseComments)
	if err != nil {
		return err
	}
	if ast.IsGenerated(file) {
		return nil
	}
	key := filepath.Dir(path) + "\x00" + file.Name.Name
	if _, ok := a.packages[key]; !ok {
		a.packages[key] = false
	}
	if file.Doc != nil {
		a.packages[key] = true
	}
	a.findings = append(a.findings, declarationFindings(a.fset, path, file)...)
	return nil
}

func (a *auditor) addPackageFindings() {
	for key, documented := range a.packages {
		if documented {
			continue
		}
		directory, packageName, _ := strings.Cut(key, "\x00")
		a.findings = append(a.findings, finding{
			path: directory, symbol: "package " + packageName, reason: "missing package comment",
		})
	}
}

func findingLess(left, right finding) bool {
	if left.path != right.path {
		return left.path < right.path
	}
	if left.line != right.line {
		return left.line < right.line
	}
	return left.symbol < right.symbol
}

func declarationFindings(fset *token.FileSet, path string, file *ast.File) []finding {
	var findings []finding
	for _, declaration := range file.Decls {
		switch declaration := declaration.(type) {
		case *ast.FuncDecl:
			if declaration.Name.IsExported() && receiverExported(declaration.Recv) &&
				declaration.Doc == nil {
				findings = append(findings, missingComment(
					fset, path, declaration.Pos(), declaration.Name.Name,
				))
			}
		case *ast.GenDecl:
			findings = append(findings, generalDeclarationFindings(
				fset, path, declaration,
			)...)
		}
	}
	return findings
}

func generalDeclarationFindings(
	fset *token.FileSet,
	path string,
	declaration *ast.GenDecl,
) []finding {
	var findings []finding
	for _, spec := range declaration.Specs {
		findings = append(findings, generalSpecFindings(
			fset, path, declaration.Doc, spec,
		)...)
	}
	return findings
}

func generalSpecFindings(
	fset *token.FileSet,
	path string,
	groupDoc *ast.CommentGroup,
	spec ast.Spec,
) []finding {
	switch spec := spec.(type) {
	case *ast.TypeSpec:
		return typeSpecFindings(fset, path, groupDoc, spec)
	case *ast.ValueSpec:
		return valueSpecFindings(fset, path, groupDoc, spec)
	default:
		return nil
	}
}

func typeSpecFindings(
	fset *token.FileSet,
	path string,
	groupDoc *ast.CommentGroup,
	spec *ast.TypeSpec,
) []finding {
	if !spec.Name.IsExported() {
		return nil
	}
	var findings []finding
	if groupDoc == nil && spec.Doc == nil {
		findings = append(findings, missingComment(fset, path, spec.Pos(), spec.Name.Name))
	}
	if interfaceType, ok := spec.Type.(*ast.InterfaceType); ok {
		findings = append(findings, interfaceFindings(
			fset, path, spec.Name.Name, interfaceType,
		)...)
	}
	return findings
}

func valueSpecFindings(
	fset *token.FileSet,
	path string,
	groupDoc *ast.CommentGroup,
	spec *ast.ValueSpec,
) []finding {
	if groupDoc != nil || spec.Doc != nil {
		return nil
	}
	var findings []finding
	for _, name := range spec.Names {
		if name.IsExported() {
			findings = append(findings, missingComment(fset, path, name.Pos(), name.Name))
		}
	}
	return findings
}

func interfaceFindings(
	fset *token.FileSet,
	path, owner string,
	interfaceType *ast.InterfaceType,
) []finding {
	var findings []finding
	for _, method := range interfaceType.Methods.List {
		if method.Doc != nil || method.Comment != nil {
			continue
		}
		for _, name := range method.Names {
			if name.IsExported() {
				findings = append(findings, missingComment(
					fset, path, name.Pos(), owner+"."+name.Name,
				))
			}
		}
	}
	return findings
}

func receiverExported(receiver *ast.FieldList) bool {
	if receiver == nil {
		return true
	}
	expression := receiver.List[0].Type
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}
	switch value := expression.(type) {
	case *ast.IndexExpr:
		expression = value.X
	case *ast.IndexListExpr:
		expression = value.X
	}
	name, ok := expression.(*ast.Ident)
	return ok && name.IsExported()
}

func missingComment(
	fset *token.FileSet,
	path string,
	position token.Pos,
	symbol string,
) finding {
	return finding{
		path: path, line: fset.Position(position).Line,
		symbol: symbol, reason: "missing doc comment",
	}
}
