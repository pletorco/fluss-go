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
	fset := token.NewFileSet()
	packages := make(map[string]bool)
	var findings []finding
	for _, root := range paths {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if path != root && strings.HasPrefix(entry.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if err != nil {
				return err
			}
			if ast.IsGenerated(file) {
				return nil
			}
			packageKey := filepath.Dir(path) + "\x00" + file.Name.Name
			if _, ok := packages[packageKey]; !ok {
				packages[packageKey] = false
			}
			if file.Doc != nil {
				packages[packageKey] = true
			}
			findings = append(findings, declarationFindings(fset, path, file)...)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", root, err)
		}
	}
	for key, documented := range packages {
		if documented {
			continue
		}
		directory, packageName, _ := strings.Cut(key, "\x00")
		findings = append(findings, finding{
			path: directory, symbol: "package " + packageName, reason: "missing package comment",
		})
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].path != findings[j].path {
			return findings[i].path < findings[j].path
		}
		if findings[i].line != findings[j].line {
			return findings[i].line < findings[j].line
		}
		return findings[i].symbol < findings[j].symbol
	})
	return findings, nil
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
		switch spec := spec.(type) {
		case *ast.TypeSpec:
			if spec.Name.IsExported() && declaration.Doc == nil && spec.Doc == nil {
				findings = append(findings, missingComment(
					fset, path, spec.Pos(), spec.Name.Name,
				))
			}
			if interfaceType, ok := spec.Type.(*ast.InterfaceType); ok &&
				spec.Name.IsExported() {
				findings = append(findings, interfaceFindings(
					fset, path, spec.Name.Name, interfaceType,
				)...)
			}
		case *ast.ValueSpec:
			if declaration.Doc != nil || spec.Doc != nil {
				continue
			}
			for _, name := range spec.Names {
				if name.IsExported() {
					findings = append(findings, missingComment(
						fset, path, name.Pos(), name.Name,
					))
				}
			}
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
