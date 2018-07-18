package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

	// Initialize the builtins.
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
)

func main() {
	sqlRE := regexp.MustCompile(`(?is)(~~~.?sql\s*)(.*?)(\s*~~~)`)
	exprRE := regexp.MustCompile(`^(?s)(\s*)(.*?)(\s*)$`)
	splitRE := regexp.MustCompile(`(?m)^>`)
	cfg := tree.DefaultPrettyCfg()
	cfg.LineWidth = 80
	cfg.UseTabs = false
	cfg.TabWidth = 2

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || ignorePath(path) {
			return nil
		}
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		n := sqlRE.ReplaceAllFunc(b, func(found []byte) []byte {
			blockMatch := sqlRE.FindSubmatch(found)
			var buf bytes.Buffer
			buf.Write(blockMatch[1])
			exprs := splitRE.Split(string(blockMatch[2]), -1)
			for i, expr := range exprs {
				expr := []byte(expr)
				if i > 0 {
					buf.WriteByte('>')
				}
				if skip(expr) {
					buf.Write(expr)
					continue
				}

				exprMatch := exprRE.FindSubmatch(expr)
				s, err := parser.ParseOne(string(exprMatch[2]))
				if err != nil {
					buf.Write(expr)
					continue
				}
				buf.Write(exprMatch[1])
				buf.WriteString(cfg.Pretty(s))
				buf.WriteByte(';')
				buf.Write(exprMatch[3])
			}
			buf.Write(blockMatch[3])
			return buf.Bytes()
		})
		if bytes.Equal(b, n) {
			return nil
		}
		return ioutil.WriteFile(path, n, 0666)
	})
	if err != nil {
		fmt.Println(err)
	}
}

var ignorePaths = map[string]bool{}

func init() {
	for _, p := range []string{
		"bytes.md",         // unwanted change of escape handling
		"sql-constants.md", // unwanted change of escape handling
	} {
		ignorePaths[p] = true
	}
}

func ignorePath(path string) bool {
	if !strings.HasSuffix(path, ".md") {
		return true
	}

	base := filepath.Base(path)
	if ignorePaths[base] {
		return true
	}

	if strings.Contains(path, "v2.1") {
		return false
	}
	// Allow processing of files in the root directory.
	if !strings.Contains(path, "/") {
		return false
	}

	return true
}

func skip(expr []byte) bool {
	for _, c := range expr {
		if c > 127 {
			return true
		}
	}

	expr = bytes.ToLower(expr)
	for _, contains := range [][]byte{
		[]byte("--"),
		[]byte("backup"),
		[]byte("begin"),
		[]byte("cancel"),
		[]byte("cluster setting"),
		[]byte("collate"),
		[]byte("commit"),
		[]byte("create role"),
		[]byte("create user"),
		[]byte("create view"),
		[]byte("drop role"),
		[]byte("drop user"),
		[]byte("export"),
		[]byte("import"),
		[]byte("partition"),
		[]byte("password"),
		[]byte("pause"),
		[]byte("reset"),
		[]byte("restore"),
		[]byte("resume"),
		[]byte("rollback"),
		[]byte("set database"),
		[]byte("set tracing"),
		[]byte("show"),
		[]byte("transaction"),
		[]byte("using gin"),

		// exprs with non-standard type names are ignored; they are often examples
		[]byte("boolean"),
		[]byte("numeric"),
		[]byte("timestamptz"),
	} {
		if bytes.Contains(expr, contains) {
			return true
		}
	}

	// https://github.com/mjibson/sqlfmt/issues/26
	s := string(expr)
	if strings.Contains(s, "create table") && strings.Contains(s, "as") {
		return true
	}

	return false
}
