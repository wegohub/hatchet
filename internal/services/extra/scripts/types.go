//go:build ignore

// 生成阶段读取 NATS 公开结构体；运行时不使用 AST 或反射。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// Field 保存原生字段声明，生成器据此保持指针、数值和容器语义。
type Field struct {
	// Name 是导出字段名称，嵌入字段为空。
	Name string
	// Type 是 Go 类型表达式，包含指针和容器信息。
	Type string
	// Tag 和 Doc 保留字段注解，供协议审查与生成使用。
	Tag string
	Doc string
}

// Type 描述原生导出类型；生成仅处理明确选择的公开结构体。
type Type struct {
	// Package 和 Name 唯一定位原生类型。
	Package string
	Name    string
	// Kind 区分 struct 与别名，Base 保存别名的基础类型。
	Kind string
	Base string
	// Doc 保留类型用途说明，Fields 保持字段声明顺序。
	Doc    string
	Fields []Field
}

// main 读取固定依赖的导出声明，失败时终止生成，不能输出部分字段模型。
func main() {
	if len(os.Args) != 2 {
		panic("expected NATS module directory")
	}
	root := os.Args[1]
	fs := token.NewFileSet()
	out := []Type{}
	render := func(n ast.Node) string {
		var b bytes.Buffer
		if err := printer.Fprint(&b, fs, n); err != nil {
			panic(err)
		}
		return b.String()
	}
	for _, dir := range []string{
		".",
		"jetstream",
		"micro",
	} {
		files, err := filepath.Glob(filepath.Join(root, dir, "*.go"))
		if err != nil {
			panic(err)
		}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			f, e := parser.ParseFile(fs, path, nil, parser.ParseComments)
			if e != nil {
				panic(e)
			}
			pkg := "nats"
			if dir != "." {
				pkg = dir
			}
			for _, d := range f.Decls {
				g, ok := d.(*ast.GenDecl)
				if !ok {
					continue
				}
				for _, s := range g.Specs {
					t, ok := s.(*ast.TypeSpec)
					if !ok || !t.Name.IsExported() {
						continue
					}
					v := Type{
						Package: pkg,
						Name:    t.Name.Name,
						Kind:    "alias",
						Base:    render(t.Type),
					}
					if t.Doc != nil {
						v.Doc = t.Doc.Text()
					} else if g.Doc != nil {
						v.Doc = g.Doc.Text()
					}
					if st, ok := t.Type.(*ast.StructType); ok {
						v.Kind = "struct"
						v.Base = ""
						for _, ff := range st.Fields.List {
							tag, doc := "", ""
							if ff.Tag != nil {
								tag = ff.Tag.Value
							}
							if ff.Doc != nil {
								doc = ff.Doc.Text()
							}
							if len(ff.Names) == 0 {
								v.Fields = append(v.Fields, Field{"", render(ff.Type), tag, doc})
							}
							for _, n := range ff.Names {
								if n.IsExported() {
									v.Fields = append(v.Fields, Field{n.Name, render(ff.Type), tag, doc})
								}
							}
						}
					}
					out = append(out, v)
				}
			}
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		panic(fmt.Errorf("encode native declarations: %w", err))
	}
}
