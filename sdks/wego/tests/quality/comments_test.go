package quality

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// hasChinese 判断说明是否包含中文，而非仅有生成器的英文提示。
// 例如“缓存描述”有效，“Code generated”不能满足中文注释门禁。
func hasChinese(text string) bool {
	// r 是当前 Unicode 码点，使用 Han 范围而不是按 UTF-8 字节判断。
	for _, r := range text {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}

// TestDeclarationsHaveChineseComments 检查整个 SDK 的公开和私有声明，包括测试及生成文件。
// 例如只给 Frame 写中文而漏掉私有 unknownFields 时，本门禁应报告具体文件和行号。
// 局部短变量与循环由逻辑块说明覆盖，不强制给 err 检查等 Go 惯用语逐个加注释。
// 此检查保证声明覆盖；说明的准确性、逻辑块与数据例子仍须通过源码校对确认。
func TestDeclarationsHaveChineseComments(t *testing.T) {
	// base 使用仓库绝对路径，测试从任意目录启动都扫描同一份 SDK。
	base := filepath.Join(root(t), "sdks", "wego")
	// err 保存完整目录遍历的错误；一个文件读取失败不能导致该文件被静默略过。
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		// 隐藏目录包含工具缓存和运行报告，不属于发布的 Go 源码。
		if entry.IsDir() {
			if strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		// source 是待检查文件的完整字节，读取错误需要停止本轮扫描并报告。
		source, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		// positions 把声明位置还原为真实行号；含 build tag 的文件也只解析而不启动引擎。
		positions := token.NewFileSet()
		// file 是保留注释的 Go 语法树；不依赖类型加载即可覆盖嵌入独立模块。
		file, parseErr := parser.ParseFile(positions, path, source, parser.ParseComments)
		if parseErr != nil {
			return parseErr
		}
		// lines 用于兼容匿名结构体及接口中的说明，它们没有独立的文件级 Doc。
		lines := strings.Split(string(source), "\n")
		// check 校验一个声明的紧邻说明；命名函数的参数由其函数说明统一解释。
		check := func(name string, node ast.Node, doc *ast.CommentGroup) {
			if doc != nil && hasChinese(doc.Text()) {
				return
			}
			// line 是声明起始行，转换为零基索引后向上收集连续注释。
			line := positions.Position(node.Pos()).Line
			// previous 指向声明前一行；越过 const / var / type 包装行寻找组说明。
			previous := line - 2
			if previous >= 0 {
				// text 是可能存在的单声明包装，例如“type (”，并非业务说明本身。
				text := strings.TrimSpace(lines[previous])
				if text == "type (" || text == "var (" || text == "const (" {
					previous--
				}
			}
			for previous >= 0 && strings.HasPrefix(strings.TrimSpace(lines[previous]), "//") {
				if hasChinese(lines[previous]) {
					return
				}
				previous--
			}
			t.Errorf("%s:%d: %s 缺少中文声明说明", path, line, name)
		}

		// 按 declaration 的节点类型检查声明，不把结构体字面量的赋值键当作字段重复要求说明。
		ast.Inspect(file, func(node ast.Node) bool {
			// declaration 保存类型分派后的准确值，各分支只按该类型读取字段或调用方法。
			switch declaration := node.(type) {
			case *ast.FuncDecl:
				check(declaration.Name.Name, declaration, declaration.Doc)
			case *ast.TypeSpec:
				check(declaration.Name.Name, declaration, declaration.Doc)
			case *ast.ValueSpec:
				// names 是同一逻辑声明中的变量或常量，允许一段说明解释整个声明组。
				names := make([]string, 0, len(declaration.Names))
				// name 是当前声明中的一个标识符，合并后报告整组声明的位置。
				for _, name := range declaration.Names {
					names = append(names, name.Name)
				}
				check(strings.Join(names, ","), declaration, declaration.Doc)
			case *ast.StructType:
				// field 包含当前结构体字段的类型与说明，匿名嵌入也必须有中文边界说明。
				for _, field := range declaration.Fields.List {
					check("结构体字段", field, field.Doc)
				}
			case *ast.InterfaceType:
				// method 包含接口方法或类型约束，不能因为未导出而略过检查。
				for _, method := range declaration.Methods.List {
					check("接口方法或约束", method, method.Doc)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
