package quality

import (
	"encoding/json"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// rootPath 是仓库 module import 前缀，用于区分 Hatchet 实现包与第三方依赖。
const rootPath = "github.com/hatchet-dev/hatchet"

// sdkPath 是 wego import 前缀；其内部包允许实现自己的适配，但不能泄漏 Hatchet 类型。
const sdkPath = rootPath + "/sdks/wego"

// root 定位仓库根目录，避免质量测试依赖启动时的工作目录。
func root(t *testing.T) string {
	t.Helper()
	// path, err 接收 filepath.Abs 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	path, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// TestOnlyBackendImportsHatchet 扫描整个 SDK 与示例的 imports，断言 backend 之外没有 Hatchet 实现依赖。
func TestOnlyBackendImportsHatchet(t *testing.T) {
	// 组合仓库与资源路径，扫描、生成和清理使用同一位置。
	base := filepath.Join(root(t), "sdks", "wego")
	// err 接收 filepath.WalkDir 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".tools" || entry.Name() == "docs" || entry.Name() == "deployment" {
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		// file, err 接收 parser.ParseFile 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}

		// 检查每项 import：例如 pkg/client 仅允许出现在 internal/backend 中。
		for _, imp := range file.Imports {
			// value, _ 还原字面量中的真实路径或名称，用于准确检查 import 边界。
			value, _ := strconv.Unquote(imp.Path.Value)
			if strings.HasPrefix(value, "github.com/hatchet-dev/") && !strings.HasPrefix(value, sdkPath) {
				// relative, _ 生成相对扫描路径，后端导入许可仅适用于内部 backend 目录。
				relative, _ := filepath.Rel(base, path)
				if !strings.HasPrefix(filepath.ToSlash(relative), "internal/backend/") {
					t.Errorf("%s imports implementation %s", relative, value)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestPublicTypeGraphContainsNoHatchetTypes 加载公开包并递归检查类型图，覆盖字段、签名、别名、泛型和方法集。
func TestPublicTypeGraphContainsNoHatchetTypes(t *testing.T) {
	// command 创建可审计的外部检查命令，失败时保留退出码和完整诊断。
	command := exec.Command("go", "list", "-deps", "-export", "-json", "./sdks/wego/...")
	command.Dir = root(t)
	// Go 子进程从仓库根启动时显式选择 SDK 工作区。
	command.Env = append(os.Environ(), "GOWORK="+filepath.Join(root(t), "sdks", "wego", "go.work"))
	// output, err 接收 command.Output 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	// decoder 按数据流读取结构化结果，遇到 EOF 表示清单解析完毕。
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	// exports 包路径到 Go export 文件的映射，用于类型图加载和公开依赖检查。
	exports := map[string]string{}
	// targets 待检查的 wego 公开包路径集合，不把测试内部包当作业务 API。
	targets := []string{}
	// go list 输出连续 JSON 对象；读取到 EOF 才完成清单，格式错误不能被视为正常结束。
	for {
		// p go list 的包路径与 export 文件记录，用于递归加载公开类型。
		var p struct {
			// ImportPath, Export go list 返回的包路径与类型导出数据文件。
			ImportPath, Export string
		}
		if err = decoder.Decode(&p); err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		exports[p.ImportPath] = p.Export
		if strings.HasPrefix(p.ImportPath, sdkPath) && !strings.Contains(strings.TrimPrefix(p.ImportPath, sdkPath), "/internal/") && !strings.Contains(p.ImportPath, "/tests/") && !strings.Contains(p.ImportPath, "/examples/") {
			targets = append(targets, p.ImportPath)
		}
	}
	// loader 读取 Go 导出类型图，检查别名、泛型和完整方法集的边界。
	loader := importer.ForCompiler(token.NewFileSet(), "gc", func(path string) (io.ReadCloser, error) {
		return os.Open(exports[path])
	})
	// 每个公开包都加载实际 export 数据，避免只检查根包而遗漏子包暴露的类型。
	for _, path := range targets {
		// p, err 接收 loader.Import 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		p, err := loader.Import(path)
		if err != nil {
			t.Fatal(err)
		}
		// 从每项导出声明递归检查类型图；公开别名与方法签名也必须遵守隔离规则。
		for _, name := range p.Scope().Names() {
			if ast.IsExported(name) {
				checkType(t, p.Scope().Lookup(name).Type(), map[types.Type]bool{}, path+"."+name)
			}
		}
	}
}

// checkType 递归检查公开类型图中的签名、字段、方法集和泛型参数，禁止 Hatchet 类型泄漏。
func checkType(t *testing.T, v types.Type, seen map[types.Type]bool, origin string) {
	if v == nil || seen[v] {
		return
	}

	seen[v] = true
	// n, ok 识别命名类型及存在标记，后续继续检查其底层类型、方法集和泛型约束。
	if n, ok := v.(*types.Named); ok {
		// p 读取声明所属包，路径在 Hatchet 实现范围而非 wego 范围时立即报告公开类型泄漏。
		if p := n.Obj().Pkg(); p != nil && strings.HasPrefix(p.Path(), rootPath) && !strings.HasPrefix(p.Path(), sdkPath) {
			t.Errorf("%s exposes %s", origin, n)
			return
		}

		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := 0; i < n.NumMethods(); i++ {
			if n.Method(i).Exported() {
				checkType(t, n.Method(i).Type(), seen, origin)
			}
		}
		checkType(t, n.Underlying(), seen, origin)
		// params 读取泛型约束集合，存在时递归检查每个约束，防止公开类型经泛型泄漏后端依赖。
		if params := n.TypeParams(); params != nil {
			// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
			for i := 0; i < params.Len(); i++ {
				checkType(t, params.At(i).Constraint(), seen, origin)
			}
		}
		return
	}

	// n 保存类型分派后的准确值，各分支只按该类型读取字段或调用方法。
	switch n := v.(type) {
	case *types.Alias:
		checkType(t, types.Unalias(n), seen, origin)
	case *types.Pointer:
		checkType(t, n.Elem(), seen, origin)
	case *types.Slice:
		checkType(t, n.Elem(), seen, origin)
	case *types.Array:
		checkType(t, n.Elem(), seen, origin)
	case *types.Map:
		checkType(t, n.Key(), seen, origin)
		checkType(t, n.Elem(), seen, origin)
	case *types.Chan:
		checkType(t, n.Elem(), seen, origin)
	case *types.Struct:
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := 0; i < n.NumFields(); i++ {
			if n.Field(i).Exported() {
				checkType(t, n.Field(i).Type(), seen, origin)
			}
		}
	case *types.Signature:
		checkType(t, n.Params(), seen, origin)
		checkType(t, n.Results(), seen, origin)
	case *types.Tuple:
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := 0; i < n.Len(); i++ {
			checkType(t, n.At(i).Type(), seen, origin)
		}
	case *types.Interface:
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := 0; i < n.NumMethods(); i++ {
			checkType(t, n.Method(i).Type(), seen, origin)
		}
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := 0; i < n.NumEmbeddeds(); i++ {
			checkType(t, n.EmbeddedType(i), seen, origin)
		}
	case *types.TypeParam:
		checkType(t, n.Constraint(), seen, origin)
	case *types.Union:
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := 0; i < n.Len(); i++ {
			checkType(t, n.Term(i).Type(), seen, origin)
		}
	}
}
