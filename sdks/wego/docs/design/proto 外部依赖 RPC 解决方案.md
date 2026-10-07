以下是完整的 proto 外部依赖 RPC 解决方案，包含自定义 option、匹配规则、运行时解析逻辑和使用示例。

---

### 一、自定义 Option 定义

```protobuf
// graph_options.proto
syntax = "proto3";
package graph.options;

option go_package = "graph/options;options";

import "google/protobuf/descriptor.proto";

extend google.protobuf.MethodOptions {
  // 当方法名匹配到多个外部 rpc 时，用这个精确指定目标
  // 支持格式: "Method"、"Service.Method"、"package.Service.Method"
  string target_method = 50000;
}
```

---

### 二、外部依赖 Proto（被依赖方）

```protobuf
// external.proto
syntax = "proto3";
package myapp.external;

option go_package = "myapp/external;external";

message Token {
  string value = 1;
}

message Result {
  bool ok = 1;
  string message = 2;
}

message Data {
  string content = 1;
}

service AuthService {
  rpc Validate(Token) returns (Result);
  rpc Validate1(Token) returns (Result);
}

service AnotherService {
  rpc Validate(Token) returns (Result);  // 与 AuthService.Validate 签名相同
}

service DataService {
  rpc Fetch(Token) returns (Data);
}
```

---

### 三、当前 Proto（依赖方）

```protobuf
// pipeline.proto
syntax = "proto3";
package myapp.pipeline;

option go_package = "myapp/pipeline;pipeline";

import "google/protobuf/descriptor.proto";
import "graph_options.proto";
import "external.proto";

service Pipeline {
  // 场景1：Token → Result 匹配到 Validate 和 Validate1
  // 方法名相同，自动选择 Validate，无需 option
  rpc Validate(Token) returns (Result) {}

  // 场景2：Token → Data 只匹配到 DataService.Fetch
  // 唯一匹配，自动绑定，无需 option
  rpc FetchData(Token) returns (Data) {}

  // 场景3：Token → Result 匹配到两个 Validate（AuthService 和 AnotherService）
  // 方法名相同且 service 不同，必须通过 option 指定
  rpc ValidateStrict(Token) returns (Result) {
    option (graph.options.target_method) = "AuthService.Validate";
  }

  // 场景4：Token → Result 无同名候选
  // 必须通过 option 指定
  rpc CustomStep(Token) returns (Result) {
    option (graph.options.target_method) = "Validate";
  }
}
```

---

### 四、运行时解析逻辑（Go）

```go
package graph

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"graph/options"
)

// ExternalRPC 匹配到的外部 rpc 信息
type ExternalRPC struct {
	Package  string // myapp.external
	Service  string // AuthService
	Method   string // Validate
	FullName string // myapp.external.AuthService.Validate
}

// ResolveExternalDeps 解析当前 service 中所有 rpc 的外部依赖
func ResolveExternalDeps(currentService protoreflect.ServiceDescriptor) ([]ExternalRPC, error) {
	var results []ExternalRPC

	for i := 0; i < currentService.Methods().Len(); i++ {
		method := currentService.Methods().Get(i)
		inputType := method.Input()
		outputType := method.Output()

		// 规则1：入参和响应必须来自同一个包
		inputPkg := inputType.ParentFile().Package()
		outputPkg := outputType.ParentFile().Package()
		if inputPkg != outputPkg {
			continue
		}

		// 遍历所有已注册的 proto 文件，查找匹配的 rpc
		candidates, err := findMatchingMethods(inputType, outputType, currentService.ParentFile().Path())
		if err != nil {
			return nil, err
		}

		var target ExternalRPC
		switch len(candidates) {
		case 0:
			// 没有匹配到任何外部 rpc，视为当前 service 自身节点
			continue
		case 1:
			// 规则2：唯一匹配，自动绑定
			target = candidates[0]
		default:
			// 规则3/4：匹配到多个，按方法名消歧义
			var err error
			target, err = resolveTargetMethod(method, candidates)
			if err != nil {
				return nil, err
			}
		}

		results = append(results, target)
	}

	return results, nil
}

// findMatchingMethods 在所有已注册的 proto 中查找入参/响应类型匹配的 rpc
func findMatchingMethods(
	inputType, outputType protoreflect.Descriptor,
	currentFilePath string,
) ([]ExternalRPC, error) {
	var matches []ExternalRPC

	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		// 跳过当前文件自身
		if fd.Path() == currentFilePath {
			return true
		}

		for s := 0; s < fd.Services().Len(); s++ {
			svc := fd.Services().Get(s)
			for m := 0; m < svc.Methods().Len(); m++ {
				method := svc.Methods().Get(m)
				if method.Input().FullName() == inputType.FullName() &&
					method.Output().FullName() == outputType.FullName() {
					matches = append(matches, ExternalRPC{
						Package:  string(fd.Package()),
						Service:  string(svc.Name()),
						Method:   string(method.Name()),
						FullName: string(method.FullName()),
					})
				}
			}
		}
		return true
	})

	return matches, nil
}

// resolveTargetMethod 按方法名消歧义，必要时读取 option
func resolveTargetMethod(
	currentMethod protoreflect.MethodDescriptor,
	candidates []ExternalRPC,
) (ExternalRPC, error) {
	currentName := string(currentMethod.Name())

	// 过滤出方法名相同的候选
	var nameMatches []ExternalRPC
	for _, c := range candidates {
		if c.Method == currentName {
			nameMatches = append(nameMatches, c)
		}
	}

	switch len(nameMatches) {
	case 1:
		// 规则3：只有一个同名候选，自动选择
		return nameMatches[0], nil
	case 0:
		// 没有同名候选，必须写 option
		target, err := getTargetMethod(currentMethod)
		if err != nil {
			return ExternalRPC{}, fmt.Errorf(
				"method %s: no candidate has matching name %q, target_method option required",
				currentName, currentName)
		}
		for _, c := range candidates {
			if matchTarget(c, target) {
				return c, nil
			}
		}
		return ExternalRPC{}, fmt.Errorf(
			"method %s: target_method %q not found among candidates %v",
			currentName, target, candidates)
	default:
		// 规则4：多个同名候选，必须写 option 指定 service
		target, err := getTargetMethod(currentMethod)
		if err != nil {
			return ExternalRPC{}, fmt.Errorf(
				"method %s: %d candidates have same name %q, target_method option required (format: Service.Method or package.Service.Method)",
				currentName, len(nameMatches), currentName)
		}
		for _, c := range nameMatches {
			if matchTarget(c, target) {
				return c, nil
			}
		}
		return ExternalRPC{}, fmt.Errorf(
			"method %s: target_method %q not found among name-matched candidates %v",
			currentName, target, nameMatches)
	}
}

// matchTarget 支持多种 target_method 格式匹配
func matchTarget(c ExternalRPC, target string) bool {
	// 精确匹配 FullName: "myapp.external.AuthService.Validate"
	if c.FullName == target {
		return true
	}
	// 匹配 Service.Method: "AuthService.Validate"
	if c.Service+"."+c.Method == target {
		return true
	}
	// 仅匹配方法名: "Validate"（仅在唯一同名候选时生效，此处作为兜底）
	if c.Method == target {
		return true
	}
	return false
}

// getTargetMethod 从 MethodOptions 中读取 target_method
func getTargetMethod(method protoreflect.MethodDescriptor) (string, error) {
	opts := method.Options().(*descriptorpb.MethodOptions)
	val := proto.GetExtension(opts, options.E_TargetMethod)
	if val == nil {
		return "", fmt.Errorf("target_method option not set")
	}
	targetMethod, ok := val.(string)
	if !ok || targetMethod == "" {
		return "", fmt.Errorf("target_method option is empty")
	}
	return targetMethod, nil
}
```

---

### 五、匹配规则总结

| 优先级 | 条件 | 行为 | 是否需要 option |
|---|---|---|---|
| 1 | 入参和响应来自不同包 | 跳过，不作为外部依赖 | — |
| 2 | 入参和响应匹配到**唯一** rpc | 自动绑定 | ❌ |
| 3 | 匹配到多个 rpc，但**只有一个方法名与当前 rpc 同名** | 自动绑定该同名方法 | ❌ |
| 4 | 匹配到多个 rpc，且**多个方法名都与当前 rpc 同名** | 通过 `target_method` 精确指定 | ✅ |
| 5 | 匹配到多个 rpc，但**没有一个方法名与当前 rpc 同名** | 通过 `target_method` 精确指定 | ✅ |

---

### 六、`target_method` 支持格式

| 格式 | 示例 | 匹配范围 |
|---|---|---|
| 仅方法名 | `"Validate"` | 匹配任意同名方法（仅在候选唯一时安全） |
| Service.Method | `"AuthService.Validate"` | 精确匹配指定 service 下的方法 |
| package.Service.Method | `"myapp.external.AuthService.Validate"` | 全限定名，最精确 |

推荐始终使用 `Service.Method` 或 `package.Service.Method` 格式，避免歧义。

---

### 七、使用示例

```go
func main() {
	// 从生成的代码中获取 service descriptor
	var currentService protoreflect.ServiceDescriptor = pipeline.File_myapp_pipeline_pipeline_proto.Services().ByName("Pipeline")

	deps, err := ResolveExternalDeps(currentService)
	if err != nil {
		log.Fatal(err)
	}

	for _, dep := range deps {
		fmt.Printf("外部依赖: %s\n", dep.FullName)
		// dep.FullName = "myapp.external.AuthService.Validate"
		// 通过 protoregistry.GlobalFiles.FindDescriptorByName 解析出 MethodDescriptor
		// 然后调用对应的 gRPC 客户端执行
	}
}
```

---

### 八、核心设计要点

- **不写死路径**：option 只存方法名或 `Service.Method`，包名和服务名由运行时自动推导
- **跨文件查找**：通过 `protoregistry.GlobalFiles.RangeFiles` 遍历所有已编译的 proto
- **编译期安全**：入参/响应类型是强类型 descriptor，类型不存在时编译阶段报错
- **渐进式消歧义**：大部分场景无需写 option，只有真正歧义时才需要手动指定

---
需要我把这部分代码封装成独立的 Go 模块吗？比如 `github.com/yourorg/protodep`，方便你直接 `go get` 复用。
