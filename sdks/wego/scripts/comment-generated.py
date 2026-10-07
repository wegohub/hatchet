#!/usr/bin/env python3
"""为固定版本 protoc 输出补充中文实现说明，不修改任何 Go 语句。

业务消息、字段和 RPC 的含义来自 .proto；本步骤只解释生成器的运行时适配。
例如 nil 消息调用 GetCount 返回 0，消息描述缓存只初始化一次。
此步骤属于 generate.sh，质量门禁逐字节比较其输出，禁止手工改生成文件。
"""

import pathlib
import re
import sys


# 生成器私有字段的语义与业务字段不同，必须说明缓存和未知字段的用途。
RUNTIME_FIELDS = {
    "state": "protobuf 运行时消息状态，关联当前实例的反射描述；业务不直接读写。",
    "unknownFields": "保留当前版本不认识的字段，重新编码时保持协议前向兼容。",
    "sizeCache": "protobuf 编码大小缓存，由运行时维护，不表示业务消息条数。",
    "cc": "标准 gRPC 调用连接；传入 wego Conn 使用任务传输，原生 Conn 使用网络传输。",
}


def declaration(name: str) -> str:
    """解释生成的文件级声明；描述符、索引和方法名称承担不同职责。"""
    # 例如完整方法名用于 Invoke，不能用 SayHello 的短名称替代。
    if name.endswith("_FullMethodName"):
        return "完整 RPC 方法名常量，客户端 Invoke / NewStream 与服务注册绑定使用同一值。"
    if name.endswith("_ServiceDesc"):
        return "服务注册描述，关联完整服务名、方法与流方向，供 grpc.ServiceRegistrar 使用。"
    if name.endswith("rawDescOnce"):
        return "描述符压缩的一次性保护，多个 goroutine 读取时只初始化一次。"
    if name.endswith("rawDescData"):
        return "按需生成的 gzip 描述符缓存，后续读取复用相同数据。"
    if name.endswith("rawDesc"):
        return "原始 protobuf 文件描述字节，包含字段编号和服务定义，不能按业务数据修改。"
    if name.endswith("msgTypes"):
        return "消息反射类型表，按生成器编号关联 Go 消息与 protobuf 描述。"
    if name.endswith("goTypes"):
        return "Go 类型映射表，包含消息和 map entry，占位索引须与依赖表一致。"
    if name.endswith("depIdxs"):
        return "字段与服务的依赖索引表，初始化时连接输入、输出和嵌套消息类型。"
    if name.startswith("File_"):
        return "完成初始化的文件反射描述，服务绑定通过它解析准确消息类型。"
    if name == "_":
        return "编译期版本兼容断言，生成代码与 protobuf / gRPC 运行时不匹配时直接构建失败。"
    return "生成的协议声明，其值和顺序必须与 .proto 的字段及服务定义一致。"


def function(name: str, line: str) -> str:
    """解释生成的方法角色；getter 的零值回退与 handler 分派分开描述。"""
    # 例如 GetCount 对 nil 接收者返回 0，不会触发空指针解引用。
    if name.startswith("Get"):
        return "读取 " + name[3:] + "；nil 接收者返回该字段零值，不创建业务结果。"
    if name == "Reset":
        return "清空消息字段并重新关联运行时描述，已保存业务值不再保留。"
    if name == "String":
        return "生成 protobuf 文本表示，仅用于诊断，不作为任务传输编码。"
    if name == "ProtoReflect":
        return "返回消息反射视图，首次读取缓存描述后复用；nil 消息返回类型级反射视图。"
    if name == "ProtoMessage":
        return "实现 protobuf 消息标记接口，方法本身不执行业务逻辑。"
    if name == "Descriptor":
        return "返回压缩文件描述及消息索引；业务新代码使用 ProtoReflect 获取描述。"
    if name.endswith("rawDescGZIP"):
        return "在 sync.Once 中压缩并缓存文件描述，重复调用不会重复压缩。"
    if name == "init" or name.endswith("_init"):
        return "初始化文件类型描述和依赖关联；已初始化时立即返回，构建后释放临时类型索引。"
    if name.startswith("New") and name.endswith("Client"):
        return "创建生成客户端视图，借用传入的调用连接，不负责关闭该连接。"
    if name.startswith("Register"):
        return "检查默认实现的值嵌入后注册服务，Worker 与网络 Server 均接受此标准注册入口。"
    if name.startswith("mustEmbed") or name == "testEmbeddedByValue":
        return "生成器的服务兼容性检查，要求默认实现按值嵌入，避免 nil 嵌入指针导致运行时 panic。"
    if name.endswith("_Handler"):
        return "解码请求并通过可选拦截器调用注册的业务方法，保持标准 unary 或流 handler 约定。"
    if "Unimplemented" in line:
        return "默认未实现方法，返回 Unimplemented 状态，业务需覆盖此方法才能成功执行。"
    return "调用对应的生成 RPC 接口，unary 返回业务响应，流方法返回方向匹配的标准流对象。"


def annotate(path: pathlib.Path) -> None:
    """按稳定行规则补充说明；只插入注释，随后交给 gofmt 统一排版。"""
    # 同一工具链的原始输出始终生成相同注释，保证生成门禁可以逐字节比较。
    lines = path.read_text().splitlines()
    result = []
    for line in lines:
        indent = line[: len(line) - len(line.lstrip())]
        text = line.strip()
        comment = None
        match = re.match(r"func (?:\([^)]*\) )?(\w+)\(", text)
        if match:
            name = match[1]
            comment = name + " " + function(name, text)
        match = re.match(r"type (\w+)", text)
        if match:
            comment = match[1] + " 生成的消息、调用接口或服务适配类型；业务语义以 .proto 的中文说明为准。"
        match = re.match(r"(?:var |const )?(\w+)\s*(?:=|(?:\[\]|sync\.|protoreflect\.))", text)
        if match and (text.startswith(("var ", "const ")) or "rawDesc" in match[1] or "FullMethodName" in match[1] or match[1] == "_"):
            comment = match[1] + " " + declaration(match[1])
        match = re.match(r"(\w+)\s+(?:protoimpl\.|grpc\.ClientConnInterface)", text)
        if match and match[1] in RUNTIME_FIELDS:
            comment = match[1] + " " + RUNTIME_FIELDS[match[1]]
        if " := " in text:
            name, value = text.split(" := ", 1)
            meanings = {
                "mi": "当前消息的运行时类型描述，索引与 .proto 消息顺序对应。",
                "ms": "当前接收者的消息状态缓存，存入描述后反射读取复用。",
                "cOpts": "复制调用选项并附加静态方法标记，不修改调用方传入的选项切片。",
                "out": "待填充的业务响应或文件描述构建结果，成功后才返回调用方。",
                "in": "准确请求类型的解码目标，避免将错误消息类型传入业务方法。",
                "info": "拦截器所需的方法身份和服务实例，FullMethod 使用完整路径。",
                "handler": "把通用请求参数转换为准确 protobuf 类型后调用业务方法的闭包。",
                "err": "当前调用或解码错误，失败时结束当前请求，不返回未填充的响应。",
                "stream": "底层调用连接创建的流，方向由生成的服务描述决定。",
                "x": "带类型参数的标准 gRPC 流视图，委托底层 ClientStream。",
            }
            # if 头部声明由整个条件块说明，不插入中断语句的行注释。
            if re.fullmatch(r"\w+(?:, \w+)*", name):
                comment = name + " " + meanings.get(name.split(", ")[0], "当前生成步骤使用的局部值，后续适配逻辑通过它传递请求或结果。")
        # 私有兼容性接口也需要说明；if 内的匿名接口由整个注册检查块解释。
        if text.startswith(("mustEmbedUnimplemented", "testEmbeddedByValue")):
            comment = "私有兼容性标记，保证默认服务实现按值嵌入且可安全调用。"
        if text.startswith("if t, ok := srv."):
            comment = "注册时检查默认实现的值嵌入，提前发现 nil 指针嵌入，避免首次请求才 panic。"
        if text.startswith("if err :="):
            comment = "err 保存请求解码或流消息发送的错误，失败时直接结束调用，不把未完成的请求交给业务。"
        if text == "if x != nil {":
            comment = "非 nil 接收者读取实例状态；nil 路径返回字段零值或类型级描述，避免空指针访问。"
        if text == "if ms.LoadMessageInfo() == nil {":
            comment = "消息状态尚未关联描述时填入当前类型信息，后续反射读取复用此缓存。"
        if text.startswith("if File_"):
            comment = "文件描述已经完成初始化时直接返回，避免重复构建类型关联。"
        if text == "if interceptor == nil {":
            comment = "没有 unary 拦截器时直接调用业务方法；存在拦截器时走下方统一分派。"
        if comment:
            result.append(indent + "// " + comment)
        result.append(line)
    path.write_text("\n".join(result) + "\n")


# 参数是生成输出目录中的文件列表；路径来自 generate.sh，不依赖当前工作目录。
if __name__ == "__main__":
    for argument in sys.argv[1:]:
        annotate(pathlib.Path(argument))
