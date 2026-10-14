# Go 索引数据集：双布局存储与有界内存读取实现计划

> 编写日期：2026-10-14。
>
> 本文用于后续创建独立项目。本轮只保存设计与实施计划，不实现代码，不修改 Hatchet 或 wego。
>
> 项目名称、Go module 路径和正式文件扩展名在新项目建立时确定；本文使用 `dataset` 作为包名示意。接口是拟定契约，不代表已有实现。

## 1. 背景与目标

大 payload 由大量记录组成，整体下载、整体 JSON 解码或收集全部行容易导致 OOM。目标是将记录存储到本地文件或 S3／MinIO，通过索引定位并按需读取，使 SDK 自有内存不随数据集总行数线性增长。

使用方在写入时选择行式或列式分块，读取方式保持一致：

```go
for row, err := range data.Slice(10, 100) {
    if err != nil {
        return err
    }
    fmt.Println(row["field"])
    for key, value := range row {
        // 字段遍历采用普通 Go map 语义，顺序不固定。
        _ = key
        _ = value
    }
}
```

确定的要求：

- Go 1.23 或更新版本，使用函数迭代器。
- 写入时选择 `RowLayout` 或 `ColumnLayout`；读取时从 manifest 自动识别。
- 同一快照只使用一种布局，两种布局共享行、字段、数值和范围语义。
- `Since(start)`、`End(end)`、`Slice(start, end)` 与字段选择。
- 行以 `map[string]any` 交付，支持直接字段访问和字段遍历。
- 行号从 0 开始，范围使用左闭右开区间。
- 索引分页、S3 Range 读取、独立块压缩、统一内存预算。
- 读取错误、取消、损坏和资源超限明确返回，不能伪装为成功 EOF。
- 本地与 S3／MinIO 使用相同格式和一致性测试。

首版不实现 SQL、过滤查询、向量搜索、全文搜索、更新／删除行、多人事务写入、在线尾读或 Lance 文件兼容性。借鉴其存储思想，不复制完整 Lance 系统。

## 2. 技术判断与适用边界

### 2.1 为什么提供两种布局

| 场景 | 行式分块 | 列式分块 |
|---|---|---|
| 读取整行 | 相关数据集中，通常请求更少 | 需要取多个列页并组装 |
| 读取少数字段 | 仍要读取对应行的数据块 | 可以跳过未选择的列 |
| 随机读取单行 | 定位行块，解码块内记录 | 定位各选中字段的页 |
| 动态、稀疏字段 | 自然表达 | 需要字段目录与存在性标记 |
| 压缩 | 不同字段混合 | 同类字段集中，通常更利于压缩 |
| 写入复杂度 | 较低 | 较高，需要管理列缓冲 |

默认选择 `RowLayout`。不预先宣称某种布局更快；最终通过相同数据、相同预算和相同投影条件比较。

### 2.2 内存保证的准确含义

保证 SDK 不完整加载数据、索引或历史行，并限制自有缓存、下载、解压、预取和当前记录处理。保证建立在字段数、嵌套深度、单行大小、数据页大小等格式与配置约束上。

不能承诺任意业务对象、任意恶意文件或任意 Go 进程在固定 RSS 下运行：

- `row["field"]` 要求选中字段已经物化；单个值无限大时不可能满足有限内存。
- 调用方保存返回的行或并发创建许多独立 Dataset，会增加调用方持有的内存。
- Go 对象、map、解码器和 GC 有额外开销，编码字节数不能直接当成堆大小。
- AWS SDK、TLS 和运行时开销需要在整体内存实测中计入，不能仅统计缓存。

格式支持并强制执行大小约束。超过预算明确报错，不扩大预算后继续读取。首版通过测量验证解码对象放大比例，设置保守限制。

## 3. 公开 API

### 3.1 行、布局与写入

```go
type Row map[string]any

type Layout string

const (
    RowLayout    Layout = "row"
    ColumnLayout Layout = "column"
)

func NewWriter(ctx context.Context, store Store, path string,
    opts ...WriterOption) (*Writer, error)

func WithLayout(layout Layout) WriterOption
func WithWriterMemoryBudget(bytes int64) WriterOption
func WithPageBytes(bytes int) WriterOption
func WithCompression(compression Compression) WriterOption
func WithLimits(limits Limits) WriterOption

func (w *Writer) Append(ctx context.Context, row Row) error
func (w *Writer) Commit(ctx context.Context) (*Snapshot, error)
func (w *Writer) Abort(ctx context.Context) error
func (w *Writer) Close() error
```

生命周期规则：

- Append 在返回前完成数据验证与快照；之后修改输入 map 不改变已接受的记录。
- 首版 Writer 不允许并发 Append；检测重入并明确报错，调用方自行串行化。
- Commit 完成数据与索引对象后发布 manifest，是唯一提交成功入口。
- Close 释放本地资源，不隐式发布。应用明确处理 Commit 错误。
- 重复成功 Commit 返回同一快照；提交后 Append 拒绝执行。
- 对状态不明确的 Commit，核实本次随机快照 ID 与 manifest 是否已发布；不能生成新身份盲目重试。
- Abort 只处理本 Writer 创建且能证明未发布的对象；无法确认提交状态时禁止删除可能被已发布 manifest 引用的对象。

### 3.2 打开与查询

```go
func Open(ctx context.Context, store Store, path string,
    opts ...ReaderOption) (*Dataset, error)

func WithMemoryBudget(bytes int64) ReaderOption
func WithPrefetch(pages int) ReaderOption
func WithReaderLimits(limits Limits) ReaderOption

func (d *Dataset) NumRows() uint64
func (d *Dataset) Select(fields ...string) Query
func (d *Dataset) Since(start int64) Rows
func (d *Dataset) End(end int64) Rows
func (d *Dataset) Slice(start, end int64) Rows
func (d *Dataset) Rows() Rows
func (d *Dataset) Close() error

func (q Query) Since(start int64) Rows
func (q Query) End(end int64) Rows
func (q Query) Slice(start, end int64) Rows
func (q Query) WithContext(ctx context.Context) Query

// 函数迭代器支持 range，并显式交付读取错误。
type Rows func(yield func(Row, error) bool)
```

范围和字段派生操作在 Dataset／Query 上完成，最终返回 Rows 迭代器。Query 保存不可变查询描述，迭代器闭包持有它，不维护全局身份注册表，不通过读取前面的行实现跳过。

完整遍历使用 `range data.Rows()`。如果希望写成 `range s3`，可以先令 `s3 := data.Rows()`；这里 s3 是迭代器，不是 AWS 客户端。范围选择使用 `data.Since`、`data.End` 或 `data.Slice`，两种布局一致。

使用示例：

```go
data, err := dataset.Open(ctx, store, "datasets/assets",
    dataset.WithMemoryBudget(32<<20),
    dataset.WithPrefetch(2),
)
if err != nil {
    return err
}
defer data.Close()

for row, err := range data.Select("name", "address").Slice(10, 100) {
    if err != nil {
        return err
    }
    fmt.Println(row["name"])
}
```

统一语义：

- `Slice(10, 100)` 交付行 10 至 99；end 超过总行数时截到数据末尾。
- start 超过总行数或 start 等于 end 时返回空序列。
- 负数及 end 小于 start 返回参数错误，不 panic，不发生存储 I/O。
- Select 无参数表示全部字段；重复字段稳定去重；未知字段明确报错。
- 选中字段在某行不存在时不放入 map；显式 null 以存在的键和 nil 值表示。
- 单次迭代遇到错误交付一次 `(nil, err)` 并结束；禁止忽略错误的示例。
- `for row := range rows` 在 Go 中可以忽略第二个值，但文档和示例统一使用 `row, err`，不宣称该写法能自动传播错误。
- 每次迭代持有独立游标，可重复读取；Dataset 可以并发查询，所有查询共享其预算。
- break、return、panic 离开迭代以及 context 取消都必须收尾当前迭代，取消预取、关闭响应 body、释放预算。
- Dataset 的打开 context 作为生命周期父 context；增加 `Query.WithContext(ctx)` 支持单次读取更短的 deadline。
- 返回的 Row 及嵌套值不借用会被下一次读取覆盖的页缓冲，调用方可以持有；持有成本由调用方承担。

### 3.3 超大字段

首版限制物化行和字段大小，不把无限大的 Blob 放入 Row。后续可增加显式 BlobRef 与 OpenField／Reader 接口；其流式契约和行式／列式布局分别验证后再交付。

不得为了首版 API 简洁而让 map 下标访问执行隐藏网络 I/O，也不得用字段引用悄悄替换普通字段值。

## 4. 数据语义与序列化

首版只支持 JSON 数据模型：字符串、布尔、null、有限数值、数组和对象。Row 根值必须为对象，字段名为合法 UTF-8。

- 普通整数和小数存储为精确 JSON 数字文本；动态读取统一使用 `json.Number`，两种布局完全一致。
- 拒绝 NaN、Infinity、循环引用、过深嵌套、过多字段和不支持的值类型。
- 明确字节数组不是首版独立二进制类型；业务需要时显式转成字符串或使用后续 Blob 能力。
- 不隐式调用未知对象的 MarshalJSON／String 方法绕过限制；P0 明确支持类型白名单及输入快照策略。
- 同一字段可以缺失或为 null；首版不要求业务预先声明 schema，也不按首行推断固定类型。
- 数据集级字段目录只记录字段 ID 与名称；列值保留 JSON 类型，可以存在异构值。
- 嵌套对象作为一个字段值，首版不对嵌套路径独立建列。

这是便于动态 map 使用的基础编码。整数列、字典、位打包等专用列编码属于后续优化，不能混入首版导致读写规则不稳定。

## 5. 文件组织与分页索引

### 5.1 快照布局

```text
datasets/<name>/
  manifest.json
  snapshots/<random-snapshot-id>/
    directory.idx
    fields.idx
    segments/000001.data
    segments/000001.index
    segments/000002.data
    segments/000002.index
```

- manifest 为小型固定上限对象，包含 magic、版本、布局、总行数、根目录引用和校验信息，不列出无限多的 segment。
- directory.idx 是分页目录，按行范围定位 segment；目录较大时增加树层级，根地址写入 manifest。
- fields.idx 保存分页字段目录；允许有界构建名称缓存，不无限加载全部名称。
- segment 按编码字节或行数上限轮转，segment 索引也有大小限制。
- 数据、目录和索引对象不可变，读者固定 manifest 快照。对象身份包含版本 ID／ETag 等可验证信息，避免混读被覆盖的对象。
- 目录与页头中存储编码长度和解码长度，两者均受读端本地限制；不能信任文件声明后直接分配。

字段总数设定硬上限，因此 Writer 可以有界保存字段 ID 注册表；文件数量和总行数不设全量内存数组。更高字段规模通过后续外存字段目录扩展。

### 5.2 行式布局

每页保存多条完整记录：

```text
页头
行偏移表：N+1 个偏移
JSON 记录字节区
```

索引按起始行排序，记录页 offset、长度和行数。读取目标行先定位页，再根据页内行偏移取记录。

- 页整体独立压缩，读取某行可能需要解压该页。
- Select 在行页内完成投影，不承诺减少行页下载字节。
- 行大小超过目标页大小但未超过单行上限时，使用单行大页；仍受全局最大页与预算约束，不能隐式突破限制。

### 5.3 列式布局

各字段独立写页，页可交错追加在同一个 segment 数据对象中：

```text
field=name，行 0–99
field=address，行 0–49
field=name，行 100–199
field=address，行 50–119
```

页内容包含：

- 字段 ID、起始行与行数。
- 存在性标记，区分缺失和 null。
- 变长值偏移表与 JSON 值字节区。
- 必要编码信息和校验。

索引按字段 ID 与起始行组织。字段之间页边界可以不同，读取时按逻辑行号对齐，不建立 Parquet 式共同 row group。

不能为新出现的字段回填逐行 null：未覆盖的行区间表示缺失；稀疏页中的存在性标记处理区间内部缺失。写入器用有界状态跟踪行位置，不建立与总行数等长的 bitmap。

### 5.4 索引设计

- 固定宽度索引项、固定目标页大小，索引页带类型、条目数量与校验。
- 行式键为起始行；列式键为 `(field_id, start_row)`。
- 多级分页目录支持范围定位，扫描复杂度依赖树高与访问页数，不依赖全部历史行数。
- 页位置必须落在目标对象范围内；所有 offset、length、计数运算检查溢出。
- 索引写入使用分层流式构建或本地临时文件排序，不积攒全量索引条目。
- 列页交错写入后，索引排序可使用磁盘外排序，或者每字段临时索引流后有界合并；P0 基准后选择一种，不同时实现多种排序框架。

## 6. Store 与 S3 接入

Store 只抽象数据格式必需能力：对象信息、带 context 的范围读取、流式对象写入、条件发布和删除自有临时对象。不要复制整个 AWS SDK，也不新增 RPC 服务。

```go
// 示意接口，P0 根据本地／S3实现确定参数与返回类型。
type Store interface {
    Stat(ctx context.Context, key string) (ObjectInfo, error)
    ReadRange(ctx context.Context, key string,
        offset, length int64, identity ObjectIdentity) (io.ReadCloser, error)
    Create(ctx context.Context, key string) (ObjectWriter, error)
    Publish(ctx context.Context, key string, manifest []byte,
        condition PublishCondition) (ObjectInfo, error)
    Delete(ctx context.Context, key string) error
}
```

实现本地文件和 AWS SDK for Go v2 的 S3 backend。MinIO 使用同一个 S3 backend，支持 endpoint、region、AK/SK 和 path-style 配置。

- Range 严格限制读取长度，关闭响应 body；验证响应与请求范围、对象身份匹配。
- 相邻范围可以合并，但合并后长度和额外读取量必须有上限。
- 相同对象范围的并发读取可合并，单个等待者取消不能错误取消其他仍在等待的调用。
- S3 重试只重试相同不可变对象和范围，采用有界重试与调用预算。
- Multipart 上传固定 part 大小和有限并发；S3 规定的最小 part 大小必须计入写预算。
- 不使用整对象下载缓存；本地 backend 不通过无界 mmap 绕过内存约束。

manifest 发布首版采用 create-only 条件，同一路径已存在则返回 AlreadyExists。S3 使用条件写入，本地使用独占创建或等价原子机制。验证当前 MinIO 实际支持；不支持时明确失败，不退化为检查后无条件覆盖。

新快照发布到新路径。覆盖、原子切换别名和版本保留策略留待后续。

## 7. 压缩、校验与扩展

- 首版支持无压缩与 zstd； gzip 可作为后续可选压缩组件。
- 压缩按页独立执行，范围定位基于压缩后的页 offset。
- 使用稳定编码／压缩 ID 和显式格式版本，读端不根据魔数猜测业务编码。
- 在下载、解压、解析期间执行上限检查，不能完整展开后才判断大小。
- 数据页和索引页有校验值；校验用于损坏检测，不作为来源认证。
- 同版本可扩展可忽略的元数据；必须理解的新增编码或布局明确拒绝，不能错误读取。
- 首版不实现加密；未来若增加加密，必须按独立页设计并明确索引泄露与认证范围。

## 8. 内存预算、背压与并发

### 8.1 拟定默认值

以下是初始实验值，P0／性能阶段根据对象放大和上传成本调整：

| 配置 | 拟定默认值 |
|---|---:|
| Reader 自有工作预算 | 32 MiB |
| Writer 自有工作预算 | 64 MiB |
| 目标解码页大小 | 256 KiB |
| 最大解码页大小 | 8 MiB |
| 单行序列化上限 | 4 MiB |
| 单字段序列化上限 | 4 MiB |
| 索引页大小 | 64 KiB |
| manifest 上限 | 64 KiB |
| 预取并发 | 2 |
| 字段总数上限 | 1024 |
| JSON 嵌套深度上限 | 64 |

允许关闭预取。预算无法容纳最小必需工作集时，启动／读取明确报 ResourceExhausted。配置校验先于存储操作。

### 8.2 预算算法

建立一个实例级预算管理器，按缓存、下载、解压、编码和当前行工作集登记。并发查询共用预算，不能每个查询重新创建 32 MiB 缓存。

- 发起读取和分配缓冲前申请预算。
- 缓存淘汰只回收没有活跃引用的页；仍被查询引用的页继续计入预算。
- 同一操作不能先持有部分预算、再无限等待不足的剩余预算；关键工作集联合预留，避免死锁。
- 所有等待支持 context 取消；迭代收尾释放持有额度。
- 解码后 Row 的所有权交给调用方，预算不宣称约束调用方长期持有的对象。
- 没有长期每列 goroutine 或每行 goroutine；下载和预取用固定并发。

列式全字段读取时，不能同时保留 1024 个最大解码页。采用有界行窗口，在预算内逐列填充当前窗口；如果最小单行也超过预算则明确拒绝。

### 8.3 关闭语义

- Dataset.Close 拒绝新查询，取消自有读取，等待可取消的 I/O 收尾，重复关闭安全。
- 查询取消只结束自身迭代，不关闭共享 Dataset，也不取消其他查询。
- 每个 Reader／Writer 独立拥有 context 和资源；不替换全局配置。
- 共享 Store 的 AWS client 不由 Dataset 隐式关闭；明确资源所有权。
- Writer 的取消、Abort、Commit 与 Close 状态机必须明确，禁止关闭期间发布半成品。

## 9. 目录与模块

新项目使用单 Go module，首版保持少量职责明确的包：

```text
dataset/                         项目根包：Dataset、Writer、Query、Rows、选项和错误
storage/                         Store 契约与本地实现
storage/s3/                      AWS SDK v2，S3／MinIO
internal/format/                 manifest、页、索引格式及版本校验
internal/layout/                 行式与列式读写；共用布局契约
internal/budget/                 预算、页缓存和背压
examples/                       行式、列式、字段选择、MinIO
tests/integration/               真实对象存储与故障验收
docs/                            格式、API、内存契约、性能和迁移说明
```

不要为每种页、每种压缩算法或每个小状态单独建包。纯算法与 I/O 分开，避免 layout → 公开根包 → layout 循环依赖。目录在实现中可收敛，不按表机械创建转发层。

必要中文注释解释索引边界、所有权、锁、预算申请顺序、取消和提交原子性；用具体行号和字节范围说明，不逐行翻译代码。

## 10. 实施阶段

| 阶段 | 实现内容 | 退出标准 |
|---|---|---|
| P0：契约与原型 | 固定 API、JSON 语义、文件格式、两种分页算法、预算模型 | 行／列原型语义一致；证实范围读取和预算可行 |
| P1：行式闭环 | 本地 Writer、Commit、分页索引、读取和迭代 | 范围、投影、null、精确数值和错误完整通过 |
| P2：对象存储 | AWS SDK v2、MinIO、Range、multipart、条件发布 | 不整文件下载；取消与发布不明确场景通过 |
| P3：列式闭环 | 独立列页、稀疏字段、索引排序、窗口组装 | 与行式相同输入产生相同逻辑结果 |
| P4：资源与故障 | 共享预算、缓存、预取、限额、并发关闭、fuzz | 大数据内存稳定；恶意长度不能触发无界分配 |
| P5：交付 | 性能比较、例子、格式文档、兼容 fixtures、CI | 完整门禁与真实 MinIO 验收通过 |

先有纵向闭环，再增加优化。不要在接口阶段实现空占位方法，或把“编译成功”当作格式和资源保证成立。

## 11. 测试与性能验收

### 11.1 正确性矩阵

| 分类 | 必测场景 |
|---|---|
| 范围 | 空数据、首尾边界、跨页／跨 segment、越界、负值、Since／End／Slice 一致性 |
| 字段 | 全部、子集、未知、重复、动态出现、稀疏、缺失与 null、嵌套值 |
| 数值 | int64／uint64 边界、高精度小数、json.Number、非法数值 |
| 语义一致 | 同样输入分别写行式／列式，所有查询结果完全一致 |
| 数据所有权 | 修改已 Append 输入、保留已返回 Row、重复迭代、并发查询 |
| 文件格式 | 未知版本、未知编码、损坏页、错误校验、截断、伪造长度、offset 溢出 |
| 索引 | 多级目录、索引分页、列页边界不同、字段目录超限、只访问相关索引页 |
| 压缩 | 无压缩、zstd、解压炸弹、页面上限、单行大页 |
| S3 | Range 验证、对象身份变化、响应截断、重试、取消、MinIO AK/SK 与 endpoint |
| 提交 | create-only 冲突、上传失败、manifest 响应丢失、Abort、临时对象清理 |
| 生命周期 | break、panic、deadline、Close 并发、慢消费、预算等待取消和无资源残留 |

性质测试以完整小数据作为参考，随机生成分页边界、字段稀疏性和范围，并对两种布局做对照。协议与索引解析 fuzz 必须包含真实损坏文件，不能只测人工构造的内部结构。

### 11.2 内存测试

在独立子进程或容器中限制进程内存，避免仅用 Go 堆指标掩盖额外内存：

- 数据集从 100 MiB 增长到 1 GiB、10 GiB，工作预算不变。
- 小行、多字段、深嵌套、接近上限的大行、稀疏列、低压缩比数据。
- 整行与少列读取、长范围、随机范围、并发查询和慢消费者。
- 统计 peak RSS、Go heap、分配量、预算高水位及归还情况。
- 明确报告调用方保留行的实验；不能把该增长归因于库缓存，也不能承诺替调用方释放。

内存通过标准：数据规模增加不导致 SDK 工作内存按总量线性增长；受限配置能明确拒绝无法容纳的操作；不得出现预算死锁或由元数据、列数量、解压产生的无界分配。

### 11.3 性能比较

对比行式、列式及 `parquet-go` 基线；所有实现必须使用相同数据、字段选择和存储条件。

场景覆盖：

- 全量整行扫描、10／100／1000 行范围。
- 随机单行与连续行。
- 选 1 列、10% 列、全部列。
- 10、100、1024 字段，以及不同缺失率。
- 无压缩和 zstd，不同页大小、不同缓存预算。
- 本地、MinIO 和存在真实网络延迟的 S3。

报告吞吐、首行延迟、p95 延迟、S3 请求次数、下载字节、索引字节、分配次数、峰值内存和存储大小。区分冷缓存与热缓存，不用请求合并或缓存配置差异制造性能优势。

### 11.4 门禁

```bash
go test ./...
go vet ./...
go test -race ./...
go test -tags=integration ./tests/integration/... -timeout 20m
```

使用锁定版本的 MinIO 与隔离 bucket／prefix；记录镜像 tag、digest 和 Go 依赖。只清理本次测试资源。生成格式 golden fixtures，版本更新必须验证历史稳定格式可读性。

## 12. 交付物与后续方向

首版交付：

1. 可独立使用的 Go module 和稳定公开接口。
2. 行式与列式读写、本地与 S3／MinIO、无压缩与 zstd。
3. 统一范围／投影 API、动态 Row 和精确数字语义。
4. 分页索引、统一工作预算、背压与取消。
5. 二进制格式规范、使用示例、错误与生命周期文档。
6. 单元、性质、fuzz、race、真实 MinIO 和内存验收报告。
7. 与 Parquet 的实测对比，不宣称未经验证的性能倍数。

后续独立评估：Blob 懒加载、字段专用编码、嵌套字段投影、过滤索引、快照别名、追加写入和崩溃恢复写入。每项增加前明确其一致性与资源成本。

**完成标准：相同数据用两种布局写入后，通过相同 API 得到一致结果；范围和字段选择使用索引而非全量扫描；SDK 工作内存与总数据量解耦；存储失败、取消和损坏明确可见；真实 S3 兼容存储验收、内存压力测试和全部质量门禁通过。**

## 13. 设计参考

- [Lance 文件格式](https://github.com/lance-format/lance/blob/main/docs/src/format/file/index.md)：独立列页、对象存储与随机访问。
- [Parquet 文件格式](https://parquet.apache.org/docs/file-format/) 与 [页索引](https://parquet.apache.org/docs/file-format/pageindex/)：元数据定位、列块与页级索引。
- [parquet-go/parquet-go](https://github.com/parquet-go/parquet-go)：Go 参考实现与性能基线。
- [airbusgeo/osio](https://github.com/airbusgeo/osio)：对象存储随机读取、分块缓存与并发请求合并参考。
- [S3 GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)：Range、对象身份与条件读取。

参考项目功能与代码需要在新项目开始时重新核验并固定版本；当前计划不依赖未发布补丁。

## 14. 项目命名建议

建议使用短小、全小写、容易拼写的单词作为仓库名和 Go 包名，避免连字符、下划线，以及同时出现品牌名与冗余的 `go` 前缀。不将名称绑定到 S3 或行式布局，因为本项目同时支持本地存储和两种布局。

| 候选 | 长度 | 寓意 | 使用形式 |
|---|---:|---|---|
| **pago** | 4 | 取 page 与 Go 的组合，突出分页存储与读取 | `pago.Open()`、`pago.NewWriter()` |
| **rivu** | 4 | 取 rivulet 的短形式，突出按需流式读取 | `rivu.Open()`、`rivu.NewWriter()` |
| **bloc** | 4 | block 的简短形式，突出独立数据块 | `bloc.Open()`、`bloc.NewWriter()` |

优先推荐 **pago**：与分页索引、有界数据页最贴合，读写两侧都适用，作为包限定符也较简洁。

拟定定位：

> pago：支持行式与列式布局、面向本地和对象存储的有界内存数据集库。

示意 API：

```go
writer, err := pago.NewWriter(ctx, store, path,
    pago.WithLayout(pago.ColumnLayout),
)

data, err := pago.Open(ctx, store, path,
    pago.WithMemoryBudget(32<<20),
)

for row, err := range data.Select("name").Slice(10, 100) {
    // 处理当前行和读取错误。
    _ = row
    _ = err
}
```

以上均为命名候选，尚未确认 GitHub 仓库、Go module、域名或商标的可用性；不表示用户已经选择最终名称。建立新项目时先核验，再固定 module 路径、包名和文件 magic。文档其他章节暂保留 `dataset` 示意包名，最终定名后统一替换。
