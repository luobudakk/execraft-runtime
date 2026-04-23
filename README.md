# execraft-runtime

`execraft-runtime` 是 Execraft 生态中的数据面执行运行时，基于 Rust 实现。
它提供面向生产环境的 HTTP API 与 CLI，用于任务提交、调度执行、状态查询与持久化管理。

## 项目定位

- 为控制面提供统一的执行后端能力
- 提供异步任务处理能力（队列、并发、超时、重试）
- 提供可观测与可运维能力（健康检查、就绪探针、指标接口）
- 支持 Docker 优先部署，便于快速落地与集成

## 核心能力

- **HTTP API**
  - 提交任务
  - 查询任务状态
  - 取消任务
  - 获取任务事件
  - 查询 runtime 信息 / capabilities / config / resources
  - 健康检查、就绪检查、Prometheus 指标
- **CLI**
  - `serve` / `submit` / `status` / `wait` / `kill` / `run`
- **持久化**
  - SQLite 元数据存储
  - 任务产物落盘到 `tasks/<task_id>/`
- **执行模型**
  - 基于 internal shim 子进程执行任务
  - 支持超时控制与取消
- **能力协商**
  - 启动阶段探测宿主能力
  - 提供 requested/effective execution plan 可见性

## 目录结构

```text
execraft-runtime/
├── src/                # runtime 核心模块
├── tests/              # e2e 与行为测试
├── docs/               # 架构、API、CLI、部署文档
├── scripts/            # quickstart 与开发脚本
├── Dockerfile
└── Cargo.toml
```

## 快速开始

### 方式一：Docker（推荐）

```bash
docker build -t execraft-runtime:local .
docker run --rm -p 8080:8080 -v execraft-data:/data execraft-runtime:local
```

默认配置：
- 监听地址：`0.0.0.0:8080`
- 数据目录：`/data`

### 方式二：本地 Rust 运行

```bash
cargo build --release
cargo run -- serve --listen-addr 127.0.0.1:8080 --data-dir ./data
```

## API 最小示例

提交任务：

```bash
curl -sS -X POST "http://127.0.0.1:8080/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -d '{"execution":{"kind":"command","program":"/bin/sh","args":["-c","echo hello"]}}'
```

查询任务状态：

```bash
curl -sS "http://127.0.0.1:8080/api/v1/tasks/<task_id>"
```

## 开发与测试

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## 文档索引

- `docs/README.md`
- `docs/architecture.md`
- `docs/api.md`
- `docs/cli.md`
- `docs/deployment.md`
- `docs/development.md`

## 许可证

MIT License，详见 `LICENSE`。