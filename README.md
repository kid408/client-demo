# client-demo

`client-demo` 是一个 gRPC 客户端模拟器。

它的职责不是提供业务接口，而是：

1. 发现 `gateway-grpc`
2. 建立长连接 gRPC stream
3. 周期性发送 `login -> heartbeat -> logout`
4. 输出 Prometheus 指标和结构化日志

## 关键环境变量

- `CONSUL_HTTP_ADDR`
- `TARGET_DISCOVERY_SERVICE_NAME`
- `GATEWAY_ADDR`
- `VIRTUAL_CLIENTS`
- `HEARTBEAT_INTERVAL_MS`
- `SESSION_GAP_MS`

## 默认端口

- HTTP：`18082`
- Metrics：`12114`

## 本地运行

```powershell
go mod tidy
$env:CONSUL_HTTP_ADDR="http://127.0.0.1:8500"
$env:TARGET_DISCOVERY_SERVICE_NAME="gateway-grpc"
$env:VIRTUAL_CLIENTS="3"
go run .
```
