---
paths:
  - "internal/api/*.go"
---

修改任何相关的 `*Handlers` 方法后，必须在仓库根目录执行：

```bash
go run github.com/swaggo/swag/cmd/swag@v1.16.4 init -g main.go -d cmd/server,internal/api -o docs --parseInternal
```
