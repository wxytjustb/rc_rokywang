# Agent 规则

- 开始处理本目录及其子目录中的任务前，必须读取并遵守当前目录的 `CLAUDE.md`。
- 开始任务前，还必须递归检查 `.claude/rules/` 下的 Markdown 文件，并读取与当前任务相关的规则。
- 如果 `.claude/rules/` 中的规则声明了路径适用范围，只在当前处理的文件符合该范围时应用。
- `CLAUDE.md`、`.claude/rules/` 与本文件发生冲突时，必须停止执行并向用户说明冲突，不得自行选择其中一套规则。
- `CLAUDE.md` 是本项目通用规则的维护入口；不要在本文件中复制其中的规则。仅适用于特定路径的规则维护在 `.claude/rules/` 中。
- 维护 `CLAUDE.md` 时必须保持内容精炼，并将总行数严格控制在 200 行以内。Anthropic 官方建议每个 `CLAUDE.md` 以少于 200 行为目标；这是一项维护上限，而不是文件加载器的硬性截断限制。
- 如果规则增长到接近上限，应删除过时或可从代码直接推断的内容，并将仅适用于特定路径的规则拆分到 `.claude/rules/` 中。

参考：[Anthropic Claude Code 官方文档：How Claude remembers your project](https://code.claude.com/docs/en/memory)
