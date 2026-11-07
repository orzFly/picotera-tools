# PicoTera 项目识别：待改动

PicoTera 用正则从请求体里抓工作目录，按祖先路径匹配用户的项目，匹配不上且开了 `project.autoCreate` 就自动建项目（`pkg/server/project_extractor.go`）。这里记的是实际运行中踩到的问题和建议的四项改动；代码位置按 picotera `059b181`，示例路径都是虚构的。`ptt` 的项目解析（`internal/axonimport/rules.go`、`project.go`）就是按这四项实现的，可作参考。

## 是怎么出问题的

| 阶段 | 发生了什么 |
|---|---|
| 起因 | 有人在家目录 `~` 里 resume 了一个老会话。工作目录 `/home/alice` 匹配不到任何项目，autoCreate 就拿它当种子，建出一个路径为 `["/home/alice"]` 的项目 |
| 扩散 | 从此家目录下的一切都按祖先路径命中这个项目，autoCreate 再也不触发；几天内十万多条属于各个仓库的请求落进同一个项目 |
| 处理 | 把这个项目的路径改成一个不会出现的目录，新请求立刻按 worktree 路径各建一个碎项目，其中一个名字还带着 `)` |
| 修复 | 从 request artifact 重新解析工作目录，把请求挪回各自的项目，删掉空项目 |

## 改动一览

| # | 改动 | 解决什么 |
|---|---|---|
| 1 | 路径黑名单 | 家目录这类浅路径成为种子、再按祖先吞掉所有子目录 |
| 2 | 前缀别名 | 同一仓库在 Linux / macOS / Windows 上路径不同，每个项目要手工挂多条路径 |
| 3 | 识别 `.claude/worktrees` | 每个 worktree 被建成一个独立项目 |
| 4 | 修 `(was …)` | 正则把 `(was <旧目录>)` 吞进路径；多个候选里挑错了「当前目录」 |

## 1. 路径黑名单

| 项 | 内容 |
|---|---|
| 现状 | `autoCreateMinPathLen = 5` 只挡掉 `/`、`/tmp` 这种极短路径；`/home/alice` 有 11 个字符，照样能当种子 |
| 配置 | 用户设置 `project.blacklist`，与 `project.autoCreate` 同放 `user_setting`，值是路径数组，如 `["/home/alice", "/Users/alice", "C:\\Users\\alice"]` |
| 语义一：不当种子 | 候选路径命中黑名单（精确相等）时，不能拿它 autoCreate |
| 语义二：不当祖先 | 项目路径命中黑名单时，只允许**精确**匹配，不参与祖先匹配。这样「真在 `~` 里开的会话」若有人手工建了家目录项目仍能归进去，子目录不会被吞 |
| 默认值 | 可考虑内置一条：路径层级 ≤ 2（`/home/<名>`、`/Users/<名>`、`C:\Users\<名>`）一律视同黑名单。注意 `/tmp` 也只有一层，有人会故意挂 `/tmp` 当杂项兜底，规则不要误伤 |
| 改哪 | `maybeAutoCreate` 过滤种子；`MatchProjectByPaths` 的 SQL 改成「`path = 候选` 且（候选是请求里的原路径 或 path 不在黑名单）」，或在 Go 侧过滤后再查 |

## 2. 前缀别名

| 项 | 内容 |
|---|---|
| 现状 | 跨平台的仓库要手工给项目挂 `/Users/alice/src/foo`、`/home/alice/src/foo`、`D:\Alice\src\foo` 好几条路径；每个仓库都要挂一遍 |
| 配置 | 用户设置 `project.pathAliases`，值是 `[{"from": "D:\\Alice\\src", "to": "/home/alice/src"}, {"from": "/Users/alice/src", "to": "/home/alice/src"}]`，`to` 是规范形式 |
| 规范化 | 抽出候选路径后、展开祖先前，按最长 `from` 前缀替换成 `to`；`from` 用 `\` 分隔时，剩余部分的 `\` 一并翻译成 `/`（`D:\Alice\src\foo\cmd` → `/home/alice/src/foo/cmd`） |
| Windows 细节 | 盘符与路径按 Windows 规则不区分大小写比较前缀；尾部分隔符忽略 |
| 项目路径 | 规范化之后，项目只需挂规范形式一条；已有的多条路径不必删，照常生效 |
| autoCreate | 种子用规范化后的路径，否则 Windows 上会建出与 Linux 同名的第二个项目 |
| 改哪 | `extractProjectCandidates` 之后加一步 `normalizeProjectPath`；`Extract` 与 `maybeAutoCreate` 都用规范化结果 |

## 3. 识别 `.claude/worktrees`

| 项 | 内容 |
|---|---|
| 现状 | Claude Code 的 worktree 在 `<仓库>/.claude/worktrees/<名>`。仓库还没有项目时，第一条 worktree 请求就拿 worktree 路径当种子，建出以 worktree 命名的项目；之后每个新 worktree 再建一个 |
| 识别 | 候选路径里出现 `/.claude/worktrees/<名>`（或 `\.claude\worktrees\<名>`）时，截到 `/.claude` 之前得到仓库根 |
| 匹配 | 仓库根作为额外候选加入；原 worktree 路径保留（长度更长，若有人专门为某个 worktree 建了项目仍优先） |
| autoCreate | 种子改用仓库根，项目名取仓库名 |
| 可扩展 | 做成可配的模式表，日后加 `.worktrees/<名>`、`<仓库>-wt/<名>` 这类约定；在那之前可用改动 2 的别名或手工给项目挂路径 |
| 改哪 | `extractProjectCandidates` 产出候选时追加仓库根；`maybeAutoCreate` 选种子时先映射 |

## 4. 修 `(was …)`

Claude Code 在会话中途进出 worktree 时插入一段环境更新，同一请求体里可以出现多次：

```
# Environment update
 - Primary working directory: /home/alice/src/foo/.claude/worktrees/wt-1 (was /home/alice/src/foo)
 ...
# Environment update
 - Primary working directory: /home/alice/src/foo (was /home/alice/src/foo/.claude/worktrees/wt-1)
```

| 项 | 内容 |
|---|---|
| bug 一：吞后缀 | `Primary working directory: (.*?)(?:\\n\|\n\|$\|")` 抓到行尾，路径成了 `…/wt-1 (was /home/alice/src/foo)`，项目名成了 `foo)` |
| bug 二：挑错种子 | 所有命中去重后，`maybeAutoCreate` 取**最长**的一个当种子；带 `(was …)` 的恰好最长。正确的是取**最后一次出现**的新目录，那才是请求发出时的当前目录 |
| 正则 | 捕获组改成不贪婪地停在 ` (was ` 之前，另起一个可选组捕获旧目录：`Primary working directory: (.*?)(?: \(was (.*?)\))?(?:\\n\|\n\|$\|")` |
| 候选顺序 | 按在请求体中出现的位置排序，最后一个新目录记为「当前目录」；旧目录只作低优先级候选，不当种子 |
| 匹配 | 当前目录优先：先只用当前目录（含祖先）匹配，没命中再用其余候选；现在是所有候选一起比最长，一个历史目录可能盖过当前目录 |
| 测试样本 | 上面两段；只有 worktree 一次进出、多次进出、进出后回到仓库根三种 |

## 修完之后的数据修复

| 步骤 | 做法 |
|---|---|
| 找出受害项目 | 名字含 `)`、路径含 ` (was `、路径在 `.claude/worktrees/` 下、路径在黑名单里的项目 |
| 重分类 | `ptt reproject --project-id <id>`：从 request artifact 重新抽路径，先 dry-run 看去向，再 `--write`；它会一并挪 upstream 行、重算项目首末出现时间、刷新聚合 |
| 合并碎片 | `ptt merge-projects`：把 worktree 与别的机器上建出的碎项目并回仓库项目 |
| 追踪 | 追踪的项目是查询时取最新一条有项目的 meta 行，不用单独改 |
