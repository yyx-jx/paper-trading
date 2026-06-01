# 双 GitHub 仓库同步 Runbook

当前源码仓库固定为 `D:\P_T`。

远端约定：

- `origin` -> `git@github.com:yyx-jx/paper-trading.git`
- `mirror` -> `git@github.com:zephyrxu2024-hyper/hyper-terminal.git`

分支映射：

- 本地 `deploy` 推到 `origin/deploy`
- 本地 `deploy` 同步推到 `mirror/main`

## 一次性初始化

```powershell
git remote add mirror git@github.com:zephyrxu2024-hyper/hyper-terminal.git
git remote -v
```

如果 `mirror` 已存在，只需要确认地址正确：

```powershell
git remote set-url mirror git@github.com:zephyrxu2024-hyper/hyper-terminal.git
```

## 每次更新后的固定流程

先在仓库根目录检查工作区：

```powershell
git status --short
```

然后运行双推脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\push-both.ps1 -Message "你的提交信息"
```

如果这次需要带一个新的源码文件，再显式指定：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\push-both.ps1 -Message "你的提交信息" -IncludePath "apps/client/src/new-file.tsx"
```

## 保护规则

脚本默认只提交：

- 已跟踪文件的修改
- 你通过 `-IncludePath` 明确指定的新源码文件

脚本会阻止这些典型本地产物参与提交流程：

- `deploy/*.json`
- `deploy/*.zip`
- `deploy/windows-test-*`
- `remote_few_shot_work/`

如果脚本报 blocked path，先清理或移动这些文件，再提交。

## 镜像仓库说明

`mirror/main` 不是和当前仓库共用 Git 历史，所以同步时需要覆盖它的分支指针。
脚本内部使用的是：

```powershell
git push --force-with-lease mirror deploy:main
```

这样可以把 `D:\P_T` 的 `deploy` 当成第二仓库的来源分支持续同步，同时避免使用更冒进的裸 `--force`。
