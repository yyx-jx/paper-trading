const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const outputDir = path.join(root, "部署", "windows-test");
const readmePath = path.join(outputDir, "测试说明.md");

function assertInsideRoot(targetPath) {
  const relative = path.relative(root, targetPath);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(`Refusing to touch path outside project root: ${targetPath}`);
  }
}

function runNodeScript(scriptPath, args) {
  execFileSync(process.execPath, [scriptPath, ...args], {
    cwd: root,
    stdio: "inherit",
    env: {
      ...process.env,
      CSC_IDENTITY_AUTO_DISCOVERY: "false",
      ELECTRON_BUILDER_DISABLE_PUBLISH: "true"
    }
  });
}

assertInsideRoot(outputDir);
fs.rmSync(outputDir, { recursive: true, force: true });
fs.mkdirSync(outputDir, { recursive: true });

const npmCli = process.env.npm_execpath;
if (!npmCli) {
  throw new Error("npm_execpath is not available; run this script through npm.");
}

runNodeScript(npmCli, ["run", "build"]);
runNodeScript(require.resolve("electron-builder/cli.js"), ["--win", "nsis", "--x64", "--publish", "never"]);

for (const entry of fs.readdirSync(outputDir)) {
  if (entry === path.basename(readmePath) || entry.toLowerCase().endsWith(".exe")) {
    continue;
  }
  fs.rmSync(path.join(outputDir, entry), { recursive: true, force: true });
}

fs.writeFileSync(
  readmePath,
  `\uFEFF# BTC Paper Trading Test 测试说明

## 安装和启动

1. 双击 \`BTC Paper Trading Test Setup.exe\`。
2. 按安装向导完成安装。
3. 从桌面或开始菜单启动 \`BTC Paper Trading Test\`。

## 测试账号

- 管理员：\`admin / admin123\`
- 测试员：\`tester / tester123\`
- 高级测试员：\`senior / senior123\`
- 测试工程师：\`engineer / engineer123\`

## 重要说明

- 这个安装包用于功能测试确认。
- 不需要安装 Node.js、npm、Docker、PostgreSQL 或 Redis。
- 安装包不会包含开发机上的 \`.env\`、数据库目录或日志目录。
- 测试版默认使用内存模式，重启后交易数据不保证保留。
- Chainlink 在测试版中默认关闭；行情功能依赖当前网络访问 Binance 和 Polymarket。
`,
  "utf8"
);

console.log(`\nWindows test installer is ready: ${outputDir}`);
