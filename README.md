# Parquet Viewer（纯前端 / GitHub Pages）

这是一个纯前端 Parquet 文件查看器：文件只在浏览器本地读取与解析，不会上传到 GitHub。

## 本地运行

```bash
npm install
npm run dev
```

## 构建

```bash
npm run build
npm run preview
```

## 部署到 GitHub Pages

- 已包含 GitHub Actions 工作流：[.github/workflows/deploy.yml](.github/workflows/deploy.yml)
- 推送到 `main` 分支会自动构建并发布到 Pages
- 在 GitHub 仓库设置里启用 Pages：`Settings` → `Pages` → `Build and deployment` 选择 `GitHub Actions`

## 说明

- 支持本地选择/拖拽 `.parquet` 文件
- 可选：输入远程 URL（需要 CORS 且通常需要支持 HTTP Range）
- 默认只勾选前 30 列以避免渲染过重，可在左侧全选/搜索列名
