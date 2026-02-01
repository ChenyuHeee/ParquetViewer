# Parquet Viewer（纯前端 · GitHub Pages）

[![Deploy to GitHub Pages](https://github.com/ChenyuHeee/ParquetViewer/actions/workflows/deploy.yml/badge.svg)](https://github.com/ChenyuHeee/ParquetViewer/actions/workflows/deploy.yml)
[![GitHub Pages](https://img.shields.io/badge/GitHub%20Pages-live-brightgreen)](https://chenyuheee.github.io/ParquetViewer/)
[![Vite](https://img.shields.io/badge/Vite-5.x-646CFF?logo=vite&logoColor=white)](https://vitejs.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)

一个可以直接在浏览器里打开 Parquet 文件的轻量查看器：不需要服务器、不需要上传文件，适合快速检查数据、做字段筛选与抽样预览。

在线访问：
- GitHub Pages：`https://chenyuheee.github.io/ParquetViewer/`

---

## English (short)

A lightweight, serverless Parquet viewer that runs entirely in your browser.

- Open local `.parquet` files without uploading them anywhere
- Preview rows with pagination
- Select/search columns, export current page to CSV
- Optional: open remote Parquet URLs (requires CORS, usually HTTP Range support)

## 你能用它做什么

- 本地打开：选择或拖拽 `.parquet` 文件，在浏览器本地解析
- 抽样预览：按页读取（默认 100 行/页），支持上一页/下一页
- 列选择：默认勾选前 30 列（避免页面过重），支持搜索列名、全选/全不选
- 导出：一键导出“当前页 + 已选列”为 CSV
- 远程 URL（可选）：直接读取公网 Parquet 链接（需要 CORS，通常还需要支持 HTTP Range）

## 隐私与安全

- 本地文件不会上传到 GitHub，也不会发送到任何服务器
- 页面仅运行在你的浏览器中；如果你打开“远程 URL”，才会从该 URL 拉取数据

## 使用指南（面向普通用户）

### 1) 打开本地 Parquet

1. 进入页面
2. 点击“选择本地 .parquet”，或把文件拖进左侧的拖拽区域
3. 等待加载完成（会出现加载动画）
4. 在左侧勾选要看的列；右侧会按当前选择渲染表格

### 2) 查看更多行（分页）

- 右侧点“上一页 / 下一页”翻页
- 左侧可以调整“每页行数”（10～5000）

### 3) 导出当前页

- 点“导出当前页 CSV”即可下载

### 4) 打开远程 URL（可选）

在左侧输入 `https://.../file.parquet` 并点击“打开 URL”。

注意：这依赖目标站点允许跨域（CORS），并且很多情况下需要支持 Range 请求，否则可能无法读取或会很慢。

## 常见问题（FAQ）

### 为什么默认只选前 30 列？

Parquet 文件列多时（尤其是上百列），一次性渲染会明显变卡、并且表格很宽。默认限制能让你更快看到“可用预览”，再按需勾选更多列。

### 能打开超大文件吗？

可以尝试，但受浏览器内存与性能影响。建议：
- 先减少列、减少每页行数
- 只查看你关心的字段

### 为什么打开某些远程 URL 失败？

多半是 CORS 或 Range 支持问题。这不是本项目能绕过的限制；建议把文件下载到本地再打开。

## 开发者：本地开发

环境：Node.js 20+（建议）

```bash
npm install
npm run dev
```

## 部署到 GitHub Pages

本仓库已内置 GitHub Actions 自动部署。

1. 推送到 `main` 分支
2. 打开 GitHub 仓库设置：Settings → Pages
3. Build and deployment 选择 “GitHub Actions”
4. 等待 Actions 运行完成，即可通过 Pages 访问

## 技术栈

- 前端构建：Vite + TypeScript
- Parquet 解析：hyparquet（浏览器端）

## Roadmap（可能会做）

- 更快的列选择体验（例如：只选搜索结果、一键反选）
- 更丰富的数据类型展示（日期/嵌套结构更友好）
- 大文件优化（更细粒度的读取策略）
