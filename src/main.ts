import './style.css'
import {
  asyncBufferFromBrowserFile,
  makeSourceFromUrl,
  readMetadata,
  readRows,
  type ParquetSource,
} from './parquet'

type LoadState =
  | { kind: 'idle' }
  | { kind: 'loading'; message: string }
  | { kind: 'ready'; message: string }
  | { kind: 'error'; message: string }

const app = document.querySelector<HTMLDivElement>('#app')
if (!app) throw new Error('Missing #app')

let source: ParquetSource | null = null
let totalRows = 0
let allColumns: string[] = []
let selectedColumns = new Set<string>()
let schemaText = ''

let pageSize = 100
let pageIndex = 0
let currentRows: Record<string, unknown>[] = []
let columnSearch = ''

let loadState: LoadState = { kind: 'idle' }

render()

function setState(next: Partial<{
  source: ParquetSource | null
  totalRows: number
  allColumns: string[]
  selectedColumns: Set<string>
  schemaText: string
  pageSize: number
  pageIndex: number
  currentRows: Record<string, unknown>[]
  columnSearch: string
  loadState: LoadState
}>) {
  if (next.source !== undefined) source = next.source
  if (next.totalRows !== undefined) totalRows = next.totalRows
  if (next.allColumns !== undefined) allColumns = next.allColumns
  if (next.selectedColumns !== undefined) selectedColumns = next.selectedColumns
  if (next.schemaText !== undefined) schemaText = next.schemaText
  if (next.pageSize !== undefined) pageSize = next.pageSize
  if (next.pageIndex !== undefined) pageIndex = next.pageIndex
  if (next.currentRows !== undefined) currentRows = next.currentRows
  if (next.columnSearch !== undefined) columnSearch = next.columnSearch
  if (next.loadState !== undefined) loadState = next.loadState
  render()
}

async function loadFromFile(file: File) {
  try {
    setState({
      loadState: { kind: 'loading', message: `正在读取: ${file.name}` },
      source: { kind: 'file', label: file.name, file: asyncBufferFromBrowserFile(file) },
      pageIndex: 0,
      currentRows: [],
    })
    await refreshMetadataAndFirstPage()
  } catch (e) {
    setState({ loadState: { kind: 'error', message: formatError(e) } })
  }
}

async function loadFromUrl(url: string) {
  try {
    const trimmed = url.trim()
    if (!trimmed) return
    setState({
      loadState: { kind: 'loading', message: `正在打开 URL: ${trimmed}` },
      source: null,
      pageIndex: 0,
      currentRows: [],
    })
    const s = await makeSourceFromUrl(trimmed)
    setState({ source: s })
    await refreshMetadataAndFirstPage()
  } catch (e) {
    setState({ loadState: { kind: 'error', message: formatError(e) } })
  }
}

async function refreshMetadataAndFirstPage() {
  if (!source) return
  setState({ loadState: { kind: 'loading', message: '读取 Parquet 元数据…' } })
  const meta = await readMetadata(source)
  const defaultCols = meta.columns.slice(0, 30)
  setState({
    totalRows: meta.numRows,
    allColumns: meta.columns,
    selectedColumns: new Set(defaultCols),
    schemaText: meta.schemaText,
  })
  await loadPage(0)
  setState({ loadState: { kind: 'ready', message: `已加载：${source.label}` } })
}

async function loadPage(nextPageIndex: number) {
  if (!source) return
  const safePageIndex = Math.max(0, nextPageIndex)
  const start = safePageIndex * pageSize
  const end = Math.min(totalRows, start + pageSize)
  if (totalRows > 0 && start >= totalRows) return

  const cols = Array.from(selectedColumns)
  setState({ loadState: { kind: 'loading', message: `读取行 ${start}..${end}…` } })
  const rows = await readRows(source, { rowStart: start, rowEnd: end, columns: cols })
  setState({ pageIndex: safePageIndex, currentRows: rows, loadState: { kind: 'ready', message: '' } })
}

function clearAll() {
  setState({
    source: null,
    totalRows: 0,
    allColumns: [],
    selectedColumns: new Set(),
    schemaText: '',
    pageIndex: 0,
    currentRows: [],
    columnSearch: '',
    loadState: { kind: 'idle' },
  })
}

function render() {
  const busy = isLoading()
  const disableWhenBusy = busy ? 'disabled' : ''
  const statusText =
    loadState.kind === 'idle'
      ? '等待打开文件…'
      : loadState.kind === 'ready'
        ? loadState.message || '就绪'
        : loadState.message

  app.innerHTML = `
    <div class="container">
      <div class="header">
        <div>
          <div class="title">Parquet Viewer</div>
          <div class="subtitle">纯前端解析（不上传文件），适配 GitHub Pages</div>
        </div>
        <div class="row">
          <span class="pill">rows: ${totalRows ? totalRows.toLocaleString() : '-'}</span>
          <span class="pill">page: ${totalRows ? pageIndex + 1 : '-'}</span>
        </div>
      </div>

      <div class="grid">
        <div class="panel">
          <div class="row">
            <button class="btn primary" id="pickFileBtn" ${disableWhenBusy}>选择本地 .parquet</button>
            <input id="fileInput" type="file" accept=".parquet,application/octet-stream" style="display:none" />
            <button class="btn danger" id="clearBtn" ${source && !busy ? '' : 'disabled'}>清空</button>
          </div>

          <div class="row">
            <div class="drop" id="dropZone">
              拖拽 Parquet 文件到这里（仅在浏览器本地解析）
            </div>
          </div>

          <div class="row">
            <div style="width:100%">
              <label>也可以打开远程 URL（需要 CORS + 支持 Range）</label>
              <div class="row" style="width:100%">
                <div style="flex:1; min-width: 220px;">
                  <input type="text" id="urlInput" placeholder="https://.../file.parquet" ${busy ? 'disabled' : ''} />
                </div>
                <button class="btn" id="openUrlBtn" ${disableWhenBusy}>打开 URL</button>
              </div>
            </div>
          </div>

          <div class="row">
            <div style="flex:1; min-width: 160px;">
              <label>每页行数</label>
              <input type="number" id="pageSizeInput" min="10" max="5000" value="${pageSize}" ${busy ? 'disabled' : ''} />
            </div>
            <div style="flex:1; min-width: 160px;">
              <label>列名搜索（${selectedColumns.size}/${allColumns.length || 0} 已选）</label>
              <input type="text" id="colSearchInput" placeholder="输入关键字" value="${escapeHtml(columnSearch)}" ${busy ? 'disabled' : ''} />
            </div>
          </div>

          <div class="row">
            <button class="btn" id="selectAllBtn" ${allColumns.length && !busy ? '' : 'disabled'}>全选</button>
            <button class="btn" id="selectNoneBtn" ${allColumns.length && !busy ? '' : 'disabled'}>全不选</button>
            <button class="btn" id="reloadBtn" ${source && !busy ? '' : 'disabled'}>刷新当前页</button>
          </div>

          <div class="row">
            <div style="width:100%">
              <label>列（默认只选前 30 列，避免一次性渲染过重）</label>
              <div class="columns" id="columnsBox"></div>
            </div>
          </div>

          <div class="row">
            <div style="width:100%">
              <details class="details" ${schemaText ? '' : 'open'}>
                <summary>Schema（点击展开/收起）</summary>
                <div class="details-body">${escapeHtml(schemaText || '')}</div>
              </details>
            </div>
          </div>

          <div class="row">
            <div class="status ${loadState.kind === 'error' ? 'error' : loadState.kind === 'ready' ? 'ok' : ''}">
              ${busy ? '<span class="spinner-sm" aria-hidden="true"></span>' : ''}${escapeHtml(statusText)}
            </div>
          </div>
        </div>

        <div class="panel" aria-busy="${busy ? 'true' : 'false'}">
          <div class="row" style="justify-content: space-between; width:100%">
            <div class="row">
              <button class="btn" id="prevBtn" ${canPrev() && !busy ? '' : 'disabled'}>上一页</button>
              <button class="btn" id="nextBtn" ${canNext() && !busy ? '' : 'disabled'}>下一页</button>
            </div>
            <div class="row">
              <button class="btn" id="downloadCsvBtn" ${currentRows.length && !busy ? '' : 'disabled'}>导出当前页 CSV</button>
              <span class="muted">${pageRangeText()}</span>
            </div>
          </div>

          <div class="row" style="margin-top: 12px; width:100%">
            <div class="table-wrap" style="width:100%">
              ${renderTable()}
            </div>
          </div>

          ${busy ? renderLoadingOverlay(loadState.kind === 'loading' ? loadState.message : '加载中…') : ''}
        </div>
      </div>
    </div>
  `

  wireEvents()
  renderColumnsBox()
}

function wireEvents() {
  const pickFileBtn = document.querySelector<HTMLButtonElement>('#pickFileBtn')
  const fileInput = document.querySelector<HTMLInputElement>('#fileInput')
  const clearBtn = document.querySelector<HTMLButtonElement>('#clearBtn')
  const dropZone = document.querySelector<HTMLDivElement>('#dropZone')
  const openUrlBtn = document.querySelector<HTMLButtonElement>('#openUrlBtn')
  const urlInput = document.querySelector<HTMLInputElement>('#urlInput')
  const pageSizeInput = document.querySelector<HTMLInputElement>('#pageSizeInput')
  const colSearchInput = document.querySelector<HTMLInputElement>('#colSearchInput')
  const selectAllBtn = document.querySelector<HTMLButtonElement>('#selectAllBtn')
  const selectNoneBtn = document.querySelector<HTMLButtonElement>('#selectNoneBtn')
  const reloadBtn = document.querySelector<HTMLButtonElement>('#reloadBtn')
  const prevBtn = document.querySelector<HTMLButtonElement>('#prevBtn')
  const nextBtn = document.querySelector<HTMLButtonElement>('#nextBtn')
  const downloadCsvBtn = document.querySelector<HTMLButtonElement>('#downloadCsvBtn')

  pickFileBtn?.addEventListener('click', () => fileInput?.click())
  fileInput?.addEventListener('change', async () => {
    const file = fileInput.files?.[0]
    if (file) await loadFromFile(file)
    fileInput.value = ''
  })

  clearBtn?.addEventListener('click', () => clearAll())

  dropZone?.addEventListener('dragover', (e) => {
    e.preventDefault()
    dropZone.classList.add('dragover')
  })
  dropZone?.addEventListener('dragleave', () => dropZone.classList.remove('dragover'))
  dropZone?.addEventListener('drop', async (e) => {
    e.preventDefault()
    dropZone.classList.remove('dragover')
    const file = e.dataTransfer?.files?.[0]
    if (file) await loadFromFile(file)
  })

  openUrlBtn?.addEventListener('click', async () => {
    const url = urlInput?.value ?? ''
    await loadFromUrl(url)
  })

  pageSizeInput?.addEventListener('change', async () => {
    const n = Number(pageSizeInput.value)
    const next = clampInt(n, 10, 5000)
    setState({ pageSize: next, pageIndex: 0 })
    if (source) await loadPage(0)
  })

  colSearchInput?.addEventListener('input', () => {
    setState({ columnSearch: colSearchInput.value })
  })

  selectAllBtn?.addEventListener('click', () => {
    setState({ selectedColumns: new Set(allColumns) })
  })
  selectNoneBtn?.addEventListener('click', () => {
    setState({ selectedColumns: new Set() })
  })

  reloadBtn?.addEventListener('click', async () => {
    if (!source) return
    await loadPage(pageIndex)
  })

  prevBtn?.addEventListener('click', async () => {
    if (!source) return
    await loadPage(pageIndex - 1)
  })
  nextBtn?.addEventListener('click', async () => {
    if (!source) return
    await loadPage(pageIndex + 1)
  })

  downloadCsvBtn?.addEventListener('click', () => {
    if (!currentRows.length) return
    downloadCsv(currentRows, Array.from(selectedColumns), `parquet_page_${pageIndex + 1}.csv`)
  })
}

function renderColumnsBox() {
  const box = document.querySelector<HTMLDivElement>('#columnsBox')
  if (!box) return
  const q = columnSearch.trim().toLowerCase()
  const cols = q ? allColumns.filter((c) => c.toLowerCase().includes(q)) : allColumns
  box.innerHTML = cols
    .map((c) => {
      const checked = selectedColumns.has(c)
      return `
        <label class="item" title="${escapeHtml(c)}">
          <input type="checkbox" data-col="${escapeHtmlAttr(c)}" ${checked ? 'checked' : ''} />
          <span>${escapeHtml(c)}</span>
        </label>
      `
    })
    .join('')

  box.querySelectorAll<HTMLInputElement>('input[type=checkbox][data-col]').forEach((el) => {
    el.addEventListener('change', () => {
      const col = el.dataset.col
      if (!col) return
      const next = new Set(selectedColumns)
      if (el.checked) next.add(col)
      else next.delete(col)
      setState({ selectedColumns: next })
    })
  })
}

function isLoading(): boolean {
  return loadState.kind === 'loading'
}

function renderLoadingOverlay(message: string): string {
  return `
    <div class="loading-overlay" role="status" aria-live="polite">
      <div class="loading-card">
        <div class="spinner" aria-hidden="true"></div>
        <div class="status">${escapeHtml(message || '加载中…')}</div>
      </div>
    </div>
  `
}

function canPrev(): boolean {
  return !!source && pageIndex > 0
}

function canNext(): boolean {
  if (!source) return false
  const nextStart = (pageIndex + 1) * pageSize
  return totalRows === 0 ? true : nextStart < totalRows
}

function pageRangeText(): string {
  if (!source) return ''
  if (totalRows === 0) return ''
  const start = pageIndex * pageSize
  const end = Math.min(totalRows, start + pageSize)
  return `显示 ${start.toLocaleString()}..${(end - 1).toLocaleString()}（共 ${totalRows.toLocaleString()} 行）`
}

function renderTable(): string {
  const cols = Array.from(selectedColumns)
  if (!source) {
    return `<div class="status">打开一个 Parquet 文件后，会在这里显示预览表格。</div>`
  }
  if (!cols.length) {
    return `<div class="status">未选择任何列。请在左侧勾选要展示的列。</div>`
  }
  if (!currentRows.length) {
    return `<div class="status">暂无数据。可以尝试“刷新当前页”或调整页码/列选择。</div>`
  }

  const head = cols.map((c) => `<th>${escapeHtml(c)}</th>`).join('')
  const body = currentRows
    .map((row) => {
      const tds = cols
        .map((c) => {
          const v = (row as any)[c]
          return `<td title="${escapeHtmlAttr(valueToString(v))}">${escapeHtml(valueToString(v))}</td>`
        })
        .join('')
      return `<tr>${tds}</tr>`
    })
    .join('')

  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`
}

function valueToString(v: unknown): string {
  if (v === null) return 'null'
  if (v === undefined) return ''
  if (typeof v === 'bigint') return v.toString()
  if (typeof v === 'string') return v
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  if (v instanceof Date) return v.toISOString()
  try {
    return JSON.stringify(v)
  } catch {
    return String(v)
  }
}

function downloadCsv(rows: Record<string, unknown>[], cols: string[], filename: string) {
  const header = cols.map(csvEscape).join(',')
  const lines = rows.map((r) => cols.map((c) => csvEscape(valueToString((r as any)[c]))).join(','))
  const csv = [header, ...lines].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

function csvEscape(s: string): string {
  const mustQuote = /[",\n\r]/.test(s)
  const escaped = s.replace(/"/g, '""')
  return mustQuote ? `"${escaped}"` : escaped
}

function escapeHtml(s: string): string {
  return s
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;')
}

function escapeHtmlAttr(s: string): string {
  return escapeHtml(s)
}

function clampInt(n: number, min: number, max: number): number {
  if (!Number.isFinite(n)) return min
  const i = Math.floor(n)
  return Math.min(max, Math.max(min, i))
}

function formatError(e: unknown): string {
  if (e instanceof Error) return e.stack || e.message
  return String(e)
}
