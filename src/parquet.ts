import {
  asyncBufferFromUrl,
  parquetMetadataAsync,
  parquetReadObjects,
  parquetSchema,
} from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

export type AsyncBuffer = {
  byteLength: number
  slice(start: number, end?: number): ArrayBuffer | Promise<ArrayBuffer>
}

export type ParquetSource =
  | { kind: 'file'; label: string; file: AsyncBuffer }
  | { kind: 'url'; label: string; file: AsyncBuffer; url: string }

export type ParquetMeta = {
  numRows: number
  columns: string[]
  schemaText: string
}

export function asyncBufferFromBrowserFile(file: File): AsyncBuffer {
  return {
    byteLength: file.size,
    slice: async (start: number, end?: number) => {
      const blob = file.slice(start, end)
      return await blob.arrayBuffer()
    },
  }
}

export async function makeSourceFromUrl(url: string): Promise<ParquetSource> {
  const file = await asyncBufferFromUrl({ url })
  return { kind: 'url', label: url, url, file }
}

export function normalizeParquetUrl(input: string): { url: string; note?: string } {
  // Common case: GitHub file page URL ("blob") is HTML, not the raw bytes.
  // Convert it to a raw.githubusercontent.com URL which is fetchable.
  try {
    const u = new URL(input)
    if (u.hostname === 'github.com') {
      const parts = u.pathname.split('/').filter(Boolean)
      // /{owner}/{repo}/blob/{ref}/{path...}
      if (parts.length >= 5 && parts[2] === 'blob') {
        const owner = parts[0]
        const repo = parts[1]
        const ref = parts[3]
        const path = parts.slice(4).join('/')
        return {
          url: `https://raw.githubusercontent.com/${owner}/${repo}/${ref}/${path}`,
          note: '已将 GitHub blob 链接转换为 raw 直链',
        }
      }
      // /{owner}/{repo}/raw/{ref}/{path...}
      if (parts.length >= 5 && parts[2] === 'raw') {
        const owner = parts[0]
        const repo = parts[1]
        const ref = parts[3]
        const path = parts.slice(4).join('/')
        return {
          url: `https://raw.githubusercontent.com/${owner}/${repo}/${ref}/${path}`,
          note: '已将 GitHub raw 链接标准化为 raw.githubusercontent.com',
        }
      }
    }
    // Some users paste "?raw=1" URLs from GitHub UI; keep it as-is unless it's a blob link.
    return { url: input }
  } catch {
    return { url: input }
  }
}

export async function readMetadata(source: ParquetSource): Promise<ParquetMeta> {
  const metadata = await parquetMetadataAsync(source.file)
  const numRows = Number(metadata.num_rows)
  const schema = parquetSchema(metadata)
  const columns = schema.children.map((e) => e.element.name)
  const schemaText = schemaToText(schema)
  return { numRows, columns, schemaText }
}

export async function readRows(source: ParquetSource, opts: {
  rowStart: number
  rowEnd: number
  columns?: string[]
}): Promise<Record<string, unknown>[]> {
  const data = await parquetReadObjects({
    file: source.file,
    compressors,
    columns: opts.columns,
    rowStart: opts.rowStart,
    rowEnd: opts.rowEnd,
  })
  return data
}

function schemaToText(schema: any): string {
  // Keep it lightweight and readable for the UI.
  // hyparquet's schema object is nested; we'll print a compact tree.
  const lines: string[] = []
  const walk = (node: any, indent: string) => {
    const name = node?.element?.name ?? '(root)'
    const type = node?.element?.type ?? node?.element?.logicalType ?? ''
    if (name !== '(root)') {
      lines.push(`${indent}${name}${type ? `: ${String(type)}` : ''}`)
    }
    const children: any[] = Array.isArray(node?.children) ? node.children : []
    for (const child of children) {
      walk(child, indent + '  ')
    }
  }
  walk(schema, '')
  return lines.join('\n')
}
