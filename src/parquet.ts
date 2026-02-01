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

export type UrlCandidate = {
  url: string
  label: string
}

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

export function normalizeParquetUrl(input: string): { candidates: UrlCandidate[]; note?: string } {
  // Goals:
  // - Support pasting GitHub file page links (blob) by converting to fetchable endpoints.
  // - Handle Git LFS: raw.githubusercontent.com may return an LFS pointer; media.githubusercontent.com often serves the actual bytes.
  // - Keep a fallback list to maximize chances of working under different CORS/Range behaviors.

  const fallback = (url: string) => ({ candidates: [{ url, label: 'Direct URL' }] })

  try {
    const u = new URL(input)

    // GitHub file page URLs
    if (u.hostname === 'github.com') {
      const parts = u.pathname.split('/').filter(Boolean)
      // /{owner}/{repo}/blob/{ref}/{path...}
      if (parts.length >= 5 && parts[2] === 'blob') {
        const owner = parts[0]
        const repo = parts[1]
        const ref = parts[3]
        const path = parts.slice(4).join('/')
        return {
          note: '检测到 GitHub 文件页面链接，将尝试可下载的直链（含 Git LFS 兼容）',
          candidates: githubCandidates(owner, repo, ref, path),
        }
      }
      // /{owner}/{repo}/raw/{ref}/{path...} (sometimes provided by UI)
      if (parts.length >= 5 && parts[2] === 'raw') {
        const owner = parts[0]
        const repo = parts[1]
        const ref = parts[3]
        const path = parts.slice(4).join('/')
        return {
          note: '检测到 GitHub raw 链接，将标准化并尝试多种直链',
          candidates: githubCandidates(owner, repo, ref, path),
        }
      }
      return fallback(input)
    }

    // raw.githubusercontent.com URLs
    if (u.hostname === 'raw.githubusercontent.com') {
      const parts = u.pathname.split('/').filter(Boolean)
      if (parts.length >= 4) {
        const owner = parts[0]
        const repo = parts[1]
        const ref = parts[2]
        const path = parts.slice(3).join('/')
        return {
          note: '检测到 raw.githubusercontent.com，将同时尝试 media 直链（兼容 Git LFS）',
          candidates: [
            { url: `https://media.githubusercontent.com/media/${owner}/${repo}/${ref}/${path}`, label: 'GitHub media (LFS-friendly)' },
            { url: input, label: 'GitHub raw' },
            { url: `https://cdn.jsdelivr.net/gh/${owner}/${repo}@${ref}/${path}`, label: 'jsDelivr CDN' },
          ],
        }
      }
      return fallback(input)
    }

    // media.githubusercontent.com URLs
    if (u.hostname === 'media.githubusercontent.com') {
      // /media/{owner}/{repo}/{ref}/{path...}
      const parts = u.pathname.split('/').filter(Boolean)
      if (parts.length >= 5 && parts[0] === 'media') {
        const owner = parts[1]
        const repo = parts[2]
        const ref = parts[3]
        const path = parts.slice(4).join('/')
        return {
          candidates: [
            { url: input, label: 'GitHub media (LFS-friendly)' },
            { url: `https://raw.githubusercontent.com/${owner}/${repo}/${ref}/${path}`, label: 'GitHub raw' },
            { url: `https://cdn.jsdelivr.net/gh/${owner}/${repo}@${ref}/${path}`, label: 'jsDelivr CDN' },
          ],
        }
      }
      return fallback(input)
    }

    return fallback(input)
  } catch {
    return fallback(input)
  }
}

function githubCandidates(owner: string, repo: string, ref: string, path: string): UrlCandidate[] {
  return [
    { url: `https://media.githubusercontent.com/media/${owner}/${repo}/${ref}/${path}`, label: 'GitHub media (LFS-friendly)' },
    { url: `https://raw.githubusercontent.com/${owner}/${repo}/${ref}/${path}`, label: 'GitHub raw' },
    { url: `https://cdn.jsdelivr.net/gh/${owner}/${repo}@${ref}/${path}`, label: 'jsDelivr CDN' },
  ]
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
