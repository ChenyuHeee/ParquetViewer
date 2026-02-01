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
