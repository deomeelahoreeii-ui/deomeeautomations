export type CsvColumn<T extends Record<string, unknown>> = {
  key: keyof T | string;
  label: string;
};

export function csvEscape(value: unknown): string {
  const text = String(value ?? "");
  const escaped = text.replace(/"/g, '""');
  return /[",\n\r]/.test(escaped) ? `"${escaped}"` : escaped;
}

export function rowsToCsv<T extends Record<string, unknown>>(
  columns: CsvColumn<T>[],
  rows: T[],
): string {
  const header = columns.map((column) => csvEscape(column.label)).join(",");
  const body = rows.map((row) => columns.map((column) => csvEscape(row[column.key])).join(","));

  return [header, ...body].join("\r\n");
}
