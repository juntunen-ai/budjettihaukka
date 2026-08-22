const euroFormatter = new Intl.NumberFormat('fi-FI', {
  minimumFractionDigits: 0,
  maximumFractionDigits: 1,
});

const compactFormatter = new Intl.NumberFormat('fi-FI', {
  notation: 'compact',
  compactDisplay: 'short',
  minimumFractionDigits: 0,
  maximumFractionDigits: 1,
});

export function formatEuro(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '—';
  return `${euroFormatter.format(value)} €`;
}

export function formatMillionEuro(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '—';
  return `${euroFormatter.format(value / 1_000_000)} milj. €`;
}

export function formatCompact(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '—';
  return compactFormatter.format(value);
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '—';
  return `${euroFormatter.format(value)} %`;
}

export function formatBytes(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${euroFormatter.format(size)} ${units[index]}`;
}

export function titleCaseWords(input: string): string {
  return input
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}
