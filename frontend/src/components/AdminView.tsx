import { useEffect, useMemo, useState } from 'react';

import { fetchQuestionLibrary } from '../lib/admin';
import type { QuestionLibraryEntry } from '../types';

function toLocalTimestamp(value?: string | null): string {
  if (!value) return '—';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat('fi-FI', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(parsed);
}

function countBy<T extends string>(items: T[]): Array<{ label: string; count: number }> {
  const counts = new Map<string, number>();
  for (const item of items) {
    const key = item && item.trim() ? item : 'unknown';
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count);
}

function download(filename: string, content: string, mime: string) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="summary-card">
      <div className="summary-label">{label}</div>
      <div className="summary-value">{value}</div>
    </div>
  );
}

function SimpleTable({
  title,
  columns,
  rows,
  tableClassName,
}: {
  title: string;
  columns: string[];
  rows: Array<Record<string, string | number>>;
  tableClassName?: string;
}) {
  return (
    <section className="panel">
      <div className="panel-header tight">
        <div>
          <div className="section-kicker">Admin</div>
          <h2>{title}</h2>
        </div>
      </div>
      <div className="table-shell">
        <table className={tableClassName ? `data-table ${tableClassName}` : 'data-table'}>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={`${title}-${index}`}>
                {columns.map((column) => (
                  <td key={column}>{String(row[column] ?? '—')}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function AdminView() {
  const [rows, setRows] = useState<QuestionLibraryEntry[]>([]);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState('Kaikki');
  const [searchText, setSearchText] = useState('');
  const [showLimit, setShowLimit] = useState(100);

  useEffect(() => {
    let ignore = false;
    setBusy(true);
    fetchQuestionLibrary(5000)
      .then((payload) => {
        if (!ignore) {
          setRows(payload.slice().reverse());
          setError(null);
        }
      })
      .catch((err) => {
        if (!ignore) {
          setError(err instanceof Error ? err.message : 'Tuntematon virhe');
        }
      })
      .finally(() => {
        if (!ignore) setBusy(false);
      });
    return () => {
      ignore = true;
    };
  }, []);

  const totalQueries = rows.length;
  const uniqueQuestions = new Set(rows.map((row) => row.question).filter(Boolean)).size;
  const uniqueSessions = new Set(rows.map((row) => row.session_id).filter(Boolean)).size;
  const successRate = totalQueries
    ? ((rows.filter((row) => row.status === 'success').length / totalQueries) * 100).toFixed(1)
    : '0.0';

  const statusCounts = useMemo(
    () => countBy(rows.map((row) => row.status || 'unknown')).slice(0, 20).map((item) => ({ status: item.label, kyselyitä: item.count })),
    [rows],
  );
  const intentCounts = useMemo(
    () => countBy(rows.map((row) => row.intent || 'unknown')).slice(0, 15).map((item) => ({ intent: item.label, kyselyitä: item.count })),
    [rows],
  );
  const conceptCounts = useMemo(
    () =>
      countBy(rows.map((row) => row.resolved_concept_label || 'ei ratkaistu'))
        .slice(0, 20)
        .map((item) => ({ käsite: item.label, kyselyitä: item.count })),
    [rows],
  );
  const repeatedQuestions = useMemo(
    () => countBy(rows.map((row) => row.question || '')).slice(0, 20).map((item) => ({ kysymys: item.label, toistot: item.count })),
    [rows],
  );

  const statusOptions = useMemo(() => ['Kaikki', ...countBy(rows.map((row) => row.status || 'unknown')).map((item) => item.label)], [rows]);

  const filteredRows = useMemo(() => {
    return rows.filter((row) => {
      const statusOk = statusFilter === 'Kaikki' || (row.status || 'unknown') === statusFilter;
      const searchOk = !searchText.trim() || row.question.toLowerCase().includes(searchText.trim().toLowerCase());
      return statusOk && searchOk;
    });
  }, [rows, searchText, statusFilter]);

  const visibleRows = filteredRows.slice(0, showLimit).map((row) => ({
    ts: toLocalTimestamp(row.ts),
    question: row.question,
    status: row.status || 'unknown',
    intent: row.intent || '—',
    fiscal_side: row.fiscal_side || '—',
    resolved_concept_label: row.resolved_concept_label || 'ei ratkaistu',
    query_source: row.query_source || '—',
    result_row_count: row.result_row_count ?? 0,
    used_moment_count: row.used_moment_count ?? 0,
    error_class: row.error_class || '—',
  }));

  return (
    <div className="content-grid admin-grid">
      <section className="panel admin-hero">
        <div className="section-kicker">Admin</div>
        <h1>Admin</h1>
        <p className="hero-copy">Kysymyskirjaston kertymä palvelun kehittämistä varten.</p>
      </section>

      {busy ? (
        <section className="panel"><p>Ladataan kysymyskirjastoa…</p></section>
      ) : error ? (
        <section className="panel error-panel"><p>{error}</p></section>
      ) : rows.length === 0 ? (
        <section className="panel"><p>Kysymyskirjasto on vielä tyhjä.</p></section>
      ) : (
        <>
          <section className="summary-grid">
            <SummaryCard label="Kyselyitä yhteensä" value={totalQueries.toLocaleString('fi-FI')} />
            <SummaryCard label="Uniikkeja kysymyksiä" value={uniqueQuestions.toLocaleString('fi-FI')} />
            <SummaryCard label="Sessioita" value={uniqueSessions.toLocaleString('fi-FI')} />
            <SummaryCard label="Onnistumisaste" value={`${successRate} %`} />
          </section>

          <section className="two-col-grid">
            <SimpleTable title="Statukset" columns={['status', 'kyselyitä']} rows={statusCounts} tableClassName="compact-table" />
            <SimpleTable title="Intentit" columns={['intent', 'kyselyitä']} rows={intentCounts} tableClassName="compact-table" />
            <SimpleTable
              title="Ratkaistut käsitteet"
              columns={['käsite', 'kyselyitä']}
              rows={conceptCounts}
              tableClassName="compact-table"
            />
            <SimpleTable
              title="Toistuvimmat kysymykset"
              columns={['kysymys', 'toistot']}
              rows={repeatedQuestions}
              tableClassName="compact-table"
            />
          </section>

          <section className="panel">
            <div className="panel-header">
              <div>
                <div className="section-kicker">Kysymykset</div>
                <h2>Kysymyskirjasto</h2>
              </div>
              <div className="panel-meta">Suodatuksen jälkeen rivejä: {filteredRows.length}</div>
            </div>

            <div className="admin-filters">
              <label className="admin-filter">
                <span>Status</span>
                <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                  {statusOptions.map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </label>
              <label className="admin-filter grow">
                <span>Hae kysymystekstistä</span>
                <input value={searchText} onChange={(event) => setSearchText(event.target.value)} placeholder="Kirjoita hakusana" />
              </label>
              <label className="admin-filter">
                <span>Näytä rivejä</span>
                <select value={showLimit} onChange={(event) => setShowLimit(Number(event.target.value))}>
                  {[20, 50, 100, 200, 500].map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <div className="table-shell">
              <table className="data-table admin-data-table">
                <thead>
                  <tr>
                    {['ts', 'question', 'status', 'intent', 'fiscal_side', 'resolved_concept_label', 'query_source', 'result_row_count', 'used_moment_count', 'error_class'].map((column) => (
                      <th key={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {visibleRows.map((row, index) => (
                    <tr key={`${row.ts}-${index}`}>
                      <td>{row.ts}</td>
                      <td>{row.question}</td>
                      <td>{row.status}</td>
                      <td>{row.intent}</td>
                      <td>{row.fiscal_side}</td>
                      <td>{row.resolved_concept_label}</td>
                      <td>{row.query_source}</td>
                      <td>{String(row.result_row_count)}</td>
                      <td>{String(row.used_moment_count)}</td>
                      <td>{row.error_class}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="panel admin-downloads">
            <div className="panel-header tight">
              <div>
                <div className="section-kicker">Lataus</div>
                <h2>Vie kysymyskirjasto</h2>
              </div>
            </div>
            <div className="download-row">
              <button
                className="primary-button"
                type="button"
                onClick={() => {
                  const header = Object.keys(visibleRows[0] || {});
                  const csv = [header.join(',')]
                    .concat(
                      visibleRows.map((row) =>
                        header
                          .map((column) => `"${String(row[column as keyof typeof row] ?? '').split('"').join('""')}"`)
                          .join(','),
                      ),
                    )
                    .join('\n');
                  download('budjettihaukka_question_library.csv', csv, 'text/csv');
                }}
              >
                Lataa CSV
              </button>
              <button
                className="primary-button"
                type="button"
                onClick={() => download('budjettihaukka_question_library.json', JSON.stringify(rows, null, 2), 'application/json')}
              >
                Lataa JSON
              </button>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
