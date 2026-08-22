import type { AnalyzeResponse } from '../types';

function renderValue(value: unknown): string {
  if (value == null) return '—';
  if (typeof value === 'number') return Number.isFinite(value) ? value.toLocaleString('fi-FI') : '—';
  if (typeof value === 'boolean') return value ? 'Kyllä' : 'Ei';
  return String(value);
}

export function ResultTable({ response }: { response: AnalyzeResponse }) {
  const columns = response.analytics_frame?.columns?.length ? response.analytics_frame.columns : response.result_columns;
  const rows = response.analytics_frame?.rows?.length ? response.analytics_frame.rows : response.result_rows;

  if (!columns.length || !rows.length) {
    return null;
  }

  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <div className="section-kicker">Tulostaulu</div>
          <h2>Analyysin palauttamat rivit</h2>
        </div>
        <div className="panel-meta">Näytetään {Math.min(rows.length, 20)} / {rows.length}</div>
      </div>
      <div className="table-shell">
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.slice(0, 20).map((row, index) => (
              <tr key={`${index}-${String(row[columns[0]] ?? index)}`}>
                {columns.map((column) => (
                  <td key={column}>{renderValue(row[column])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
