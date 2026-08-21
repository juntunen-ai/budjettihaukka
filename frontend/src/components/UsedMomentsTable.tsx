import type { AnalyzeResponse } from '../types';

function visibleColumns(rows: Array<Record<string, unknown>>): string[] {
  const preferred = ['momentti_tunnusp', 'momentti_snimi', 'budjettipuoli', 'budjettiryhma', 'membership_type', 'confidence', 'nettokertyma_sum'];
  const available = new Set(rows.flatMap((row) => Object.keys(row)));
  const selected = preferred.filter((key) => available.has(key));
  if (selected.length) return selected;
  return Array.from(available).slice(0, 8);
}

function renderValue(value: unknown): string {
  if (value == null) return '—';
  if (typeof value === 'number') return Number.isFinite(value) ? value.toLocaleString('fi-FI') : '—';
  if (typeof value === 'boolean') return value ? 'Kyllä' : 'Ei';
  return String(value);
}

export function UsedMomentsTable({ response }: { response: AnalyzeResponse }) {
  const rows = response.used_moments || [];
  if (!rows.length) {
    return (
      <section className="panel">
        <div className="panel-header">
          <div>
            <div className="section-kicker">Käytetyt budjettimomentit</div>
            <h2>Ei momenttievidenssiä</h2>
          </div>
        </div>
        <p>Frontend ei saanut tältä kyselyltä momenttievidenssiä taustapalvelusta.</p>
      </section>
    );
  }

  const columns = visibleColumns(rows);

  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <div className="section-kicker">Käytetyt budjettimomentit</div>
          <h2>Vastaus rajattiin näillä budjettiriveillä</h2>
        </div>
        <div className="panel-meta">{rows.length} riviä</div>
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
