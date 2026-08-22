import ReactEChartsCore from 'echarts-for-react/lib/core';
import type { EChartsOption, SeriesOption } from 'echarts';
import * as echarts from 'echarts/core';
import { BarChart, LineChart } from 'echarts/charts';
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

import { formatMillionEuro, formatPercent, titleCaseWords } from '../lib/format';
import type { AnalyzeResponse } from '../types';

echarts.use([LineChart, BarChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer]);

function coerceNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function displayValue(value: number, fiscalSide?: string | null): number {
  if (fiscalSide === 'revenue') return Math.abs(value);
  return value;
}

function pickFrameRows(response: AnalyzeResponse): Array<Record<string, unknown>> {
  return response.analytics_frame?.rows?.length ? response.analytics_frame.rows : response.result_rows;
}

function buildTimeSeriesOption(response: AnalyzeResponse): EChartsOption | null {
  const rows = pickFrameRows(response);
  if (!rows.length) return null;
  const fiscalSide = (response.resolved_analysis?.fiscal_side as string | undefined) || (response.analysis_spec?.fiscal_side as string | undefined);
  const xKey = rows[0].time != null ? 'time' : rows[0].vuosi != null ? 'vuosi' : rows[0]._time_axis != null ? '_time_axis' : null;
  const yKey = rows[0].value != null ? 'value' : rows[0].nettokertyma_sum != null ? 'nettokertyma_sum' : rows[0].summa != null ? 'summa' : null;
  if (!xKey || !yKey) return null;

  const grouped = new Map<string, Array<[string, number]>>();
  for (const row of rows) {
    const rawY = coerceNumber(row[yKey]);
    const rawX = row[xKey];
    if (rawY == null || rawX == null) continue;
    const entity = typeof row.entity === 'string' && row.entity.trim() ? row.entity : 'Budjettisarja';
    const item = grouped.get(entity) || [];
    item.push([String(rawX), displayValue(rawY, fiscalSide)]);
    grouped.set(entity, item);
  }
  if (!grouped.size) return null;

  const xValues = Array.from(new Set(Array.from(grouped.values()).flat().map(([x]) => x)));
  const series: SeriesOption[] = Array.from(grouped.entries()).map(([name, points]) => ({
    type: 'line',
    name,
    smooth: true,
    symbolSize: 8,
    lineStyle: { width: 3 },
    data: points.map(([x, y]) => [x, y]),
    emphasis: { focus: 'series' },
  }));

  return {
    backgroundColor: 'transparent',
    animationDuration: 500,
    tooltip: {
      trigger: 'axis',
      valueFormatter: (value) => formatMillionEuro(typeof value === 'number' ? value : Number(value)),
    },
    legend: {
      top: 0,
      textStyle: { color: '#111111', fontFamily: 'Open Sans' },
    },
    grid: { left: 56, right: 24, top: 56, bottom: 48 },
    xAxis: {
      type: 'category',
      data: xValues,
      axisLine: { lineStyle: { color: '#111111' } },
      axisLabel: { color: '#111111' },
    },
    yAxis: {
      type: 'value',
      axisLabel: {
        color: '#111111',
        formatter: (value: number) => formatMillionEuro(value),
      },
      splitLine: { lineStyle: { color: 'rgba(17,17,17,0.12)' } },
    },
    series,
  };
}

function buildRankingOption(response: AnalyzeResponse): EChartsOption | null {
  const rows = pickFrameRows(response);
  if (!rows.length) return null;
  const fiscalSide = (response.resolved_analysis?.fiscal_side as string | undefined) || (response.analysis_spec?.fiscal_side as string | undefined);

  const labelKey = rows[0].entity != null ? 'entity' : rows[0].momentti_snimi != null ? 'momentti_snimi' : rows[0].hallinnonala != null ? 'hallinnonala' : null;
  const valueKey = rows[0].muutos_pct != null ? 'muutos_pct' : rows[0].pct != null ? 'pct' : rows[0].loppuvuosi_sum != null ? 'loppuvuosi_sum' : rows[0].value != null ? 'value' : null;
  if (!labelKey || !valueKey) return null;

  const items = rows
    .map((row) => ({
      label: String(row[labelKey] ?? 'Tuntematon'),
      value: coerceNumber(row[valueKey]),
    }))
    .filter((item): item is { label: string; value: number } => item.value != null)
    .slice(0, 15)
    .map((item) => ({ ...item, value: displayValue(item.value, fiscalSide) }));

  if (!items.length) return null;

  const asPercent = valueKey.includes('pct');

  return {
    backgroundColor: 'transparent',
    animationDuration: 400,
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      valueFormatter: (value) =>
        asPercent
          ? formatPercent(typeof value === 'number' ? value : Number(value))
          : formatMillionEuro(typeof value === 'number' ? value : Number(value)),
    },
    grid: { left: 220, right: 24, top: 24, bottom: 32 },
    xAxis: {
      type: 'value',
      axisLabel: {
        color: '#111111',
        formatter: (value: number) => (asPercent ? formatPercent(value) : formatMillionEuro(value)),
      },
      splitLine: { lineStyle: { color: 'rgba(17,17,17,0.12)' } },
    },
    yAxis: {
      type: 'category',
      data: items.map((item) => item.label),
      axisLabel: {
        color: '#111111',
        width: 200,
        overflow: 'truncate',
      },
    },
    series: [
      {
        type: 'bar',
        data: items.map((item) => item.value),
        itemStyle: {
          color: '#111111',
          borderRadius: [0, 8, 8, 0],
        },
      },
    ],
  };
}

export function ChartPanel({ response }: { response: AnalyzeResponse }) {
  const frameType = response.analytics_frame?.frame_type;
  const title = response.resolved_analysis?.concept_label
    ? `${String(response.resolved_analysis.concept_label)} · ${titleCaseWords(frameType || 'analyysi')}`
    : titleCaseWords(frameType || 'analyysi');

  const option = frameType === 'ranking' ? buildRankingOption(response) : buildTimeSeriesOption(response);
  if (!option) {
    return (
      <section className="panel empty-chart-panel">
        <h2>Visualisointi</h2>
        <p>Tälle vastaukselle ei vielä löytynyt sopivaa charttia ensimmäisessä frontend-versiossa.</p>
      </section>
    );
  }

  return (
    <section className="panel chart-panel">
      <div className="panel-header">
        <div>
          <div className="section-kicker">Visualisointi</div>
          <h2>{title}</h2>
        </div>
        <div className="panel-meta">{response.visualization_plan?.primary_chart || frameType}</div>
      </div>
      <ReactEChartsCore echarts={echarts} option={option} style={{ height: 420, width: '100%' }} notMerge lazyUpdate />
    </section>
  );
}
