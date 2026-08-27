import type { AnalyzeRequest, AnalyzeResponse } from '../types';

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '');

export async function analyzeQuestion(payload: AnalyzeRequest): Promise<AnalyzeResponse> {
  const response = await fetch(`${API_BASE_URL}/v1/analyze`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `API error ${response.status}`);
  }

  return (await response.json()) as AnalyzeResponse;
}

export async function checkHealth(): Promise<boolean> {
  try {
    const response = await fetch(`${API_BASE_URL}/health`);
    if (!response.ok) return false;
    const data = (await response.json()) as { status?: string };
    return data.status === 'ok';
  } catch {
    return false;
  }
}

export { API_BASE_URL };
