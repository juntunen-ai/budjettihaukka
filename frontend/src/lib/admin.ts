import type { QuestionLibraryEntry } from '../types';
import { API_BASE_URL } from './api';

export interface QuestionLibraryResponse {
  rows: QuestionLibraryEntry[];
}

export async function fetchQuestionLibrary(limit = 5000, adminKey = ''): Promise<QuestionLibraryEntry[]> {
  const response = await fetch(`${API_BASE_URL}/v1/admin/question-library?limit=${limit}`, {
    headers: adminKey ? { 'X-Admin-Key': adminKey } : {},
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `API error ${response.status}`);
  }
  const payload = (await response.json()) as QuestionLibraryResponse;
  return payload.rows || [];
}
