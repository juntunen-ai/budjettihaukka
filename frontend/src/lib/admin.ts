import type { QuestionLibraryEntry } from '../types';
import { API_BASE_URL } from './api';
import { getAuthToken } from './firebase';

export interface QuestionLibraryResponse {
  rows: QuestionLibraryEntry[];
}

export async function fetchQuestionLibrary(limit = 5000, adminKey = ''): Promise<QuestionLibraryEntry[]> {
  const token = await getAuthToken();
  const response = await fetch(`${API_BASE_URL}/v1/admin/question-library?limit=${limit}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      ...(adminKey ? { 'X-Admin-Key': adminKey } : {}),
    },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `API error ${response.status}`);
  }
  const payload = (await response.json()) as QuestionLibraryResponse;
  return payload.rows || [];
}
