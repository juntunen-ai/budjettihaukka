export type VerificationStatus = 'trusted' | 'trusted_with_warning' | 'needs_clarification' | 'unsupported' | string;
export type AnalyzeStatus = 'success' | 'clarification_required' | 'unsupported' | 'error' | string;

export interface AnalyzeRequest {
  question: string;
  clarifications?: Record<string, string>;
  language?: string;
  ui_context?: Record<string, unknown>;
}

export interface AnalyticsFrame {
  frame_type: 'time_series' | 'ranking' | 'table' | string;
  columns: string[];
  rows: Array<Record<string, unknown>>;
  row_count: number;
}

export interface VisualizationPlan {
  templates: string[];
  primary_chart: string | null;
  secondary_chart: string | null;
  table_view: boolean;
  show_used_moments: boolean;
}

export interface AnalyzeResponse {
  status: AnalyzeStatus;
  question: string;
  execution_question: string;
  analysis_spec: Record<string, unknown>;
  resolved_analysis: Record<string, unknown>;
  analytics_frame: AnalyticsFrame | null;
  visualization_plan: VisualizationPlan | null;
  result_rows: Array<Record<string, unknown>>;
  result_columns: string[];
  used_moments: Array<Record<string, unknown>>;
  explanation: string;
  query_id: string | null;
  query_source: string | null;
  query_contract: string | null;
  sql_query: string | null;
  dry_run_bytes: number | null;
  retries: number;
  error: string | null;
  error_class: string | null;
  verification_status: VerificationStatus | null;
  warnings: string[];
  metadata: Record<string, unknown>;
}

export interface QuestionLibraryEntry {
  ts?: string | null;
  session_id?: string | null;
  surface?: string | null;
  language?: string | null;
  status?: string | null;
  question: string;
  clarification_required?: boolean | null;
  clarification_choices?: Record<string, unknown>;
  clarification_missing_fields?: string[];
  intent?: string | null;
  metric?: string | null;
  fiscal_side?: string | null;
  entity_level?: string | null;
  growth_type?: string | null;
  time_from?: number | null;
  time_to?: number | null;
  requested_time_from?: number | null;
  requested_time_to?: number | null;
  confidence?: number | null;
  resolved_concept_id?: string | null;
  resolved_concept_label?: string | null;
  ontology_match_score?: number | null;
  ontology_risk_level?: string | null;
  ontology_must_clarify?: boolean | null;
  query_source?: string | null;
  query_contract?: string | null;
  query_id?: string | null;
  used_moment_count?: number | null;
  result_row_count?: number | null;
  error_class?: string | null;
  error_message?: string | null;
}
