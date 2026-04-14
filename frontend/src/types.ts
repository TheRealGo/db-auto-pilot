export type DatasetSummary = {
  id: string;
  name: string;
  status: string;
  created_at: string;
  updated_at: string;
  approved_proposal_id?: string | null;
};

export type ProposalResponse = {
  id: string;
  dataset_id: string;
  version: number;
  status: string;
  feedback?: string | null;
  created_at: string;
  proposal: {
    summary: string;
    canonical_proposal?: {
      overview: {
        summary: string;
        feedback_applied: boolean;
        source_table_count: number;
        merged_component_count: number;
        review_item_count: number;
        blocking_review_count?: number;
        question_count: number;
      };
      approval_checklist: Array<{
        title: string;
        status: string;
        items: string[];
      }>;
      candidates: Array<{
        component_id: string;
        canonical_name: string;
        decision: string;
        confidence: number;
        review_status: string;
        reason: string;
        source_count: number;
        signal: string;
        override_applied?: boolean;
        evidence_summary?: {
          candidate_score: number;
          signal: string;
          value_overlap: number;
          reasons: string[];
        };
        matches: Array<{
          source_table: string;
          source_column: string;
          display_name: string;
        }>;
      }>;
    };
    observations: Array<{
      table_name: string;
      display_name: string;
      row_count: number;
      columns: Array<{
        source_column: string;
        display_name: string;
        logical_type: string;
        profile: {
          sample_values: Array<string | number | null>;
          unique_samples: Array<string | number | null>;
          null_ratio: number;
          distinct_ratio: number;
          numeric_ratio: number;
          datetime_ratio: number;
        };
      }>;
    }>;
    comparison_candidates: Array<{
      candidate_id: string;
      score: number;
      left: {
        source_table: string;
        source_column: string;
        display_name: string;
        logical_type: string;
      };
      right: {
        source_table: string;
        source_column: string;
        display_name: string;
        logical_type: string;
      };
      reasoning: string[];
    }>;
    agent_steps: Array<{
      step: number;
      mode: string;
      notes: string[];
      observation_requests: Array<{
        tool: string;
        args: Record<string, unknown>;
        reason: string;
        result: unknown;
      }>;
    }>;
    questions_for_user: string[];
    raw_tables: Array<{
      table_name: string;
      display_name: string;
      row_count: number;
      columns: Array<{
        original_name: string;
        normalized_name: string;
        logical_type: string;
      }>;
    }>;
    merged_tables: Array<{
      table_name: string;
      display_name: string;
      source_tables: string[];
      columns: Array<{
        name: string;
        logical_type: string;
        notes: string;
        status: string;
        source_columns: Array<{
          source_table: string;
          source_column: string;
          display_name: string;
        }>;
      }>;
    }>;
    schema_draft: Array<{
      component_id: string;
      display_name: string;
      source_tables: string[];
      columns: Array<{
        name: string;
        logical_type: string;
        status: string;
        source_count: number;
        rationale: string;
      }>;
    }>;
    column_mappings: Array<{
      component_id: string;
      canonical_name: string;
      logical_type: string;
      decision: string;
      review_status: string;
      merge_recommended: boolean;
      confidence: number;
      rationale: string;
      matches: Array<{
        source_table: string;
        source_column: string;
        display_name: string;
      }>;
    }>;
    review_items: Array<{
      type: string;
      severity?: string;
      canonical_name: string;
      component_id: string;
      message: string;
      override_applied?: boolean;
      evidence_summary?: {
        candidate_score: number;
        signal: string;
        value_overlap: number;
        reasons: string[];
      };
      matches: Array<{
        source_table: string;
        source_column: string;
        display_name: string;
      }>;
    }>;
    normalization_actions: Array<{
      table_name: string;
      display_name: string;
      source_column: string;
      normalized_column: string;
      actions: string[];
    }>;
    notes: string[];
    user_decisions: Array<{
      canonical_name: string;
      decision: string;
      reason: string;
      source_columns: Array<{
        source_table: string;
        source_column: string;
      }>;
    }>;
    feedback_overrides?: {
      raw_feedback?: string | null;
      overrides: Array<{
        candidate_id: string;
        type: string;
        canonical_name?: string;
        reason: string;
      }>;
    };
    materialization_plan_draft: {
      components: Array<{
        component_id: string;
        display_name: string;
        source_tables: string[];
        columns: Array<{
          name: string;
          status: string;
          logical_type: string;
          reason: string;
          source_columns: Array<{
            source_table: string;
            source_column: string;
            display_name: string;
            actions: string[];
          }>;
        }>;
      }>;
    };
  };
};

export type DatasetDetail = {
  dataset: DatasetSummary;
  source_files: Array<{
    id: string;
    filename: string;
    file_type: string;
    sheet_count: number;
  }>;
  latest_proposal: ProposalResponse | null;
  proposals: ProposalResponse[];
  latest_materialization_proposal: MaterializationProposalResponse | null;
  tables: Array<{
    mode: "raw" | "merged";
    table_name: string;
    display_name: string;
    schema: {
      columns: Array<{
        name: string;
        logical_type: string;
      }>;
      lineage?: Array<{
        column_name: string;
        source_columns: Array<{
          source_table: string;
          source_column: string;
          display_name: string;
        }>;
        status: string;
      }>;
    };
  }>;
  approval_decisions: Array<{
    canonical_name: string;
    decision: string;
    reason: string;
  }>;
  column_lineage: Array<{
    table_name: string;
    column_name: string;
    source_columns: Array<{
      source_table: string;
      source_column: string;
      display_name: string;
    }>;
    status: string;
  }>;
  materialization_proposals: MaterializationProposalResponse[];
  materialization_runs: Array<{
    id: string;
    proposal_id: string;
    status: string;
    generated_code: string;
    created_at: string;
    result: {
      plan?: unknown;
      generation_summary?: string;
      guard_summary?: {
        status: string;
        imports: string[];
        violations: Array<{
          type: string;
          value: string;
        }>;
      };
      repair_summary?: {
        status: string;
        applied_repairs: string[];
      };
      resource_summary?: {
        merged_table_count: number;
        max_rows_seen: number;
        max_columns_seen: number;
      };
      quality_summary?: {
        status: string;
        warning_count: number;
        table_summaries: Array<{
          component_id: string;
          display_name: string;
          column_summaries: Array<{
            column_name: string;
            logical_type: string;
            actions: string[];
            before: {
              row_count: number;
              null_ratio: number;
              distinct_ratio: number;
              numeric_ratio: number;
              datetime_ratio: number;
            };
            after: {
              row_count: number;
              null_ratio: number;
              distinct_ratio: number;
              numeric_ratio: number;
              datetime_ratio: number;
            };
            warnings: string[];
            warning_details?: Array<{
              code: string;
              severity: string;
              suggested_actions: string[];
            }>;
          }>;
        }>;
        warning_catalog?: Array<{
          code: string;
          severity: string;
          suggested_actions: string[];
        }>;
      };
      warnings?: string[];
      execution_notes?: string[];
      merged_tables?: Array<{
        component_id: string;
        display_name: string;
        physical_name: string;
        row_count: number;
      }>;
      error_stage?: string;
      error?: string;
    };
  }>;
};

export type MaterializationProposalResponse = {
  id: string;
  dataset_id: string;
  proposal_id: string;
  version: number;
  status: string;
  source_run_id?: string | null;
  created_at: string;
  materialization: {
    summary: string;
    normalization_decisions: Array<{
      component_id: string;
      column_name: string;
      actions: string[];
      config?: Record<string, unknown>;
      reason: string;
    }>;
    transformation_notes: string[];
    risk_notes: string[];
    expected_outputs: string[];
    quality_expectations: string[];
    generated_code: string;
    plan: {
      components: Array<{
        component_id: string;
        display_name: string;
        physical_name: string;
        source_tables: string[];
        columns: Array<{
          name: string;
          logical_type: string;
          status: string;
          reason: string;
          source_columns: Array<{
            source_table: string;
            source_column: string;
            display_name: string;
            actions: string[];
          }>;
        }>;
      }>;
    };
    retry_context?: {
      reason: string;
      previous_run_id?: string | null;
      previous_error_stage?: string | null;
      previous_error?: string | null;
      guard_violations?: string[];
      quality_warnings?: string[];
      focus_points?: string[];
      column_issues?: Array<{
        table_name: string;
        column_name: string;
        warnings: string[];
        actions: string[];
        warning_details?: Array<{
          code: string;
          severity: string;
          suggested_actions: string[];
        }>;
      }>;
      column_patches?: Array<{
        component_id: string;
        table_name: string;
        column_name: string;
        warning_types: string[];
        warning_details?: Array<{
          code: string;
          severity: string;
          suggested_actions: string[];
        }>;
        suggested_actions: string[];
        suggested_config_patch: Record<string, unknown>;
      }>;
      proposal_summary?: string | null;
    } | null;
  };
};

export type QueryResponse = {
  sql: string;
  explanation: string;
  generator: string;
  warning?: string | null;
  columns: string[];
  rows: Array<Array<string | number | null>>;
};

export type QueryHistoryEntry = {
  id: string;
  target_mode: "raw" | "merged";
  question: string;
  sql: string;
  explanation: string;
  result: {
    columns: string[];
    rows: Array<Array<string | number | null>>;
  };
  created_at: string;
};

export type AppSettings = {
  api_key: string | null;
  endpoint: string | null;
  model: string | null;
};
