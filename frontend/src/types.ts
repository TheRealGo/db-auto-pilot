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
      canonical_name: string;
      component_id: string;
      message: string;
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
};

export type QueryResponse = {
  sql: string;
  explanation: string;
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
