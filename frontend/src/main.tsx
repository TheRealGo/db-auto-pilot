import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

type SourceFile = {
  id: string;
  filename: string;
  sheet_name?: string | null;
  table_name: string;
  rows: number;
  columns: string[];
  profile: Record<string, unknown>;
};

type Dataset = {
  id: string;
  name: string;
  status: string;
  created_at: string;
  updated_at: string;
  source_count: number;
  materialized_table?: string | null;
  sources?: SourceFile[];
  proposal?: Record<string, unknown> | null;
  proposal_id?: string | null;
  proposal_version?: number | null;
  materialization?: Record<string, unknown> | null;
  materialization_runs?: MaterializationRun[];
};

type MaterializationRun = {
  id: string;
  dataset_id: string;
  table_name: string;
  status: string;
  row_count: number;
  column_count: number;
  plan: Record<string, unknown>;
  error?: string | null;
  created_at: string;
};

type QueryResult = {
  sql: string;
  rows: Record<string, unknown>[];
  columns: string[];
  explanation: string;
  history_id: string;
};

type HistoryEntry = {
  id: string;
  dataset_id?: string | null;
  question: string;
  sql: string;
  row_count: number;
  created_at: string;
  explanation: string;
  columns: string[];
  result_preview: Record<string, unknown>[];
  materialized_table?: string | null;
  materialization_run_id?: string | null;
  target_mode: string;
  proposal_id?: string | null;
  proposal_version?: number | null;
  materialization_proposal_id?: string | null;
  materialization_proposal_version?: number | null;
};

type ApprovalEntry = {
  id: string;
  dataset_id: string;
  proposal_id?: string | null;
  decision_type: string;
  notes: string;
  payload: Record<string, unknown>;
  created_at: string;
};

type Analytics = {
  dataset_id: string;
  materialized_table?: string | null;
  materialization_run_id?: string | null;
  tables: AnalyticsTable[];
};

type AnalyticsTable = {
  table_name: string;
  row_count: number;
  column_count: number;
  numeric_summaries: Record<string, Record<string, number | null>>;
  categorical_top_values: Record<string, Array<{ value: string; count: number }>>;
  recommended_charts: Array<Record<string, unknown>>;
};

type MaterializationProposal = {
  dataset_id: string;
  materialization_proposal_id: string;
  version: number;
  plan: Record<string, unknown>;
};

type AppSettings = {
  openai_model: string;
  llm_enabled: boolean;
  llm_data_policy: string;
  openai_api_key_configured: boolean;
  max_upload_mb: number;
  max_materialization_rows: number;
  max_materialization_columns: number;
  query_row_limit: number;
  cors_allow_origins: string[];
};

type Diagnostics = {
  status: string;
  app_version: string;
  schema_version: number;
  database_user_version: number;
  database_ready: boolean;
  uploads_dir_ready: boolean;
  database_integrity: string;
  foreign_key_violations: number;
  migration_backup_count: number;
  latest_migration_backup?: string | null;
  counts: Record<string, number>;
  settings: AppSettings;
};

type ProposalHistoryEntry = {
  id: string;
  dataset_id: string;
  version: number;
  feedback: string;
  proposal: Record<string, unknown>;
  created_at: string;
};

type MaterializationProposalHistoryEntry = {
  id: string;
  dataset_id: string;
  version: number;
  feedback: string;
  plan: Record<string, unknown>;
  created_at: string;
};

const api = async <T,>(path: string, init?: RequestInit): Promise<T> => {
  const response = await fetch(path, init);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || response.statusText);
  }
  return response.json() as Promise<T>;
};

const countArray = (value: unknown): number | null => Array.isArray(value) ? value.length : null;

const proposalSummary = (proposal: Record<string, unknown> | null | undefined): Array<[string, string]> => {
  const columns = Array.isArray(proposal?.canonical_columns) ? proposal.canonical_columns : [];
  if (columns.length === 0) return [];
  const counts = columns.reduce(
    (acc, item) => {
      const group = typeof item === "object" && item !== null ? item as Record<string, unknown> : {};
      const decision = typeof group.decision === "string" ? group.decision : "unknown";
      acc.total += 1;
      acc[decision] = (acc[decision] || 0) + 1;
      if (group.review_required === true) acc.review_required += 1;
      return acc;
    },
    { total: 0, review_required: 0 } as Record<string, number>
  );
  return [
    ["columns", String(counts.total)],
    ["review required", String(counts.review_required)],
    ["merge", String(counts.merge || 0)],
    ["keep", String(counts.keep || 0)],
    ["review", String(counts.review || 0)]
  ];
};

const proposalChanges = (proposal: Record<string, unknown> | null | undefined): string[] => {
  const changes = proposal?.change_summary;
  return Array.isArray(changes) ? changes.filter((item): item is string => typeof item === "string") : [];
};

const sourceProfileRows = (source: SourceFile): Array<{ column: string; type: string; nullRatio: string; distinct: string }> => {
  const columns = typeof source.profile?.columns === "object" && source.profile.columns !== null
    ? source.profile.columns as Record<string, Record<string, unknown>>
    : {};
  return source.columns.map((column) => {
    const profile = columns[column] || {};
    const nullRatio = typeof profile.null_ratio === "number" ? `${Math.round(profile.null_ratio * 100)}%` : "-";
    const distinct = typeof profile.distinct_count === "number" ? String(profile.distinct_count) : "-";
    const type = typeof profile.logical_type === "string" ? profile.logical_type : "unknown";
    return { column, type, nullRatio, distinct };
  });
};

const asRecordList = (value: unknown): Record<string, unknown>[] => {
  return Array.isArray(value) ? value.filter((item): item is Record<string, unknown> => typeof item === "object" && item !== null && !Array.isArray(item)) : [];
};

const approvalFacts = (entry: ApprovalEntry): Array<[string, string]> => {
  const payload = entry.payload || {};
  const proposal = typeof payload.proposal === "object" && payload.proposal !== null ? payload.proposal as Record<string, unknown> : null;
  const plan = typeof payload.plan === "object" && payload.plan !== null ? payload.plan as Record<string, unknown> : null;
  const facts: Array<[string, string]> = [];

  const proposalVersion = payload.proposal_version;
  if (typeof proposalVersion === "number") facts.push(["proposal v", String(proposalVersion)]);

  const materializationVersion = payload.materialization_proposal_version;
  if (typeof materializationVersion === "number") facts.push(["materialization v", String(materializationVersion)]);

  const proposalId = payload.proposal_id;
  if (typeof proposalId === "string") facts.push(["proposal", proposalId]);

  const materializationProposalId = payload.materialization_proposal_id;
  if (typeof materializationProposalId === "string") facts.push(["materialization proposal", materializationProposalId]);

  const proposalColumns = proposal ? countArray(proposal.canonical_columns) ?? countArray(proposal.column_groups) : null;
  if (proposalColumns !== null) facts.push(["proposal columns", String(proposalColumns)]);

  const materializedColumns = plan ? countArray(plan.canonical_columns) ?? countArray(plan.mappings) : null;
  if (materializedColumns !== null) facts.push(["materialized columns", String(materializedColumns)]);

  return facts;
};

const chartLabel = (chart: Record<string, unknown>): string => {
  const dimension = typeof chart.dimension === "string" ? chart.dimension : "category";
  const measure = typeof chart.measure === "string" ? chart.measure : "count";
  return `${measure} by ${dimension}`;
};

const chartRows = (table: AnalyticsTable, chart: Record<string, unknown>): Array<{ label: string; value: number }> => {
  const dimension = typeof chart.dimension === "string" ? chart.dimension : "";
  const values = table.categorical_top_values[dimension] || Object.values(table.categorical_top_values)[0] || [];
  return values.map((item) => ({ label: item.value, value: item.count }));
};

const retryGuidance = (run: MaterializationRun): string => {
  const value = run.plan?.retry_guidance;
  return typeof value === "string" ? value : "";
};

function App() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [selectedId, setSelectedId] = useState<string>("");
  const [detail, setDetail] = useState<Dataset | null>(null);
  const [files, setFiles] = useState<FileList | null>(null);
  const [datasetName, setDatasetName] = useState("sales-analysis");
  const [feedback, setFeedback] = useState("");
  const [question, setQuestion] = useState("部署別の売上合計を見たい");
  const [sqlOverride, setSqlOverride] = useState("");
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [materializationProposal, setMaterializationProposal] = useState<MaterializationProposal | null>(null);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [approvals, setApprovals] = useState<ApprovalEntry[]>([]);
  const [proposalHistory, setProposalHistory] = useState<ProposalHistoryEntry[]>([]);
  const [materializationProposalHistory, setMaterializationProposalHistory] = useState<MaterializationProposalHistoryEntry[]>([]);
  const [analytics, setAnalytics] = useState<Analytics | null>(null);
  const [settings, setSettings] = useState<AppSettings | null>(null);
  const [settingsModel, setSettingsModel] = useState("gpt-4.1-mini");
  const [settingsKey, setSettingsKey] = useState("");
  const [settingsLlmEnabled, setSettingsLlmEnabled] = useState(false);
  const [diagnostics, setDiagnostics] = useState<Diagnostics | null>(null);
  const [busy, setBusy] = useState<string>("");
  const [error, setError] = useState<string>("");

  const selected = useMemo(() => datasets.find((dataset) => dataset.id === selectedId), [datasets, selectedId]);

  const refresh = async (datasetId = selectedId) => {
    const loadedSettings = await api<AppSettings>("/settings");
    setDiagnostics(await api<Diagnostics>("/diagnostics"));
    setSettings(loadedSettings);
    setSettingsModel(loadedSettings.openai_model);
    setSettingsLlmEnabled(loadedSettings.llm_enabled);
    const list = await api<Dataset[]>("/datasets");
    setDatasets(list);
    let historyDatasetId = datasetId;
    if (datasetId) {
      const next = await api<Dataset>(`/datasets/${datasetId}`);
      setDetail(next);
      setSelectedId(datasetId);
      setApprovals(await api<ApprovalEntry[]>(`/datasets/${datasetId}/approvals`));
      setProposalHistory(await api<ProposalHistoryEntry[]>(`/datasets/${datasetId}/proposals`));
      setMaterializationProposalHistory(await api<MaterializationProposalHistoryEntry[]>(`/datasets/${datasetId}/materialization-proposals`));
    } else if (list[0]) {
      historyDatasetId = list[0].id;
      setSelectedId(historyDatasetId);
      setDetail(await api<Dataset>(`/datasets/${historyDatasetId}`));
      setApprovals(await api<ApprovalEntry[]>(`/datasets/${historyDatasetId}/approvals`));
      setProposalHistory(await api<ProposalHistoryEntry[]>(`/datasets/${historyDatasetId}/proposals`));
      setMaterializationProposalHistory(await api<MaterializationProposalHistoryEntry[]>(`/datasets/${historyDatasetId}/materialization-proposals`));
    } else {
      setDetail(null);
      setApprovals([]);
      setProposalHistory([]);
      setMaterializationProposalHistory([]);
    }
    setHistory(historyDatasetId ? await api<HistoryEntry[]>(`/datasets/${historyDatasetId}/query-history`) : []);
  };

  useEffect(() => {
    refresh().catch((exc: Error) => setError(exc.message));
  }, []);

  useEffect(() => {
    if (!selectedId) return;
    setQueryResult(null);
    setSqlOverride("");
    api<Dataset>(`/datasets/${selectedId}`).then(setDetail).catch((exc: Error) => setError(exc.message));
    api<ApprovalEntry[]>(`/datasets/${selectedId}/approvals`).then(setApprovals).catch((exc: Error) => setError(exc.message));
    api<ProposalHistoryEntry[]>(`/datasets/${selectedId}/proposals`).then(setProposalHistory).catch((exc: Error) => setError(exc.message));
    api<MaterializationProposalHistoryEntry[]>(`/datasets/${selectedId}/materialization-proposals`).then(setMaterializationProposalHistory).catch((exc: Error) => setError(exc.message));
    api<HistoryEntry[]>(`/datasets/${selectedId}/query-history`).then(setHistory).catch((exc: Error) => setError(exc.message));
  }, [selectedId]);

  const run = async (label: string, action: () => Promise<void>) => {
    setBusy(label);
    setError("");
    try {
      await action();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setBusy("");
    }
  };

  const upload = () => run("upload", async () => {
    if (!files || files.length === 0) throw new Error("ファイルを選択してください");
    const form = new FormData();
    Array.from(files).forEach((file) => form.append("files", file));
    form.append("name", datasetName);
    const uploaded = await api<Dataset>("/datasets/upload", { method: "POST", body: form });
    await refresh(uploaded.id);
  });

  const saveSettings = () => run("settings", async () => {
    const body: Record<string, unknown> = {
      openai_model: settingsModel,
      llm_enabled: settingsLlmEnabled
    };
    if (settingsKey.trim()) body.openai_api_key = settingsKey.trim();
    const updated = await api<AppSettings>("/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    setSettings(updated);
    setSettingsModel(updated.openai_model);
    setSettingsLlmEnabled(updated.llm_enabled);
    setSettingsKey("");
  });

  const clearApiKey = () => run("settings", async () => {
    const updated = await api<AppSettings>("/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clear_openai_api_key: true })
    });
    setSettings(updated);
    setSettingsLlmEnabled(updated.llm_enabled);
    setSettingsKey("");
  });

  const propose = () => run("proposal", async () => {
    if (!selectedId) return;
    const response = await api<{ dataset_id: string; proposal_id: string; version: number; proposal: Record<string, unknown> }>(`/datasets/${selectedId}/proposal`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ feedback })
    });
    setDetail((current) => current ? {
      ...current,
      proposal: response.proposal,
      proposal_id: response.proposal_id,
      proposal_version: response.version,
      status: "proposed"
    } : current);
    await refresh(selectedId);
  });

  const approve = () => run("approval", async () => {
    if (!selectedId) return;
    await api(`/datasets/${selectedId}/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ proposal_id: detail?.proposal_id, proposal_version: detail?.proposal_version, notes: feedback })
    });
    await refresh(selectedId);
  });

  const approveIntegration = () => run("integration-approval", async () => {
    if (!selectedId) return;
    await api<Dataset>(`/datasets/${selectedId}/proposal/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ proposal_id: detail?.proposal_id, proposal_version: detail?.proposal_version, notes: feedback })
    });
    setMaterializationProposal(null);
    await refresh(selectedId);
  });

  const proposeMaterialization = () => run("materialization-proposal", async () => {
    if (!selectedId) return;
    const response = await api<MaterializationProposal>(`/datasets/${selectedId}/materialization-proposal`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ feedback })
    });
    setMaterializationProposal(response);
    setMaterializationProposalHistory(await api<MaterializationProposalHistoryEntry[]>(`/datasets/${selectedId}/materialization-proposals`));
  });

  const approveMaterialization = () => run("materialization-approval", async () => {
    if (!selectedId || !materializationProposal) return;
    await api(`/datasets/${selectedId}/materialization-proposal/${materializationProposal.materialization_proposal_id}/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        materialization_proposal_version: materializationProposal.version,
        notes: feedback
      })
    });
    setMaterializationProposal(null);
    await refresh(selectedId);
  });

  const retryMaterialization = () => run("materialization-retry", async () => {
    if (!selectedId || !materializationProposal) return;
    const response = await api<MaterializationProposal>(
      `/datasets/${selectedId}/materialization-proposal/${materializationProposal.materialization_proposal_id}/retry`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ feedback })
      }
    );
    setMaterializationProposal(response);
    await refresh(selectedId);
  });

  const query = () => run("query", async () => {
    const response = await api<QueryResult>("/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dataset_id: selectedId,
        question,
        ...(sqlOverride.trim() ? { sql: sqlOverride.trim() } : {})
      })
    });
    setQueryResult(response);
    setSqlOverride(response.sql);
    setHistory(await api<HistoryEntry[]>(`/datasets/${selectedId}/query-history`));
  });

  const loadAnalytics = () => run("analytics", async () => {
    if (!selectedId) return;
    setAnalytics(await api<Analytics>(`/datasets/${selectedId}/analytics`));
  });

  const exportEvidence = () => run("export", async () => {
    if (!selectedId) return;
    const payload = await api<Record<string, unknown>>(`/datasets/${selectedId}/export`);
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${detail?.name || selectedId}-evidence.json`;
    link.click();
    URL.revokeObjectURL(url);
  });

  return (
    <main className="shell">
      <aside className="sidebar">
        <div>
          <p className="eyebrow">db-auto-pilot</p>
          <h1>Spreadsheet to SQLite cockpit</h1>
        </div>
        <section className="panel">
          <h2>Dataset</h2>
          <input value={datasetName} onChange={(event) => setDatasetName(event.target.value)} />
          <input type="file" multiple accept=".csv,.xls,.xlsx,.xlsm" onChange={(event) => setFiles(event.target.files)} />
          <button onClick={upload} disabled={busy === "upload"}>Upload</button>
        </section>
        <section className="panel">
          <h2>Existing</h2>
          <div className="dataset-list">
            {datasets.map((dataset) => (
              <button className={dataset.id === selectedId ? "selected" : ""} key={dataset.id} onClick={() => setSelectedId(dataset.id)}>
                <span>{dataset.name}</span>
                <small>{dataset.status} / {dataset.source_count} sources</small>
              </button>
            ))}
          </div>
        </section>
        <section className="panel">
          <h2>Settings</h2>
          <label className="check-row">
            <input type="checkbox" checked={settingsLlmEnabled} onChange={(event) => setSettingsLlmEnabled(event.target.checked)} />
            <span>LLM proposals</span>
          </label>
          <input value={settingsModel} onChange={(event) => setSettingsModel(event.target.value)} placeholder="OpenAI model" />
          <input value={settingsKey} onChange={(event) => setSettingsKey(event.target.value)} placeholder={settings?.openai_api_key_configured ? "API key configured" : "OpenAI API key"} type="password" />
          <button onClick={saveSettings} disabled={busy === "settings"}>Save settings</button>
          <button onClick={clearApiKey} disabled={busy === "settings" || !settings?.openai_api_key_configured}>Clear API key</button>
          {settings && (
            <dl className="settings-facts">
              <dt>policy</dt><dd>{settings.llm_data_policy}</dd>
              <dt>key</dt><dd>{settings.openai_api_key_configured ? "configured" : "missing"}</dd>
              <dt>upload MB</dt><dd>{settings.max_upload_mb}</dd>
              <dt>query rows</dt><dd>{settings.query_row_limit}</dd>
              <dt>materialization</dt><dd>{settings.max_materialization_rows} rows / {settings.max_materialization_columns} cols</dd>
              <dt>CORS</dt><dd>{settings.cors_allow_origins.join(", ") || "none"}</dd>
            </dl>
          )}
        </section>
        <section className="panel">
          <h2>Diagnostics</h2>
          {diagnostics ? (
            <dl className="settings-facts">
              <dt>status</dt><dd>{diagnostics.status}</dd>
              <dt>app</dt><dd>{diagnostics.app_version}</dd>
              <dt>schema</dt><dd>{diagnostics.schema_version} / db {diagnostics.database_user_version}</dd>
              <dt>ready</dt><dd>{diagnostics.database_ready ? "database" : "no database"} / {diagnostics.uploads_dir_ready ? "uploads" : "no uploads"}</dd>
              <dt>integrity</dt><dd>{diagnostics.database_integrity} / fk {diagnostics.foreign_key_violations}</dd>
              <dt>backups</dt><dd>{diagnostics.migration_backup_count}{diagnostics.latest_migration_backup ? ` / ${diagnostics.latest_migration_backup}` : ""}</dd>
              <dt>datasets</dt><dd>{diagnostics.counts.datasets ?? 0}</dd>
              <dt>queries</dt><dd>{diagnostics.counts.query_history ?? 0}</dd>
            </dl>
          ) : <p className="empty">No diagnostics</p>}
        </section>
      </aside>

      <section className="workspace">
        {error && <div className="error">{error}</div>}
        <div className="topbar">
          <div>
            <p className="eyebrow">Selected</p>
            <h2>{detail?.name || selected?.name || "No dataset"}</h2>
          </div>
          <div className="topbar-actions">
            <button onClick={exportEvidence} disabled={!selectedId || busy === "export"}>Export evidence</button>
            <span className="status">{detail?.status || "idle"}</span>
          </div>
        </div>

        <div className="grid">
          <section className="panel">
            <h2>Sources</h2>
            <div className="cards">
              {(detail?.sources || []).map((source) => (
                <article key={source.id} className="source-card">
                  <strong>{source.filename}{source.sheet_name ? ` / ${source.sheet_name}` : ""}</strong>
                  <small>{source.rows} rows</small>
                  <p>{source.columns.join(", ")}</p>
                  <div className="profile-grid">
                    {sourceProfileRows(source).slice(0, 12).map((row) => (
                      <React.Fragment key={row.column}>
                        <span>{row.column}</span>
                        <small>{row.type}</small>
                        <small>null {row.nullRatio}</small>
                        <small>{row.distinct} distinct</small>
                      </React.Fragment>
                    ))}
                  </div>
                </article>
              ))}
            </div>
          </section>

          <section className="panel">
            <h2>Proposal and approval</h2>
            <textarea value={feedback} onChange={(event) => setFeedback(event.target.value)} placeholder="例: 顧客IDとcustomer_codeを同じキーとして扱う" />
            <div className="actions">
              <button onClick={propose} disabled={!selectedId || busy === "proposal"}>Generate proposal</button>
              <button onClick={approveIntegration} disabled={!selectedId || !detail?.proposal_id || busy === "integration-approval"}>Approve integration</button>
              <button onClick={proposeMaterialization} disabled={!selectedId || (detail?.status !== "approved" && detail?.status !== "materialized") || busy === "materialization-proposal"}>Generate materialization</button>
              <button onClick={approveMaterialization} disabled={!selectedId || !materializationProposal || busy === "materialization-approval"}>Approve materialization</button>
              <button onClick={retryMaterialization} disabled={!selectedId || !materializationProposal || busy === "materialization-retry"}>Retry materialization</button>
              <button onClick={approve} disabled={!selectedId || !detail?.proposal_id || busy === "approval"}>Approve and materialize</button>
            </div>
            {proposalSummary(detail?.proposal).length > 0 && (
              <dl className="summary-strip">
                {proposalSummary(detail?.proposal).map(([label, value]) => (
                  <React.Fragment key={label}>
                    <dt>{label}</dt>
                    <dd>{value}</dd>
                  </React.Fragment>
                ))}
              </dl>
            )}
            {proposalChanges(detail?.proposal).length > 0 && (
              <ul className="change-list">
                {proposalChanges(detail?.proposal).map((change) => <li key={change}>{change}</li>)}
              </ul>
            )}
            {proposalChanges(materializationProposal?.plan).length > 0 && (
              <ul className="change-list materialization">
                {proposalChanges(materializationProposal?.plan).map((change) => <li key={change}>{change}</li>)}
              </ul>
            )}
            <pre>{JSON.stringify(materializationProposal?.plan || detail?.proposal || detail?.materialization || {}, null, 2)}</pre>
          </section>

          <section className="panel">
            <h2>Proposal revisions</h2>
            <div className="history">
              {proposalHistory.map((entry) => (
                <article key={entry.id}>
                  <strong>v{entry.version}</strong>
                  <small>{new Date(entry.created_at).toLocaleString()}</small>
                  {entry.feedback && <p>{entry.feedback}</p>}
                  <ul className="change-list compact">
                    {proposalChanges(entry.proposal).map((change) => <li key={change}>{change}</li>)}
                  </ul>
                </article>
              ))}
              {proposalHistory.length === 0 && <p className="empty">No proposal revisions</p>}
            </div>
          </section>

          <section className="panel">
            <h2>Materialization proposals</h2>
            <div className="history">
              {materializationProposalHistory.map((entry) => (
                <article key={entry.id}>
                  <strong>v{entry.version}</strong>
                  <small>{new Date(entry.created_at).toLocaleString()}</small>
                  {entry.feedback && <p>{entry.feedback}</p>}
                  <ul className="change-list compact materialization">
                    {proposalChanges(entry.plan).map((change) => <li key={change}>{change}</li>)}
                  </ul>
                </article>
              ))}
              {materializationProposalHistory.length === 0 && <p className="empty">No materialization proposals</p>}
            </div>
          </section>

          <section className="panel wide">
            <h2>Natural-language SQL</h2>
            <div className="query-row">
              <input value={question} onChange={(event) => setQuestion(event.target.value)} />
              <button onClick={query} disabled={!selectedId || busy === "query"}>Run</button>
              <button onClick={loadAnalytics} disabled={!selectedId || busy === "analytics"}>Analytics</button>
            </div>
            <textarea className="sql-input" value={sqlOverride} onChange={(event) => setSqlOverride(event.target.value)} placeholder="Optional governed SELECT SQL. Leave blank to generate from the question." />
            {queryResult && (
              <>
                <p className="explanation">{queryResult.explanation}</p>
                <pre>{queryResult.sql}</pre>
                <ResultTable rows={queryResult.rows} columns={queryResult.columns} />
              </>
            )}
          </section>

          <section className="panel">
            <h2>History</h2>
            <div className="history">
              {history.map((entry) => (
                <article key={entry.id}>
                  <strong>{entry.question}</strong>
                  <small>{entry.row_count} rows / {entry.target_mode} / {new Date(entry.created_at).toLocaleString()}</small>
                  <p>{entry.explanation}</p>
                  <small>{entry.materialized_table || "no table"} / {entry.materialization_run_id || "no run"}</small>
                  <small>
                    {entry.proposal_id || "no proposal"} v{entry.proposal_version ?? "-"} / {entry.materialization_proposal_id || "no materialization proposal"} v{entry.materialization_proposal_version ?? "-"}
                  </small>
                  <code>{entry.sql}</code>
                  {entry.columns.length > 0 && <small>{entry.columns.join(", ")}</small>}
                  {entry.result_preview.length > 0 && <ResultTable rows={entry.result_preview} columns={entry.columns} />}
                </article>
              ))}
            </div>
          </section>

          <section className="panel">
            <h2>Approvals</h2>
            <div className="history">
              {approvals.map((entry) => (
                <article key={entry.id}>
                  <strong>{entry.decision_type}</strong>
                  <small>{new Date(entry.created_at).toLocaleString()}</small>
                  <p>{entry.notes}</p>
                  <small>{entry.proposal_id || "no proposal id"}</small>
                  <dl className="facts">
                    {approvalFacts(entry).map(([label, value]) => (
                      <React.Fragment key={label}>
                        <dt>{label}</dt>
                        <dd>{value}</dd>
                      </React.Fragment>
                    ))}
                  </dl>
                </article>
              ))}
            </div>
          </section>

          <section className="panel">
            <h2>Materialization runs</h2>
            <div className="history">
              {(detail?.materialization_runs || []).map((run) => (
                <article key={run.id} className={run.status === "failed" ? "run-failed" : ""}>
                  <strong>{run.status}</strong>
                  <small>{run.table_name} / {new Date(run.created_at).toLocaleString()}</small>
                  <small>{run.row_count} rows / {run.column_count} columns</small>
                  {run.error && <p>{run.error}</p>}
                  {retryGuidance(run) && <small>{retryGuidance(run)}</small>}
                </article>
              ))}
              {(detail?.materialization_runs || []).length === 0 && <p className="empty">No materialization runs</p>}
            </div>
          </section>

          <section className="panel">
            <h2>Lineage and quality</h2>
            <div className="history">
              {asRecordList(detail?.materialization?.quality_warnings).map((warning, index) => (
                <article key={`warning-${index}`} className="run-failed">
                  <strong>{String(warning.code || "quality warning")}</strong>
                  <small>{String(warning.severity || "warning")} / {String(warning.output_column || warning.table || "dataset")}</small>
                  <p>{String(warning.message || "")}</p>
                </article>
              ))}
              {asRecordList(detail?.materialization?.lineage).slice(0, 24).map((entry, index) => (
                <article key={`lineage-${index}`}>
                  <strong>{String(entry.output_column || "output")}</strong>
                  <small>{String(entry.source_table || "source")} / {String(entry.source_column || "column")} / {String(entry.action || "keep")}</small>
                </article>
              ))}
              {!detail?.materialization && <p className="empty">No materialization evidence</p>}
            </div>
          </section>

          <section className="panel">
            <h2>Analytics</h2>
            {analytics && (
              <small>{analytics.materialized_table || "no table"} / {analytics.materialization_run_id || "no run"}</small>
            )}
            {analytics?.tables.map((table) => (
              <article key={table.table_name} className="analytics">
                <strong>{table.table_name}</strong>
                <small>{table.row_count} rows / {table.column_count} columns</small>
                {Object.entries(table.numeric_summaries).map(([column, summary]) => (
                  <p key={column}>{column}: sum {summary.sum?.toLocaleString()} / avg {summary.mean?.toLocaleString()}</p>
                ))}
                {table.recommended_charts.map((chart, index) => (
                  <MiniBarChart key={`${chartLabel(chart)}-${index}`} title={chartLabel(chart)} rows={chartRows(table, chart)} />
                ))}
              </article>
            ))}
          </section>
        </div>
      </section>
    </main>
  );
}

function MiniBarChart({ title, rows }: { title: string; rows: Array<{ label: string; value: number }> }) {
  if (rows.length === 0) return null;
  const max = Math.max(...rows.map((row) => row.value), 1);
  return (
    <div className="mini-chart">
      <strong>{title}</strong>
      {rows.slice(0, 8).map((row) => (
        <div className="bar-row" key={row.label}>
          <span>{row.label}</span>
          <div className="bar-track">
            <div className="bar-fill" style={{ width: `${Math.max((row.value / max) * 100, 4)}%` }} />
          </div>
          <small>{row.value.toLocaleString()}</small>
        </div>
      ))}
    </div>
  );
}

function ResultTable({ rows, columns }: { rows: Record<string, unknown>[]; columns: string[] }) {
  const displayColumns = columns.length > 0 ? columns : rows.length > 0 ? Object.keys(rows[0]) : [];
  if (displayColumns.length === 0) return <p className="empty">No rows</p>;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>{displayColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex}>{displayColumns.map((column) => <td key={column}>{String(row[column] ?? "")}</td>)}</tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td colSpan={displayColumns.length} className="empty-cell">No rows</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

createRoot(document.getElementById("root") as HTMLElement).render(<App />);
