import { useEffect, useState } from "react";
import {
  approveProposal,
  generateProposal,
  getAppSettings,
  getDataset,
  getQueryHistory,
  listDatasets,
  queryDataset,
  reviseProposal,
  updateAppSettings,
  uploadDataset,
} from "./api";
import type { AppSettings, DatasetDetail, DatasetSummary, ProposalResponse, QueryHistoryEntry, QueryResponse } from "./types";

function formatDate(value: string) {
  return new Date(value).toLocaleString("ja-JP");
}

export default function App() {
  const [datasets, setDatasets] = useState<DatasetSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<DatasetDetail | null>(null);
  const [history, setHistory] = useState<QueryHistoryEntry[]>([]);
  const [files, setFiles] = useState<File[]>([]);
  const [feedback, setFeedback] = useState("");
  const [question, setQuestion] = useState("");
  const [queryMode, setQueryMode] = useState<"raw" | "merged">("merged");
  const [queryResult, setQueryResult] = useState<QueryResponse | null>(null);
  const [appSettings, setAppSettings] = useState<AppSettings>({ api_key: null, endpoint: null, model: null });
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function refreshDatasets(preserveId?: string | null) {
    const next = await listDatasets();
    setDatasets(next);
    const fallback = preserveId ?? selectedId ?? next[0]?.id ?? null;
    if (fallback) {
      setSelectedId(fallback);
      const datasetDetail = await getDataset(fallback);
      setDetail(datasetDetail);
      setHistory(await getQueryHistory(fallback));
    }
  }

  useEffect(() => {
    void refreshDatasets();
    void loadSettings();
  }, []);

  async function loadSettings() {
    setAppSettings(await getAppSettings());
  }

  async function selectDataset(id: string) {
    setSelectedId(id);
    setDetail(await getDataset(id));
    setHistory(await getQueryHistory(id));
    setQueryResult(null);
  }

  async function withTask(label: string, task: () => Promise<void>) {
    setBusy(label);
    setError(null);
    try {
      await task();
    } catch (taskError) {
      setError(taskError instanceof Error ? taskError.message : String(taskError));
    } finally {
      setBusy(null);
    }
  }

  async function handleUpload() {
    if (!files.length) return;
    await withTask("Uploading dataset", async () => {
      const dataset = await uploadDataset(files);
      setFiles([]);
      await refreshDatasets(dataset.dataset.id);
    });
  }

  async function handleGenerateProposal() {
    if (!selectedId) return;
    await withTask("Generating proposal", async () => {
      await generateProposal(selectedId);
      await selectDataset(selectedId);
    });
  }

  async function handleReviseProposal() {
    if (!selectedId || !feedback.trim()) return;
    await withTask("Revising proposal", async () => {
      await reviseProposal(selectedId, feedback);
      setFeedback("");
      await selectDataset(selectedId);
    });
  }

  async function handleApproveProposal(proposal: ProposalResponse) {
    if (!selectedId) return;
    await withTask("Approving proposal", async () => {
      await approveProposal(selectedId, proposal.id);
      await selectDataset(selectedId);
    });
  }

  async function handleQuery() {
    if (!selectedId || !question.trim()) return;
    await withTask("Running query", async () => {
      const result = await queryDataset(selectedId, queryMode, question);
      setQueryResult(result);
      setHistory(await getQueryHistory(selectedId));
    });
  }

  async function handleSaveSettings() {
    await withTask("Saving settings", async () => {
      const saved = await updateAppSettings(appSettings);
      setAppSettings(saved);
    });
  }

  const proposal = detail?.latest_proposal;
  const reviewItems = proposal?.proposal.review_items ?? [];
  const normalizationActions = proposal?.proposal.normalization_actions ?? [];
  const schemaDraft = proposal?.proposal.schema_draft ?? [];
  const approvalDecisions = detail?.approval_decisions ?? [];

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_rgba(249,115,22,0.18),_transparent_25%),radial-gradient(circle_at_top_right,_rgba(56,189,248,0.16),_transparent_22%),linear-gradient(180deg,_#020617_0%,_#0f172a_55%,_#111827_100%)] text-slate-100">
      <div className="mx-auto flex max-w-7xl flex-col gap-6 px-4 py-6 lg:px-8">
        <header className="rounded-[32px] border border-white/10 bg-white/5 p-6 shadow-panel backdrop-blur">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-3xl">
              <p className="text-sm uppercase tracking-[0.3em] text-orange-300">db-auto-pilot</p>
              <h1 className="mt-2 text-4xl font-bold leading-tight text-white">Excel を、相談しながら検索できるデータセットに変える。</h1>
              <p className="mt-3 text-sm leading-6 text-slate-300">
                複数の Excel / CSV をアップロードし、LLM の統合提案を確認して承認すると、原本テーブルと統合後テーブルの両方を検索できます。
              </p>
            </div>
            <div className="rounded-3xl border border-orange-300/30 bg-orange-400/10 px-4 py-3 text-sm text-orange-100">
              {busy ? `Status: ${busy}` : "Status: Idle"}
            </div>
          </div>
        </header>

        {error ? <div className="rounded-2xl border border-rose-400/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">{error}</div> : null}

        <div className="grid gap-6 lg:grid-cols-[320px_minmax(0,1fr)]">
          <aside className="space-y-6">
            <section className="rounded-[28px] border border-white/10 bg-slate-900/80 p-5 shadow-panel">
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold">Datasets</h2>
                <button
                  className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300 hover:border-orange-300/40 hover:text-white"
                  onClick={() => void refreshDatasets(selectedId)}
                >
                  Refresh
                </button>
              </div>
              <div className="mt-4 space-y-2">
                {datasets.map((dataset) => (
                  <button
                    key={dataset.id}
                    className={`w-full rounded-2xl border px-4 py-3 text-left transition ${
                      selectedId === dataset.id
                        ? "border-orange-300/40 bg-orange-400/10"
                        : "border-white/10 bg-white/5 hover:border-white/20"
                    }`}
                    onClick={() => void selectDataset(dataset.id)}
                  >
                    <div className="font-medium text-white">{dataset.name}</div>
                    <div className="mt-1 text-xs text-slate-400">{dataset.status}</div>
                    <div className="mt-1 text-xs text-slate-500">{formatDate(dataset.created_at)}</div>
                  </button>
                ))}
                {!datasets.length ? <div className="rounded-2xl border border-dashed border-white/10 p-4 text-sm text-slate-400">No datasets yet.</div> : null}
              </div>
            </section>

            <section className="rounded-[28px] border border-white/10 bg-slate-900/80 p-5 shadow-panel">
              <h2 className="text-lg font-semibold">Upload</h2>
              <p className="mt-2 text-sm text-slate-400">複数ファイルをまとめて 1 データセットとして取り込みます。</p>
              <input
                className="mt-4 block w-full text-sm text-slate-300 file:mr-4 file:rounded-full file:border-0 file:bg-orange-400 file:px-4 file:py-2 file:text-sm file:font-semibold file:text-white hover:file:bg-orange-300"
                type="file"
                multiple
                accept=".xlsx,.xls,.csv"
                onChange={(event) => setFiles(Array.from(event.target.files ?? []))}
              />
              <button
                className="mt-4 w-full rounded-2xl bg-orange-400 px-4 py-3 font-medium text-slate-950 transition hover:bg-orange-300 disabled:cursor-not-allowed disabled:opacity-40"
                disabled={!files.length || Boolean(busy)}
                onClick={() => void handleUpload()}
              >
                Create Dataset
              </button>
            </section>

            <section className="rounded-[28px] border border-white/10 bg-slate-900/80 p-5 shadow-panel">
              <h2 className="text-lg font-semibold">LLM Settings</h2>
              <p className="mt-2 text-sm text-slate-400">OpenAI 互換 API の接続先をここで保存します。保存後の proposal と query から反映されます。</p>
              <div className="mt-4 space-y-3">
                <div>
                  <label className="text-xs uppercase tracking-[0.2em] text-slate-500">API Key</label>
                  <input
                    className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-orange-300/40"
                    type="password"
                    placeholder="sk-..."
                    value={appSettings.api_key ?? ""}
                    onChange={(event) => setAppSettings((current) => ({ ...current, api_key: event.target.value || null }))}
                  />
                </div>
                <div>
                  <label className="text-xs uppercase tracking-[0.2em] text-slate-500">Endpoint</label>
                  <input
                    className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-orange-300/40"
                    type="text"
                    placeholder="https://api.openai.com/v1"
                    value={appSettings.endpoint ?? ""}
                    onChange={(event) => setAppSettings((current) => ({ ...current, endpoint: event.target.value || null }))}
                  />
                </div>
                <div>
                  <label className="text-xs uppercase tracking-[0.2em] text-slate-500">Model</label>
                  <input
                    className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-orange-300/40"
                    type="text"
                    placeholder="gpt-4.1-mini"
                    value={appSettings.model ?? ""}
                    onChange={(event) => setAppSettings((current) => ({ ...current, model: event.target.value || null }))}
                  />
                </div>
              </div>
              <button
                className="mt-4 w-full rounded-2xl border border-sky-300/30 bg-sky-400/10 px-4 py-3 text-sm font-medium text-sky-100 transition hover:bg-sky-400/20 disabled:opacity-40"
                disabled={Boolean(busy)}
                onClick={() => void handleSaveSettings()}
              >
                Save LLM Settings
              </button>
            </section>
          </aside>

          <main className="space-y-6">
            {detail ? (
              <>
                <section className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
                  <div className="rounded-[28px] border border-white/10 bg-white/5 p-6 shadow-panel backdrop-blur">
                    <div className="flex items-start justify-between gap-4">
                      <div>
                        <p className="text-xs uppercase tracking-[0.3em] text-sky-300">Proposal Review</p>
                        <h2 className="mt-2 text-2xl font-semibold text-white">{detail.dataset.name}</h2>
                      </div>
                      <div className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300">{detail.dataset.status}</div>
                    </div>

                    <div className="mt-5 grid gap-4 md:grid-cols-2">
                      <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                        <div className="text-sm font-medium text-white">Source files</div>
                        <div className="mt-3 space-y-3 text-sm text-slate-300">
                          {detail.source_files.map((file) => (
                            <div key={file.id} className="rounded-xl border border-white/10 bg-white/5 p-3">
                              <div>{file.filename}</div>
                              <div className="mt-1 text-xs text-slate-500">
                                {file.file_type.toUpperCase()} / {file.sheet_count} sheet(s)
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                      <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                        <div className="text-sm font-medium text-white">Next action</div>
                        <div className="mt-3 text-sm text-slate-400">
                          {proposal ? "提案を確認して、必要なら補足を返してから承認します。" : "まだ提案がありません。まず Generate Proposal を実行してください。"}
                        </div>
                        <button
                          className="mt-4 w-full rounded-2xl border border-sky-300/30 bg-sky-400/10 px-4 py-3 text-sm font-medium text-sky-100 transition hover:bg-sky-400/20 disabled:opacity-40"
                          disabled={Boolean(busy)}
                          onClick={() => void handleGenerateProposal()}
                        >
                          Generate Proposal
                        </button>
                      </div>
                    </div>

                    {proposal ? (
                      <div className="mt-6 space-y-4">
                        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                          <div className="text-sm font-medium text-white">LLM notes</div>
                          <ul className="mt-3 space-y-2 text-sm text-slate-300">
                            {proposal.proposal.notes.map((note) => (
                              <li key={note} className="rounded-xl border border-white/10 bg-white/5 px-3 py-2">
                                {note}
                              </li>
                            ))}
                          </ul>
                        </div>

                        <div className="grid gap-4 xl:grid-cols-2">
                          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                            <div className="text-sm font-medium text-white">Review items</div>
                            <div className="mt-3 space-y-3">
                              {reviewItems.map((item) => (
                                <div key={`${item.component_id}-${item.canonical_name}-${item.type}`} className="rounded-xl border border-white/10 bg-white/5 p-3">
                                  <div className="flex items-center justify-between gap-3">
                                    <div className="font-medium text-white">{item.canonical_name}</div>
                                    <div className="rounded-full bg-amber-400/15 px-3 py-1 text-xs text-amber-100">{item.type}</div>
                                  </div>
                                  <p className="mt-2 text-xs text-slate-400">{item.message}</p>
                                  <div className="mt-3 flex flex-wrap gap-2">
                                    {item.matches.map((match) => (
                                      <span key={`${match.source_table}-${match.source_column}`} className="rounded-full border border-white/10 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
                                        {match.display_name} <span className="text-slate-500">@ {match.source_table}</span>
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              ))}
                              {!reviewItems.length ? <div className="rounded-xl border border-dashed border-white/10 p-3 text-sm text-slate-500">No blocking review items in the current draft.</div> : null}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                            <div className="text-sm font-medium text-white">Normalization actions</div>
                            <div className="mt-3 space-y-3">
                              {normalizationActions.slice(0, 8).map((action) => (
                                <div key={`${action.table_name}-${action.source_column}`} className="rounded-xl border border-white/10 bg-white/5 p-3">
                                  <div className="text-sm text-white">{action.source_column} → {action.normalized_column}</div>
                                  <div className="mt-1 text-xs text-slate-500">{action.display_name}</div>
                                  <div className="mt-2 flex flex-wrap gap-2">
                                    {action.actions.map((step) => (
                                      <span key={step} className="rounded-full border border-white/10 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
                                        {step}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        </div>

                        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                          <div className="text-sm font-medium text-white">Schema draft</div>
                          <div className="mt-3 space-y-4">
                            {schemaDraft.map((draft) => (
                              <div key={draft.component_id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                                <div className="font-medium text-white">{draft.display_name}</div>
                                <div className="mt-1 text-xs text-slate-500">{draft.source_tables.join(", ")}</div>
                                <div className="mt-3 grid gap-3 md:grid-cols-2">
                                  {draft.columns.map((column) => (
                                    <div key={`${draft.component_id}-${column.name}`} className="rounded-xl border border-white/10 bg-slate-950/60 p-3">
                                      <div className="flex items-center justify-between gap-3">
                                        <div className="text-sm text-white">{column.name}</div>
                                        <div className="rounded-full border border-white/10 px-3 py-1 text-[11px] text-slate-300">{column.status}</div>
                                      </div>
                                      <div className="mt-2 text-xs text-slate-500">{column.logical_type} / {column.source_count} source(s)</div>
                                      <p className="mt-2 text-xs text-slate-400">{column.rationale}</p>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>

                        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                          <div className="flex items-center justify-between">
                            <div className="text-sm font-medium text-white">Merge candidates</div>
                            <div className="text-xs text-slate-500">v{proposal.version}</div>
                          </div>
                          <div className="mt-3 grid gap-3">
                            {proposal.proposal.column_mappings.map((mapping) => (
                              <div key={`${mapping.canonical_name}-${mapping.rationale}`} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                                <div className="flex items-center justify-between gap-3">
                                  <div className="font-medium text-white">{mapping.canonical_name}</div>
                                  <div
                                    className={`rounded-full px-3 py-1 text-xs ${
                                      mapping.decision === "merge"
                                        ? "bg-emerald-400/15 text-emerald-200"
                                        : mapping.decision === "uncertain"
                                          ? "bg-amber-400/15 text-amber-100"
                                          : "bg-slate-700/70 text-slate-200"
                                    }`}
                                  >
                                    {mapping.decision}
                                  </div>
                                </div>
                                <div className="mt-2 text-xs text-slate-400">confidence {mapping.confidence.toFixed(2)}</div>
                                <div className="mt-3 flex flex-wrap gap-2">
                                  {mapping.matches.map((match) => (
                                    <span key={`${match.source_table}-${match.source_column}-${match.display_name}`} className="rounded-full border border-white/10 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
                                      {match.display_name} <span className="text-slate-500">@ {match.source_table}:{match.source_column}</span>
                                    </span>
                                  ))}
                                </div>
                                <p className="mt-3 text-sm text-slate-400">{mapping.rationale}</p>
                              </div>
                            ))}
                          </div>
                        </div>

                        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                          <div className="text-sm font-medium text-white">Natural language feedback</div>
                          <textarea
                            className="mt-3 min-h-28 w-full rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-orange-300/40"
                            placeholder="例: X.xlsx の A列 と Y.xlsx の B列 は似ていますが、業務上は別概念なので統合しないでください"
                            value={feedback}
                            onChange={(event) => setFeedback(event.target.value)}
                          />
                          <div className="mt-4 flex flex-col gap-3 md:flex-row">
                            <button
                              className="rounded-2xl border border-orange-300/30 bg-orange-400/10 px-4 py-3 text-sm font-medium text-orange-100 transition hover:bg-orange-400/20 disabled:opacity-40"
                              disabled={!feedback.trim() || Boolean(busy)}
                              onClick={() => void handleReviseProposal()}
                            >
                              Revise Proposal
                            </button>
                            <button
                              className="rounded-2xl bg-emerald-400 px-4 py-3 text-sm font-medium text-slate-950 transition hover:bg-emerald-300 disabled:opacity-40"
                              disabled={Boolean(busy)}
                              onClick={() => void handleApproveProposal(proposal)}
                            >
                              Approve and Build DB
                            </button>
                          </div>
                        </div>
                      </div>
                    ) : null}
                  </div>

                  <section className="rounded-[28px] border border-white/10 bg-white/5 p-6 shadow-panel backdrop-blur">
                    <p className="text-xs uppercase tracking-[0.3em] text-emerald-300">Query Studio</p>
                    <h2 className="mt-2 text-2xl font-semibold text-white">自然言語で検索する</h2>
                    <div className="mt-5 flex gap-2 rounded-2xl border border-white/10 bg-slate-950/50 p-2">
                      {(["merged", "raw"] as const).map((mode) => (
                        <button
                          key={mode}
                          className={`flex-1 rounded-xl px-3 py-2 text-sm ${
                            queryMode === mode ? "bg-white text-slate-950" : "text-slate-400"
                          }`}
                          onClick={() => setQueryMode(mode)}
                        >
                          {mode === "merged" ? "統合後テーブル" : "原本テーブル"}
                        </button>
                      ))}
                    </div>
                    <textarea
                      className="mt-4 min-h-28 w-full rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-sky-300/40"
                      placeholder="例: 部署別の売上合計を見たい"
                      value={question}
                      onChange={(event) => setQuestion(event.target.value)}
                    />
                    <button
                      className="mt-4 w-full rounded-2xl bg-sky-400 px-4 py-3 font-medium text-slate-950 transition hover:bg-sky-300 disabled:opacity-40"
                      disabled={detail.dataset.status !== "approved" || !question.trim() || Boolean(busy)}
                      onClick={() => void handleQuery()}
                    >
                      Run Query
                    </button>

                    {queryResult ? (
                      <div className="mt-6 space-y-4">
                        <div className="rounded-2xl border border-white/10 bg-slate-950/60 p-4">
                          <div className="text-sm font-medium text-white">Generated SQL</div>
                          <pre className="mt-3 overflow-x-auto text-xs text-sky-200">{queryResult.sql}</pre>
                        </div>
                        <div className="rounded-2xl border border-white/10 bg-slate-950/60 p-4 text-sm text-slate-300">
                          {queryResult.explanation}
                        </div>
                        <div className="overflow-hidden rounded-2xl border border-white/10">
                          <div className="overflow-x-auto">
                            <table className="min-w-full divide-y divide-white/10 text-sm">
                              <thead className="bg-white/5">
                                <tr>
                                  {queryResult.columns.map((column) => (
                                    <th key={column} className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wide text-slate-400">
                                      {column}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody className="divide-y divide-white/5 bg-slate-950/50">
                                {queryResult.rows.map((row, rowIndex) => (
                                  <tr key={rowIndex}>
                                    {row.map((value, columnIndex) => (
                                      <td key={`${rowIndex}-${columnIndex}`} className="px-4 py-3 text-slate-200">
                                        {String(value ?? "")}
                                      </td>
                                    ))}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </div>
                    ) : null}
                  </section>
                </section>

                <section className="grid gap-6 xl:grid-cols-[1fr_1fr]">
                  <div className="rounded-[28px] border border-white/10 bg-slate-900/80 p-6 shadow-panel">
                    <h3 className="text-lg font-semibold text-white">Approved tables</h3>
                    <div className="mt-4 space-y-3">
                      {detail.tables.map((table) => (
                        <div key={`${table.mode}-${table.table_name}`} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                          <div className="flex items-center justify-between gap-4">
                            <div>
                              <div className="font-medium text-white">{table.display_name}</div>
                              <div className="mt-1 text-xs text-slate-500">{table.table_name}</div>
                            </div>
                            <div className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300">{table.mode}</div>
                          </div>
                          <div className="mt-3 flex flex-wrap gap-2">
                            {table.schema.columns.map((column) => (
                              <span key={`${table.table_name}-${column.name}`} className="rounded-full border border-white/10 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
                                {column.name}
                              </span>
                            ))}
                          </div>
                          {table.schema.lineage?.length ? (
                            <div className="mt-4 space-y-2">
                              {table.schema.lineage.map((item) => (
                                <div key={`${table.table_name}-${item.column_name}`} className="rounded-xl border border-white/10 bg-slate-950/40 p-3">
                                  <div className="flex items-center justify-between gap-3">
                                    <div className="text-sm text-white">{item.column_name}</div>
                                    <div className="rounded-full border border-white/10 px-3 py-1 text-[11px] text-slate-300">{item.status}</div>
                                  </div>
                                  <div className="mt-2 flex flex-wrap gap-2">
                                    {item.source_columns.map((source) => (
                                      <span key={`${item.column_name}-${source.source_table}-${source.source_column}`} className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300">
                                        {source.display_name} <span className="text-slate-500">@ {source.source_table}</span>
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      ))}
                      {!detail.tables.length ? <div className="rounded-2xl border border-dashed border-white/10 p-4 text-sm text-slate-400">Approve a proposal to materialize tables.</div> : null}
                    </div>
                  </div>

                  <div className="rounded-[28px] border border-white/10 bg-slate-900/80 p-6 shadow-panel">
                    <h3 className="text-lg font-semibold text-white">Query history & approvals</h3>
                    {approvalDecisions.length ? (
                      <div className="mt-4 space-y-2">
                        {approvalDecisions.map((decision, index) => (
                          <div key={`${decision.canonical_name}-${index}`} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                            <div className="flex items-center justify-between gap-4">
                              <div className="font-medium text-white">{decision.canonical_name}</div>
                              <div className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300">{decision.decision}</div>
                            </div>
                            <div className="mt-2 text-sm text-slate-400">{decision.reason}</div>
                          </div>
                        ))}
                      </div>
                    ) : null}
                    <div className="mt-4 space-y-3">
                      {history.map((entry) => (
                        <div key={entry.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                          <div className="flex items-center justify-between gap-4">
                            <div className="font-medium text-white">{entry.question}</div>
                            <div className="text-xs text-slate-500">{formatDate(entry.created_at)}</div>
                          </div>
                          <div className="mt-2 text-xs text-sky-200">{entry.sql}</div>
                          <div className="mt-2 text-sm text-slate-400">{entry.explanation}</div>
                        </div>
                      ))}
                      {!history.length ? <div className="rounded-2xl border border-dashed border-white/10 p-4 text-sm text-slate-400">No query history yet.</div> : null}
                    </div>
                  </div>
                </section>
              </>
            ) : (
              <section className="rounded-[32px] border border-dashed border-white/10 bg-white/5 p-12 text-center shadow-panel">
                <h2 className="text-2xl font-semibold text-white">データセットを作成して開始</h2>
                <p className="mt-3 text-sm text-slate-400">左側から Excel / CSV をアップロードすると、LLM 提案の確認から検索まで進められます。</p>
              </section>
            )}
          </main>
        </div>
      </div>
    </div>
  );
}
