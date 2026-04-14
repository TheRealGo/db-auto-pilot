import { useEffect, useState } from "react";
import {
  approveMaterializationProposal,
  approveProposal,
  generateMaterializationProposal,
  generateProposal,
  getAppSettings,
  getDataset,
  getQueryHistory,
  listDatasets,
  queryDataset,
  retryMaterializationProposal,
  reviseProposal,
  updateAppSettings,
  uploadDataset,
} from "./api";
import type {
  AppSettings,
  DatasetDetail,
  DatasetSummary,
  MaterializationProposalResponse,
  ProposalResponse,
  QueryHistoryEntry,
  QueryResponse,
} from "./types";

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

  async function handleGenerateMaterializationProposal(proposalId?: string) {
    if (!selectedId) return;
    await withTask("Generating materialization proposal", async () => {
      await generateMaterializationProposal(selectedId, proposalId);
      await selectDataset(selectedId);
    });
  }

  async function handleApproveMaterializationProposal(materializationProposal: MaterializationProposalResponse) {
    if (!selectedId) return;
    await withTask("Approving materialization", async () => {
      await approveMaterializationProposal(selectedId, materializationProposal.id);
      await selectDataset(selectedId);
    });
  }

  async function handleRetryMaterializationProposal(materializationProposal: MaterializationProposalResponse) {
    if (!selectedId) return;
    await withTask("Re-proposing materialization", async () => {
      await retryMaterializationProposal(selectedId, materializationProposal.id);
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
  const proposalHistory = detail?.proposals ?? [];
  const canonicalProposal = proposal?.proposal.canonical_proposal;
  const proposalChecklist = canonicalProposal?.approval_checklist ?? [];
  const canonicalCandidates = canonicalProposal?.candidates ?? [];
  const reviewItems = proposal?.proposal.review_items ?? [];
  const normalizationActions = proposal?.proposal.normalization_actions ?? [];
  const schemaDraft = proposal?.proposal.schema_draft ?? [];
  const observations = proposal?.proposal.observations ?? [];
  const comparisonCandidates = proposal?.proposal.comparison_candidates ?? [];
  const agentSteps = proposal?.proposal.agent_steps ?? [];
  const questionsForUser = proposal?.proposal.questions_for_user ?? [];
  const approvalDecisions = detail?.approval_decisions ?? [];
  const latestMaterializationProposal = detail?.latest_materialization_proposal;
  const materializationProposals = detail?.materialization_proposals ?? [];
  const materializationRuns = detail?.materialization_runs ?? [];
  const latestMaterializationRun = materializationRuns[0] ?? null;
  const materializationRunsById = new Map(materializationRuns.map((run) => [run.id, run]));
  const latestRetryContext = latestMaterializationProposal?.materialization.retry_context ?? null;
  const sourceRunForLatestProposal =
    latestMaterializationProposal?.source_run_id ? materializationRunsById.get(latestMaterializationProposal.source_run_id) ?? null : null;
  const timelineEntries = [
    ...proposalHistory.map((item) => ({
      id: `proposal-${item.id}`,
      type: "proposal" as const,
      createdAt: item.created_at,
      title: `Proposal v${item.version}`,
      summary: item.proposal.summary,
      status: item.status,
    })),
    ...materializationProposals.map((item) => ({
      id: `materialization-proposal-${item.id}`,
      type: "materialization_proposal" as const,
      createdAt: item.created_at,
      title: `Materialization Proposal v${item.version}`,
      summary: item.materialization.summary,
      status: item.status,
    })),
    ...materializationRuns.map((item) => ({
      id: `materialization-run-${item.id}`,
      type: "materialization_run" as const,
      createdAt: item.created_at,
      title: `Materialization ${item.status}`,
      summary: item.result.error ?? item.result.generation_summary ?? item.result.execution_notes?.join(" / ") ?? "run completed",
      status: item.status,
    })),
    ...history.map((item) => ({
      id: `query-${item.id}`,
      type: "query" as const,
      createdAt: item.created_at,
      title: `Query (${item.target_mode})`,
      summary: item.question,
      status: "recorded",
    })),
  ].sort((left, right) => new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime());

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
                          {proposal
                            ? detail.dataset.status === "awaiting_materialization_approval" || detail.dataset.status === "approved"
                              ? "proposal は承認済みです。次は materialization proposal を確認して承認します。"
                              : "提案を確認して、必要なら補足を返してから承認します。"
                            : "まだ提案がありません。まず Generate Proposal を実行してください。"}
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
                          <div className="text-sm font-medium text-white">Canonical summary</div>
                          <div className="mt-2 text-sm text-slate-400">
                            {canonicalProposal?.overview.summary ?? proposal.proposal.summary}
                          </div>
                          {canonicalProposal ? (
                            <div className="mt-3 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
                              <div className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs text-slate-300">
                                source tables: {canonicalProposal.overview.source_table_count}
                              </div>
                              <div className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs text-slate-300">
                                merged components: {canonicalProposal.overview.merged_component_count}
                              </div>
                              <div className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs text-slate-300">
                                review items: {canonicalProposal.overview.review_item_count}
                              </div>
                              <div className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs text-slate-300">
                                open questions: {canonicalProposal.overview.question_count}
                              </div>
                            </div>
                          ) : null}
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
                            <div className="text-sm font-medium text-white">Approval checklist</div>
                            <div className="mt-3 space-y-3">
                              {proposalChecklist.map((item) => (
                                <div key={item.title} className="rounded-xl border border-white/10 bg-white/5 p-3">
                                  <div className="flex items-center justify-between gap-3">
                                    <div className="font-medium text-white">{item.title}</div>
                                    <div
                                      className={`rounded-full px-3 py-1 text-xs ${
                                        item.status === "ready"
                                          ? "bg-emerald-400/15 text-emerald-200"
                                          : "bg-amber-400/15 text-amber-100"
                                      }`}
                                    >
                                      {item.status}
                                    </div>
                                  </div>
                                  <div className="mt-3 space-y-2">
                                    {item.items.map((entry) => (
                                      <div key={entry} className="rounded-xl border border-white/10 bg-slate-950/60 p-3 text-sm text-slate-300">
                                        {entry}
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              ))}
                              {!proposalChecklist.length ? (
                                <div className="rounded-xl border border-dashed border-white/10 p-3 text-sm text-slate-500">
                                  No canonical checklist available for this proposal.
                                </div>
                              ) : null}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                            <div className="text-sm font-medium text-white">Observed columns</div>
                            <div className="mt-3 space-y-3">
                              {observations.map((table) => (
                                <div key={table.table_name} className="rounded-xl border border-white/10 bg-white/5 p-3">
                                  <div className="font-medium text-white">{table.display_name}</div>
                                  <div className="mt-1 text-xs text-slate-500">{table.row_count} rows</div>
                                  <div className="mt-3 flex flex-wrap gap-2">
                                    {table.columns.slice(0, 6).map((column) => (
                                      <span key={`${table.table_name}-${column.source_column}`} className="rounded-full border border-white/10 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
                                        {column.display_name} <span className="text-slate-500">({column.logical_type})</span>
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                            <div className="text-sm font-medium text-white">Questions for user</div>
                            <div className="mt-3 space-y-3">
                              {questionsForUser.map((item) => (
                                <div key={item} className="rounded-xl border border-white/10 bg-white/5 p-3 text-sm text-slate-300">
                                  {item}
                                </div>
                              ))}
                              {!questionsForUser.length ? <div className="rounded-xl border border-dashed border-white/10 p-3 text-sm text-slate-500">No open questions from the current proposal.</div> : null}
                            </div>
                          </div>
                        </div>

                        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                          <div className="text-sm font-medium text-white">Agent steps</div>
                          <div className="mt-3 space-y-3">
                            {agentSteps.map((step) => (
                              <div key={step.step} className="rounded-xl border border-white/10 bg-white/5 p-3">
                                <div className="flex items-center justify-between gap-3">
                                  <div className="font-medium text-white">Step {step.step}</div>
                                  <div className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300">{step.mode}</div>
                                </div>
                                {step.notes.length ? <div className="mt-2 text-sm text-slate-400">{step.notes.join(" / ")}</div> : null}
                                <div className="mt-3 space-y-2">
                                  {step.observation_requests.map((request, index) => (
                                    <div key={`${step.step}-${request.tool}-${index}`} className="rounded-xl border border-white/10 bg-slate-950/60 p-3">
                                      <div className="text-sm text-white">{request.tool}</div>
                                      <div className="mt-1 text-xs text-slate-500">{request.reason}</div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            ))}
                            {!agentSteps.length ? <div className="rounded-xl border border-dashed border-white/10 p-3 text-sm text-slate-500">The current proposal finalized without extra observation steps.</div> : null}
                          </div>
                        </div>

                        <div className="grid gap-4 xl:grid-cols-2">
                          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                            <div className="text-sm font-medium text-white">Review items</div>
                            <div className="mt-3 space-y-3">
                              {reviewItems.map((item) => (
                                <div key={`${item.component_id}-${item.canonical_name}-${item.type}`} className="rounded-xl border border-white/10 bg-white/5 p-3">
                                  <div className="flex items-center justify-between gap-3">
                                    <div className="font-medium text-white">{item.canonical_name}</div>
                                    <div className="flex items-center gap-2">
                                      {item.severity ? (
                                        <div className={`rounded-full px-3 py-1 text-xs ${item.severity === "blocking" ? "bg-rose-400/15 text-rose-100" : "bg-amber-400/15 text-amber-100"}`}>
                                          {item.severity}
                                        </div>
                                      ) : null}
                                      <div className="rounded-full bg-amber-400/15 px-3 py-1 text-xs text-amber-100">{item.type}</div>
                                    </div>
                                  </div>
                                  <p className="mt-2 text-xs text-slate-400">{item.message}</p>
                                  {item.override_applied ? <div className="mt-2 text-xs text-sky-200">feedback override applied</div> : null}
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
                            <div className="text-sm font-medium text-white">Canonical merge candidates</div>
                            <div className="text-xs text-slate-500">v{proposal.version} / {comparisonCandidates.length} comparisons</div>
                          </div>
                          <div className="mt-3 grid gap-3">
                            {(canonicalCandidates.length ? canonicalCandidates : proposal.proposal.column_mappings).map((mapping, index) => {
                              const mappingReason = "reason" in mapping ? mapping.reason : mapping.rationale;
                              const mappingSignal = "signal" in mapping ? mapping.signal : "review";
                              const sourceCount = "source_count" in mapping ? mapping.source_count : mapping.matches.length;
                              return (
                              <div key={`${mapping.canonical_name}-${index}`} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                                <div className="flex items-center justify-between gap-3">
                                  <div className="font-medium text-white">{mapping.canonical_name}</div>
                                  <div
                                    className={`rounded-full px-3 py-1 text-xs ${
                                      mapping.decision === "merge"
                                        ? "bg-emerald-400/15 text-emerald-200"
                                        : mapping.decision === "review"
                                          ? "bg-amber-400/15 text-amber-100"
                                          : "bg-slate-700/70 text-slate-200"
                                    }`}
                                  >
                                    {mapping.decision}
                                  </div>
                                </div>
                                <div className="mt-2 text-xs text-slate-400">confidence {mapping.confidence.toFixed(2)}</div>
                                <div className="mt-2 text-xs text-slate-500">
                                  signal {mappingSignal} / sources {sourceCount}
                                </div>
                                {"override_applied" in mapping && mapping.override_applied ? (
                                  <div className="mt-2 text-xs text-sky-200">feedback override applied</div>
                                ) : null}
                                {"evidence_summary" in mapping && mapping.evidence_summary ? (
                                  <div className="mt-2 text-xs text-slate-500">
                                    value overlap {mapping.evidence_summary.value_overlap.toFixed(2)}
                                  </div>
                                ) : null}
                                <div className="mt-3 flex flex-wrap gap-2">
                                  {mapping.matches.map((match) => (
                                    <span key={`${match.source_table}-${match.source_column}-${match.display_name}`} className="rounded-full border border-white/10 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
                                      {match.display_name} <span className="text-slate-500">@ {match.source_table}:{match.source_column}</span>
                                    </span>
                                  ))}
                                </div>
                                <p className="mt-3 text-sm text-slate-400">{mappingReason}</p>
                              </div>
                            )})}
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
                              disabled={Boolean(busy) || detail.dataset.status === "awaiting_materialization_approval" || detail.dataset.status === "approved"}
                              onClick={() => void handleApproveProposal(proposal)}
                            >
                              Approve Proposal
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
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-medium text-white">Generated SQL</div>
                            <div className="rounded-full border border-white/10 px-3 py-1 text-[11px] text-slate-300">
                              {queryResult.generator}
                            </div>
                          </div>
                          <pre className="mt-3 overflow-x-auto text-xs text-sky-200">{queryResult.sql}</pre>
                        </div>
                        <div className="rounded-2xl border border-white/10 bg-slate-950/60 p-4 text-sm text-slate-300">
                          {queryResult.explanation}
                        </div>
                        {queryResult.warning ? (
                          <div className="rounded-2xl border border-amber-400/30 bg-amber-500/10 p-4 text-sm text-amber-100">
                            {queryResult.warning}
                          </div>
                        ) : null}
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
                    <h3 className="text-lg font-semibold text-white">Materialization Review</h3>
                    <div className="mt-4 space-y-4">
                      <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-slate-300">
                        {detail.dataset.status === "uploaded" || detail.dataset.status === "awaiting_approval"
                          ? "proposal を承認すると、正規化方針と generated code を含む materialization proposal を作れます。"
                          : latestMaterializationProposal
                            ? "materialization proposal を確認し、問題なければ承認して DB を作成します。"
                            : "まだ materialization proposal がありません。Generate Materialization Proposal を実行してください。"}
                      </div>

                      <div className="flex flex-col gap-3 md:flex-row">
                        <button
                          className="rounded-2xl border border-sky-300/30 bg-sky-400/10 px-4 py-3 text-sm font-medium text-sky-100 transition hover:bg-sky-400/20 disabled:opacity-40"
                          disabled={
                            Boolean(busy) ||
                            !proposal ||
                            (detail.dataset.status !== "awaiting_materialization_approval" && detail.dataset.status !== "approved")
                          }
                          onClick={() => void handleGenerateMaterializationProposal(proposal?.id)}
                        >
                          Generate Materialization Proposal
                        </button>
                        <button
                          className="rounded-2xl bg-emerald-400 px-4 py-3 text-sm font-medium text-slate-950 transition hover:bg-emerald-300 disabled:opacity-40"
                          disabled={Boolean(busy) || !latestMaterializationProposal}
                          onClick={() => latestMaterializationProposal && void handleApproveMaterializationProposal(latestMaterializationProposal)}
                        >
                          Approve Materialization
                        </button>
                        <button
                          className="rounded-2xl border border-amber-300/30 bg-amber-400/10 px-4 py-3 text-sm font-medium text-amber-100 transition hover:bg-amber-400/20 disabled:opacity-40"
                          disabled={Boolean(busy) || !latestMaterializationProposal || latestMaterializationRun?.status !== "failed"}
                          onClick={() => latestMaterializationProposal && void handleRetryMaterializationProposal(latestMaterializationProposal)}
                        >
                          Re-propose Materialization
                        </button>
                      </div>

                      {latestMaterializationProposal ? (
                        <div className="space-y-4">
                          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                            <div className="flex items-center justify-between gap-3">
                              <div>
                                <div className="text-sm font-medium text-white">Latest materialization proposal</div>
                                <div className="mt-1 text-xs text-slate-500">
                                  v{latestMaterializationProposal.version} / {formatDate(latestMaterializationProposal.created_at)}
                                </div>
                              </div>
                              <div className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300">
                                {latestMaterializationProposal.status}
                              </div>
                            </div>
                            <div className="mt-3 grid gap-3 md:grid-cols-2">
                              <div className="rounded-xl border border-white/10 bg-slate-950/60 p-3 text-xs text-slate-400">
                                source proposal: <span className="text-slate-200">{latestMaterializationProposal.proposal_id}</span>
                              </div>
                              <div className="rounded-xl border border-white/10 bg-slate-950/60 p-3 text-xs text-slate-400">
                                source run:{" "}
                                <span className="text-slate-200">
                                  {latestMaterializationProposal.source_run_id ?? "none"}
                                </span>
                              </div>
                            </div>
                            <p className="mt-3 text-sm text-slate-300">{latestMaterializationProposal.materialization.summary}</p>
                          </div>

                          {latestRetryContext ? (
                            <div className="grid gap-4 xl:grid-cols-2">
                              <div className="rounded-2xl border border-amber-300/20 bg-amber-400/10 p-4">
                                <div className="text-sm font-medium text-white">Retry context</div>
                                <div className="mt-3 space-y-2 text-sm text-amber-50">
                                  <div>reason: {latestRetryContext.reason}</div>
                                  {latestRetryContext.previous_error_stage ? (
                                    <div>previous failure: {latestRetryContext.previous_error_stage}</div>
                                  ) : null}
                                  {latestRetryContext.previous_error ? (
                                    <div className="text-xs text-amber-100/80">{latestRetryContext.previous_error}</div>
                                  ) : null}
                                  {latestRetryContext.focus_points?.map((item) => (
                                    <div key={item} className="rounded-xl border border-white/10 bg-slate-950/50 p-3 text-xs text-slate-200">
                                      {item}
                                    </div>
                                  ))}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                                <div className="text-sm font-medium text-white">Derived from run</div>
                                {sourceRunForLatestProposal ? (
                                  <div className="mt-3 rounded-xl border border-white/10 bg-slate-950/60 p-3 text-sm text-slate-300">
                                    <div className="flex items-center justify-between gap-3">
                                      <span>{sourceRunForLatestProposal.status}</span>
                                      <span className="text-xs text-slate-500">{formatDate(sourceRunForLatestProposal.created_at)}</span>
                                    </div>
                                    {sourceRunForLatestProposal.result.error_stage ? (
                                      <div className="mt-2 text-xs text-amber-200">
                                        failure stage: {sourceRunForLatestProposal.result.error_stage}
                                      </div>
                                    ) : null}
                                    {sourceRunForLatestProposal.result.error ? (
                                      <div className="mt-2 text-xs text-rose-200">{sourceRunForLatestProposal.result.error}</div>
                                    ) : null}
                                  </div>
                                ) : (
                                  <div className="mt-3 rounded-xl border border-dashed border-white/10 p-3 text-sm text-slate-500">
                                    No prior failed run linked to this proposal.
                                  </div>
                                )}
                              </div>
                            </div>
                          ) : null}

                          <div className="grid gap-4 xl:grid-cols-2">
                            <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                              <div className="text-sm font-medium text-white">Normalization decisions</div>
                              <div className="mt-3 space-y-3">
                                {latestMaterializationProposal.materialization.normalization_decisions.map((decision, index) => (
                                  <div key={`${decision.component_id}-${decision.column_name}-${index}`} className="rounded-xl border border-white/10 bg-slate-950/60 p-3">
                                    <div className="text-sm text-white">{decision.column_name}</div>
                                    <div className="mt-2 flex flex-wrap gap-2">
                                      {decision.actions.map((action) => (
                                        <span key={action} className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300">
                                          {action}
                                        </span>
                                      ))}
                                    </div>
                                    {decision.config && Object.keys(decision.config).length ? (
                                      <pre className="mt-2 overflow-x-auto rounded-xl border border-white/10 bg-slate-900/80 p-2 text-[11px] text-slate-400">
                                        {JSON.stringify(decision.config, null, 2)}
                                      </pre>
                                    ) : null}
                                    <p className="mt-2 text-xs text-slate-400">{decision.reason}</p>
                                  </div>
                                ))}
                              </div>
                            </div>

                            <div className="space-y-4">
                              {latestRetryContext?.column_patches?.length ? (
                                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                                  <div className="text-sm font-medium text-white">Targeted fixes</div>
                                  <div className="mt-3 space-y-3">
                                    {latestRetryContext.column_patches.map((patch, index) => (
                                      <div key={`${patch.component_id}-${patch.column_name}-${index}`} className="rounded-xl border border-white/10 bg-slate-950/60 p-3">
                                        <div className="text-sm text-white">{patch.table_name}.{patch.column_name}</div>
                                        <div className="mt-2 flex flex-wrap gap-2">
                                          {patch.warning_types.map((warning) => (
                                            <span key={warning} className="rounded-full bg-amber-400/15 px-3 py-1 text-xs text-amber-100">
                                              {warning}
                                            </span>
                                          ))}
                                        </div>
                                        <div className="mt-2 flex flex-wrap gap-2">
                                          {patch.suggested_actions.map((action) => (
                                            <span key={action} className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300">
                                              {action}
                                            </span>
                                          ))}
                                        </div>
                                        {Object.keys(patch.suggested_config_patch).length ? (
                                          <pre className="mt-2 overflow-x-auto rounded-xl border border-white/10 bg-slate-900/80 p-2 text-[11px] text-slate-400">
                                            {JSON.stringify(patch.suggested_config_patch, null, 2)}
                                          </pre>
                                        ) : null}
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              ) : null}
                              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                                <div className="text-sm font-medium text-white">Expected outputs</div>
                                <div className="mt-3 space-y-2">
                                  {latestMaterializationProposal.materialization.expected_outputs.map((item) => (
                                    <div key={item} className="rounded-xl border border-white/10 bg-slate-950/60 p-3 text-sm text-slate-300">
                                      {item}
                                    </div>
                                  ))}
                                </div>
                              </div>
                              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                                <div className="text-sm font-medium text-white">Risk notes</div>
                                <div className="mt-3 space-y-2">
                                  {latestMaterializationProposal.materialization.risk_notes.map((item) => (
                                    <div key={item} className="rounded-xl border border-white/10 bg-slate-950/60 p-3 text-sm text-slate-300">
                                      {item}
                                    </div>
                                  ))}
                                  {!latestMaterializationProposal.materialization.risk_notes.length ? (
                                    <div className="rounded-xl border border-dashed border-white/10 p-3 text-sm text-slate-500">No explicit risks in the latest proposal.</div>
                                  ) : null}
                                </div>
                              </div>
                              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                                <div className="text-sm font-medium text-white">Quality expectations</div>
                                <div className="mt-3 space-y-2">
                                  {latestMaterializationProposal.materialization.quality_expectations.map((item) => (
                                    <div key={item} className="rounded-xl border border-white/10 bg-slate-950/60 p-3 text-sm text-slate-300">
                                      {item}
                                    </div>
                                  ))}
                                  {!latestMaterializationProposal.materialization.quality_expectations.length ? (
                                    <div className="rounded-xl border border-dashed border-white/10 p-3 text-sm text-slate-500">No explicit quality expectations in the latest proposal.</div>
                                  ) : null}
                                </div>
                              </div>
                            </div>
                          </div>

                          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                            <div className="text-sm font-medium text-white">Transformation notes</div>
                            <div className="mt-3 space-y-2">
                              {latestMaterializationProposal.materialization.transformation_notes.map((item) => (
                                <div key={item} className="rounded-xl border border-white/10 bg-slate-950/60 p-3 text-sm text-slate-300">
                                  {item}
                                </div>
                              ))}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                            <div className="text-sm font-medium text-white">Generated code</div>
                            <pre className="mt-3 overflow-x-auto rounded-2xl bg-slate-950/80 p-4 text-xs text-sky-200">
                              {latestMaterializationProposal.materialization.generated_code}
                            </pre>
                          </div>
                        </div>
                      ) : null}

                      {materializationProposals.length > 1 ? (
                        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                          <div className="text-sm font-medium text-white">Proposal history</div>
                          <div className="mt-3 space-y-2">
                            {materializationProposals.map((item) => (
                              <div key={item.id} className="rounded-xl border border-white/10 bg-slate-950/60 p-3 text-sm text-slate-300">
                                <div>v{item.version} / {item.status} / {formatDate(item.created_at)}</div>
                                <div className="mt-1 text-xs text-slate-500">proposal: {item.proposal_id}</div>
                                <div className="mt-1 text-xs text-slate-500">source run: {item.source_run_id ?? "none"}</div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </div>

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
                    <h3 className="text-lg font-semibold text-white">Dataset timeline</h3>
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
                      {timelineEntries.map((entry) => (
                        <div key={entry.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                          <div className="flex items-center justify-between gap-4">
                            <div className="font-medium text-white">{entry.title}</div>
                            <div className="text-xs text-slate-500">{formatDate(entry.createdAt)}</div>
                          </div>
                          <div className="mt-2 text-xs text-slate-500">{entry.type} / {entry.status}</div>
                          <div className="mt-2 text-sm text-slate-300">{entry.summary}</div>
                        </div>
                      ))}
                      {!timelineEntries.length ? (
                        <div className="rounded-2xl border border-dashed border-white/10 p-4 text-sm text-slate-400">
                          No timeline entries yet.
                        </div>
                      ) : null}
                    </div>
                    {materializationRuns.length ? (
                      <div className="mt-4 space-y-3">
                        {materializationRuns.map((run) => (
                          <div key={run.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                            <div className="flex items-center justify-between gap-4">
                              <div className="font-medium text-white">Materialization {run.status}</div>
                              <div className="text-xs text-slate-500">{formatDate(run.created_at)}</div>
                            </div>
                            <div className="mt-2 text-xs text-slate-500">proposal: {run.proposal_id}</div>
                            {run.result.execution_notes?.length ? (
                              <div className="mt-2 text-sm text-slate-400">{run.result.execution_notes.join(" / ")}</div>
                            ) : null}
                            {run.result.generation_summary ? (
                              <div className="mt-2 text-sm text-slate-400">{run.result.generation_summary}</div>
                            ) : null}
                            {run.result.guard_summary ? (
                              <div className="mt-2 space-y-1 text-xs text-slate-500">
                                <div>
                                  guard: {run.result.guard_summary.status} / imports: {run.result.guard_summary.imports.join(", ") || "none"}
                                </div>
                                {run.result.guard_summary.violations.length ? (
                                  <div>
                                    violations: {run.result.guard_summary.violations.map((item) => `${item.type}:${item.value}`).join(", ")}
                                  </div>
                                ) : null}
                              </div>
                            ) : null}
                            {run.result.repair_summary ? (
                              <div className="mt-2 text-xs text-slate-500">
                                repair: {run.result.repair_summary.status}
                                {run.result.repair_summary.applied_repairs.length ? ` / ${run.result.repair_summary.applied_repairs.join(", ")}` : ""}
                              </div>
                            ) : null}
                            {run.result.resource_summary ? (
                              <div className="mt-2 text-xs text-slate-500">
                                resource: tables {run.result.resource_summary.merged_table_count}, rows {run.result.resource_summary.max_rows_seen}, cols {run.result.resource_summary.max_columns_seen}
                              </div>
                            ) : null}
                            {run.result.quality_summary ? (
                              <div className="mt-2 text-xs text-slate-500">
                                quality: {run.result.quality_summary.status} / warnings {run.result.quality_summary.warning_count}
                              </div>
                            ) : null}
                            {run.result.warnings?.length ? (
                              <div className="mt-2 space-y-1">
                                {run.result.warnings.map((warning) => (
                                  <div key={warning} className="text-xs text-amber-200">
                                    {warning}
                                  </div>
                                ))}
                              </div>
                            ) : null}
                            {run.result.error_stage ? (
                              <div className="mt-2 text-xs text-amber-200">failure stage: {run.result.error_stage}</div>
                            ) : null}
                            {run.result.error ? <div className="mt-2 text-sm text-rose-200">{run.result.error}</div> : null}
                            <pre className="mt-3 max-h-48 overflow-auto rounded-xl border border-white/10 bg-slate-950/60 p-3 text-xs text-sky-200">
                              {run.generated_code}
                            </pre>
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
