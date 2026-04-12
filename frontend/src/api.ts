import type {
  AppSettings,
  DatasetDetail,
  DatasetSummary,
  ProposalResponse,
  QueryHistoryEntry,
  QueryResponse,
} from "./types";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ??
  (window.location.protocol === "tauri:" || window.location.port === "1420"
    ? "http://127.0.0.1:8000"
    : "");

async function fetchWithRetry(input: string, init?: RequestInit, retries = 8): Promise<Response> {
  let attempt = 0;
  let lastError: unknown = null;
  while (attempt < retries) {
    try {
      return await fetch(input, init);
    } catch (error) {
      lastError = error;
      attempt += 1;
      if (attempt >= retries) {
        break;
      }
      await new Promise((resolve) => window.setTimeout(resolve, 350));
    }
  }
  throw lastError instanceof Error ? lastError : new Error("API request failed");
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetchWithRetry(`${API_BASE}${path}`, init);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed: ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export function listDatasets() {
  return api<DatasetSummary[]>("/datasets");
}

export function getAppSettings() {
  return api<AppSettings>("/settings");
}

export function updateAppSettings(payload: AppSettings) {
  return api<AppSettings>("/settings", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function getDataset(id: string) {
  return api<DatasetDetail>(`/datasets/${id}`);
}

export async function uploadDataset(files: File[]) {
  const formData = new FormData();
  files.forEach((file) => formData.append("files", file));
  return api<DatasetDetail>("/datasets", {
    method: "POST",
    body: formData,
  });
}

export function generateProposal(datasetId: string) {
  return api<ProposalResponse>(`/datasets/${datasetId}/proposal`, {
    method: "POST",
  });
}

export function reviseProposal(datasetId: string, feedback: string) {
  return api<ProposalResponse>(`/datasets/${datasetId}/proposal/revise`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ feedback }),
  });
}

export function approveProposal(datasetId: string, proposalId: string) {
  return api<{ dataset_id: string }>(`/datasets/${datasetId}/approve`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ approved_proposal_id: proposalId }),
  });
}

export function queryDataset(datasetId: string, targetMode: "raw" | "merged", question: string) {
  return api<QueryResponse>(`/datasets/${datasetId}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ target_mode: targetMode, question }),
  });
}

export function getQueryHistory(datasetId: string) {
  return api<QueryHistoryEntry[]>(`/datasets/${datasetId}/query-history`);
}
