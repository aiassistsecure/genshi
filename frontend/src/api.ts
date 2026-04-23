import type { SheetOut, SheetSummary, Template, Cell } from "./types";

const BASE = "/api";

function getOverrides() {
  const netrows = localStorage.getItem("genshi.netrows_key") || undefined;
  const aiassist = localStorage.getItem("genshi.aiassist_key") || undefined;
  const model = localStorage.getItem("genshi.aiassist_model") || undefined;
  const provider = localStorage.getItem("genshi.aiassist_provider") || undefined;
  return { netrows, aiassist, model, provider };
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(BASE + path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!r.ok) {
    let detail = r.statusText;
    try { detail = (await r.json()).detail || detail; } catch {}
    throw new Error(`${r.status}: ${detail}`);
  }
  return r.json();
}

export const api = {
  health: () => req<{ ok: boolean; has_netrows_key: boolean; has_aiassist_key: boolean }>("/health"),
  listSheets: () => req<SheetSummary[]>("/sheets"),
  getSheet: (id: string) => req<SheetOut>(`/sheets/${id}`),
  createSheet: (name: string, headers: string[], query: string) =>
    req<SheetOut>("/sheets", { method: "POST", body: JSON.stringify({ name, headers, query }) }),
  updateSheet: (id: string, patch: Partial<{ name: string; headers: string[]; query: string }>) =>
    req<SheetOut>(`/sheets/${id}`, { method: "PATCH", body: JSON.stringify(patch) }),
  deleteSheet: (id: string) => req<{ deleted: string }>(`/sheets/${id}`, { method: "DELETE" }),
  generate: (id: string, opts: { row_limit?: number; sources?: string[] } = {}) => {
    const ov = getOverrides();
    return req<{ status: string }>(`/sheets/${id}/generate`, {
      method: "POST",
      body: JSON.stringify({
        row_limit: opts.row_limit ?? 15,
        sources: opts.sources,
        netrows_key_override: ov.netrows,
        aiassist_key_override: ov.aiassist,
        aiassist_model: ov.model,
        aiassist_provider: ov.provider,
      }),
    });
  },
  jobStatus: (id: string) => req<{ exists: boolean; done: boolean; error: string; row_count: number }>(`/sheets/${id}/job`),
  updateCell: (sheetId: string, rowId: string, header: string, value: any, reEnrich = false) => {
    const ov = getOverrides();
    return req<{ row_id: string; cell: Cell }>(
      `/sheets/${sheetId}/rows/${rowId}/cells/${encodeURIComponent(header)}`,
      { method: "PATCH", body: JSON.stringify({ value, re_enrich: reEnrich, aiassist_key_override: ov.aiassist, aiassist_model: ov.model, aiassist_provider: ov.provider, netrows_key_override: ov.netrows }) },
    );
  },
  fillBlanks: (sheetId: string) => {
    const ov = getOverrides();
    return req<{ filled_cells: number; rows_touched: number; errors: string[] }>(
      `/sheets/${sheetId}/fill-blanks`,
      { method: "POST", body: JSON.stringify({
        netrows_key_override: ov.netrows,
        aiassist_key_override: ov.aiassist,
        aiassist_model: ov.model,
        aiassist_provider: ov.provider,
      }) },
    );
  },
  addRow: (sheetId: string) => req(`/sheets/${sheetId}/rows`, { method: "POST" }),
  deleteRow: (sheetId: string, rowId: string) => req(`/sheets/${sheetId}/rows/${rowId}`, { method: "DELETE" }),
  templates: () => req<Template[]>("/templates"),
  providers: (overrideKey?: string, overrideProvider?: string) => {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (overrideKey) headers["X-Aiassist-Key"] = overrideKey;
    if (overrideProvider) headers["X-Aiassist-Provider"] = overrideProvider;
    return req<{ default_provider?: string; providers: { id: string; name: string; is_default?: boolean; models: { id: string; name: string; context_window?: number; max_output?: number; provider?: string }[] }[] }>(
      "/providers",
      Object.keys(headers).length > 1 ? { headers } : undefined,
    );
  },
  exportUrl: (sheetId: string, format: "csv" | "xlsx") => `${BASE}/sheets/${sheetId}/export?format=${format}`,
};

export function streamSheet(sheetId: string, onEvent: (ev: { type: string; data: any }) => void): EventSource {
  const es = new EventSource(`${BASE}/sheets/${sheetId}/stream`);
  const handler = (e: MessageEvent) => {
    let data: any = {};
    try { data = JSON.parse(e.data); } catch {}
    onEvent({ type: e.type, data });
  };
  // sse-starlette wraps each event as named events; subscribe to the ones we care about
  [
    "plan", "stage", "query_plan", "page_fetched", "primary_retry", "fallback",
    "producer_empty", "primary_done", "secondary_done", "deep_profiles_done",
    "yc_deep_done", "maps_place_done", "crunchbase_company_done",
    "crunchbase_person_done", "indeed_company_done", "indeed_job_details_done",
    "github_enrich_done", "emails_fetched", "intel_done", "backfill",
    "source_call", "source_error", "tick", "stale", "llm_error",
    "done", "persisted", "error", "end", "message",
  ].forEach((t) => {
    es.addEventListener(t, handler as any);
  });
  es.onerror = () => {
    // keep alive; route errors come as named events
  };
  return es;
}
