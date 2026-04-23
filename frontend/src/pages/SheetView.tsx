import { useEffect, useMemo, useRef, useState, useCallback } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { DataEditor, GridCellKind, type GridCell, type GridColumn, type Item, type EditableGridCell } from "@glideapps/glide-data-grid";
import { api, streamSheet } from "../api";
import type { Cell, SheetOut } from "../types";

type Stage = string;

export default function SheetView() {
  const { id = "" } = useParams();
  const nav = useNavigate();
  const [sheet, setSheet] = useState<SheetOut | null>(null);
  const [error, setError] = useState("");
  const [stage, setStage] = useState<Stage>("");
  const [events, setEvents] = useState<string[]>([]);
  const [stageLog, setStageLog] = useState<{label: string; count?: number; ts: number}[]>([]);
  const [primary, setPrimary] = useState<string>("");
  const [plan, setPlan] = useState<string[]>([]);
  const [queryPlan, setQueryPlan] = useState<{
    search_keyword?: string; location_name?: string | null; location_geo_id?: string | null;
    industry_name?: string | null; industry_id?: string | null; technology?: string | null;
    employee_min?: number | null; employee_max?: number | null;
  } | null>(null);
  const [emptyHint, setEmptyHint] = useState<string>("");
  const [callCount, setCallCount] = useState<number>(0);
  const [elapsedMs, setElapsedMs] = useState<number>(0);
  const [stale, setStale] = useState<string>("");
  const esRef = useRef<EventSource | null>(null);
  const [selectedCell, setSelectedCell] = useState<{ row: number; col: number } | null>(null);

  const refresh = useCallback(async () => {
    try {
      const s = await api.getSheet(id);
      setSheet(s);
      return s;
    } catch (e: any) {
      setError(e.message || String(e));
      return null;
    }
  }, [id]);

  useEffect(() => {
    refresh().then((s) => {
      if (s?.status === "generating") attachStream();
    });
    return () => esRef.current?.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  const pushStage = (label: string, count?: number) =>
    setStageLog((s) => {
      // Avoid duplicate consecutive labels
      if (s.length && s[s.length - 1].label === label) return s;
      return [...s, { label, count, ts: Date.now() }];
    });

  const attachStream = useCallback(() => {
    esRef.current?.close();
    const es = streamSheet(id, async (ev) => {
      const t = ev.type;
      const d = ev.data || {};
      if (t === "stage") {
        setStage(d.stage || "");
        pushStage(d.stage, d.count);
      } else if (t === "plan") {
        setPlan(d.sources || []);
        pushStage("plan", (d.sources || []).length);
        setEvents((p) => [`plan: ${(d.sources || []).join(" → ")}`, ...p].slice(0, 30));
      } else if (t === "query_plan") {
        setQueryPlan(d);
        pushStage("✓ query plan");
        const bits: string[] = [];
        if (d.search_keyword) bits.push(`keyword="${d.search_keyword}"`);
        if (d.location_name) bits.push(`loc=${d.location_name}${d.location_geo_id ? `(${d.location_geo_id})` : ""}`);
        if (d.industry_name) bits.push(`industry=${d.industry_name}${d.industry_id ? `(${d.industry_id})` : ""}`);
        if (d.technology) bits.push(`tech=${d.technology}`);
        if (d.employee_min != null || d.employee_max != null) bits.push(`size=${d.employee_min ?? "?"}–${d.employee_max ?? "?"}`);
        setEvents((p) => [`query plan: ${bits.join(" · ")}`, ...p].slice(0, 30));
      } else if (t === "page_fetched") {
        pushStage(`page ${d.page}`, d.count);
      } else if (t === "primary_retry") {
        pushStage("↻ retry no-filters");
        setEvents((p) => [`retry: ${d.source} without filters (${d.reason})`, ...p].slice(0, 30));
      } else if (t === "fallback") {
        pushStage(`→ ${d.to}`);
        setEvents((p) => [`fallback ${d.from} → ${d.to}`, ...p].slice(0, 30));
      } else if (t === "producer_empty") {
        setEmptyHint(d.hint || "Primary producer returned 0 results.");
        pushStage("∅ no results");
        setEvents((p) => [`⚠ ${d.source} returned 0 results`, ...p].slice(0, 30));
      } else if (t === "primary_done") {
        setPrimary(d.source || "");
        pushStage(`✓ ${d.source}`, d.count);
        setEvents((p) => [`primary ${d.source} → ${d.count} items`, ...p].slice(0, 30));
      } else if (t === "secondary_done") {
        pushStage(`+ ${d.source}`, d.count);
        setEvents((p) => [`+ ${d.source} → ${d.count} items`, ...p].slice(0, 30));
      } else if (t === "deep_profiles_done")     pushStage("✓ profiles", d.count);
      else if (t === "yc_deep_done")              pushStage("✓ yc deep", d.count);
      else if (t === "maps_place_done")           pushStage("✓ maps deep");
      else if (t === "indeed_company_done")       pushStage("✓ indeed company");
      else if (t === "indeed_job_details_done")   pushStage("✓ indeed jobs");
      else if (t === "crunchbase_company_done")   pushStage("✓ crunchbase");
      else if (t === "github_enrich_done")        pushStage("✓ github", d.count);
      else if (t === "emails_fetched") {
        pushStage("✓ emails", d.domains?.length || 0);
        setEvents((p) => [`emails fetched for ${d.domains?.length || 0} domains`, ...p].slice(0, 30));
      } else if (t === "backfill") {
        pushStage("✓ backfill", d.rescued_cells);
        setEvents((p) => [`backfilled ${d.rescued_cells} cells`, ...p].slice(0, 30));
      } else if (t === "intel_done") {
        pushStage("✓ signals", d.count);
        setEvents((p) => [`intelligence scan: ${d.count} signals`, ...p].slice(0, 30));
      } else if (t === "source_call") {
        // Per-API-call firehose. Don't push a stage chip (would flood it),
        // just bump the counter and prepend a compact line in the events log.
        setCallCount((c) => c + 1);
        const ms = d.ms != null ? `${d.ms}ms` : "";
        const cnt = d.count != null ? `· ${d.count}` : "";
        setEvents((p) => [`◆ ${d.source} ${cnt} ${ms}`.replace(/\s+/g, " ").trim(), ...p].slice(0, 80));
      } else if (t === "tick") {
        // Heartbeat — backend is alive even during long parallel-gather phases.
        setElapsedMs(d.elapsed_ms || 0);
        if (d.calls != null) setCallCount(d.calls);
        if (d.stage) setStage(d.stage);
      } else if (t === "stale") {
        // Server restarted mid-flight; offer recovery instead of spinning forever.
        setStale(d.hint || "Generation was interrupted.");
        setStage(""); es.close();
      } else if (t === "source_error") {
        setEvents((p) => [`⚠ ${d.source}: ${d.error}`, ...p].slice(0, 80));
      } else if (t === "llm_error") {
        setEvents((p) => [`⚠ LLM: ${d.error}`, ...p].slice(0, 30));
      } else if (t === "persisted") {
        setStage("persisted"); pushStage("✓ saved");
        if (d.elapsed_ms) setElapsedMs(d.elapsed_ms);
        setEvents((p) => [`✓ persisted ${d.rows} rows in ${((d.elapsed_ms || 0) / 1000).toFixed(1)}s`, ...p].slice(0, 80));
        await refresh();
      } else if (t === "error") {
        setError(d.error || "Unknown error"); setStage("");
      } else if (t === "end") {
        setStage(""); es.close(); await refresh();
      }
    });
    esRef.current = es;
  }, [id, refresh]);

  const generate = async () => {
    setError(""); setEvents([]); setStageLog([]); setPlan([]); setPrimary("");
    setQueryPlan(null); setEmptyHint(""); setStage("starting…");
    setCallCount(0); setElapsedMs(0); setStale("");
    try {
      await api.generate(id, { row_limit: 15 });
      setSheet((s) => s ? { ...s, status: "generating" } : s);
      attachStream();
    } catch (e: any) {
      setError(e.message || String(e));
      setStage("");
    }
  };

  // Recover a sheet stuck in "generating" because the in-process job was
  // killed (server restart). Resets status server-side, then re-fires.
  const recoverAndGenerate = async () => {
    try {
      await fetch(`/api/sheets/${id}/reset`, { method: "POST" });
      await refresh();
      setStale("");
      await generate();
    } catch (e: any) {
      setError(e.message || String(e));
    }
  };

  // ---- Glide Data Grid ----
  const columns: GridColumn[] = useMemo(() => {
    if (!sheet) return [];
    return sheet.headers.map((h) => ({ title: h, id: h, width: 200, hasMenu: false }));
  }, [sheet]);

  const getCell = useCallback((cell: Item): GridCell => {
    if (!sheet) return { kind: GridCellKind.Text, data: "", displayData: "", allowOverlay: false };
    const [col, row] = cell;
    const r = sheet.rows[row];
    const h = sheet.headers[col];
    const c = (r?.cells?.[h] || null) as Cell | null;
    const v = c?.value;
    const text = v == null ? "" : String(v);
    const conf = c?.confidence || "low";
    const isSel = selectedCell?.row === row && selectedCell?.col === col;
    const themeOverride: any = {};
    if (conf === "verified") themeOverride.bgCell = "#f0fdf4";
    else if (conf === "invalid") themeOverride.bgCell = "#fef2f2";
    else if (conf === "uncertain") themeOverride.bgCell = "#fefce8";
    else if (conf === "user") themeOverride.bgCell = "#eff6ff";
    if (isSel) {
      // Holographic cyan/violet selection wash
      themeOverride.bgCell = "#ecfeff";
      themeOverride.bgCellMedium = "#cffafe";
      themeOverride.textDark = "#0e7490";
      themeOverride.accentLight = "#a5f3fc";
    }
    return {
      kind: GridCellKind.Text,
      data: text,
      displayData: text,
      allowOverlay: true,
      readonly: false,
      themeOverride,
    };
  }, [sheet, selectedCell]);

  const onCellEdited = useCallback(async (cell: Item, newValue: EditableGridCell) => {
    if (!sheet || newValue.kind !== GridCellKind.Text) return;
    const [col, row] = cell;
    const r = sheet.rows[row];
    const h = sheet.headers[col];
    if (!r) return;
    // Optimistic update
    setSheet((s) => {
      if (!s) return s;
      const rows = s.rows.slice();
      const cells = { ...rows[row].cells };
      cells[h] = { ...(cells[h] || {}), value: newValue.data, source: "user", confidence: "user" };
      rows[row] = { ...rows[row], cells };
      return { ...s, rows };
    });
    try {
      await api.updateCell(sheet.id, r.id, h, newValue.data);
    } catch (e: any) {
      setError(e.message);
      refresh();
    }
  }, [sheet, refresh]);

  const reEnrichSelected = async () => {
    if (!sheet || !selectedCell) return;
    const r = sheet.rows[selectedCell.row];
    const h = sheet.headers[selectedCell.col];
    if (!r || !h) return;
    setStage(`re-enriching ${h}…`);
    try {
      await api.updateCell(sheet.id, r.id, h, null, true);
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setStage("");
    }
  };

  const addRow = async () => { if (sheet) { await api.addRow(sheet.id); refresh(); } };
  const [filling, setFilling] = useState(false);
  const fillBlanks = async () => {
    if (!sheet || filling) return;
    setFilling(true); setStage("filling blanks via SERP…");
    try {
      const res = await api.fillBlanks(sheet.id);
      setEvents((p) => [`fill-blanks: filled ${res.filled_cells} cells across ${res.rows_touched} rows${res.errors.length ? ` · ${res.errors.length} warnings` : ""}`, ...p].slice(0, 30));
      pushStage("✓ fill-blanks", res.filled_cells);
      await refresh();
    } catch (e: any) {
      setError(e.message || String(e));
    } finally {
      setFilling(false); setStage("");
    }
  };
  const deleteSelectedRow = async () => {
    if (!sheet || !selectedCell) return;
    const r = sheet.rows[selectedCell.row];
    if (!r) return;
    if (!confirm("Delete this row?")) return;
    await api.deleteRow(sheet.id, r.id); refresh();
  };

  if (!sheet) {
    return (
      <div className="p-8">
        <button className="btn-ghost mb-4" onClick={() => nav("/")}>← Back</button>
        {error ? <div className="text-red-600 text-sm">{error}</div> : <div className="text-ink-500 text-sm">Loading…</div>}
      </div>
    );
  }

  const selectedCellMeta: Cell | null = (() => {
    if (!selectedCell) return null;
    const r = sheet.rows[selectedCell.row];
    const h = sheet.headers[selectedCell.col];
    return (r?.cells?.[h] || null) as Cell | null;
  })();

  const isGenerating = (sheet.status === "generating" || stage !== "") && !stale;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-ink-200 bg-white px-3 sm:px-6 py-3 sm:py-4 flex items-center gap-2 sm:gap-3 shrink-0">
        <button className="btn-ghost shrink-0" onClick={() => nav("/")}>←</button>
        <div className="flex-1 min-w-0">
          <input
            className="text-base sm:text-lg font-semibold text-ink-900 tracking-tight bg-transparent border-0 outline-none focus:ring-0 w-full truncate"
            value={sheet.name}
            onChange={(e) => setSheet({ ...sheet, name: e.target.value })}
            onBlur={() => api.updateSheet(sheet.id, { name: sheet.name })}
          />
          {sheet.query && <div className="text-[11px] sm:text-xs text-ink-500 truncate font-mono">"{sheet.query}"</div>}
        </div>
        <span className={`chip hidden sm:inline-flex ${
          sheet.status === "ready" ? "bg-accent-100 text-accent-700" :
          sheet.status === "generating" ? "bg-amber-100 text-amber-800" :
          sheet.status === "error" ? "bg-red-100 text-red-700" :
          "bg-ink-100 text-ink-700"
        }`}>{sheet.status}</span>
        <button className="btn-secondary px-2 sm:px-4 shrink-0" onClick={generate} disabled={isGenerating}>
          <span className="sm:hidden">{isGenerating ? "…" : "↻"}</span>
          <span className="hidden sm:inline">{isGenerating ? "Generating…" : sheet.rows.length ? "Regenerate" : "Generate"}</span>
        </button>
        <a className="btn-secondary hidden sm:inline-flex" href={api.exportUrl(sheet.id, "csv")} target="_blank" rel="noreferrer">CSV</a>
        <a className="btn-secondary hidden sm:inline-flex" href={api.exportUrl(sheet.id, "xlsx")} target="_blank" rel="noreferrer">XLSX</a>
        {/* Mobile export menu */}
        <div className="sm:hidden relative">
          <details>
            <summary className="btn-secondary cursor-pointer list-none px-2">⋯</summary>
            <div className="absolute right-0 mt-1 bg-white border border-ink-200 rounded-md shadow-lg z-10 py-1 min-w-[120px]">
              <a className="block px-3 py-1.5 text-sm hover:bg-ink-50" href={api.exportUrl(sheet.id, "csv")} target="_blank" rel="noreferrer">Export CSV</a>
              <a className="block px-3 py-1.5 text-sm hover:bg-ink-50" href={api.exportUrl(sheet.id, "xlsx")} target="_blank" rel="noreferrer">Export XLSX</a>
              <div className="border-t border-ink-100 my-1" />
              <div className="px-3 py-1 text-[11px] text-ink-500">Status: {sheet.status}</div>
            </div>
          </details>
        </div>
      </div>

      {/* Stale-job recovery banner — shown when a previous generation was
          interrupted by a server restart and the in-memory job is gone. */}
      {stale && (
        <div className="border-b border-amber-300 bg-amber-50 px-6 py-2 text-xs text-amber-900 shrink-0 flex items-center gap-3">
          <span className="text-base">⚠</span>
          <span className="flex-1">{stale}</span>
          <button className="btn-primary text-xs px-3 py-1" onClick={recoverAndGenerate}>
            Recover &amp; Regenerate
          </button>
        </div>
      )}

      {/* Status strip + stage timeline */}
      {(stage || error || events.length > 0 || stageLog.length > 0 || queryPlan || emptyHint) && (
        <div className="border-b border-ink-200 bg-gradient-to-b from-ink-50 to-white px-6 py-2 text-xs text-ink-600 shrink-0">
          <div className="flex items-center gap-3 mb-1 flex-wrap">
            {stage && (
              <span className="inline-flex items-center gap-1.5 font-medium text-amber-700">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-amber-500"></span>
                </span>
                {stage}
              </span>
            )}
            {primary && <span className="text-ink-500">primary: <span className="font-mono text-ink-700">{primary}</span></span>}
            {plan.length > 0 && <span className="text-ink-400 hidden md:inline truncate">plan: <span className="font-mono">{plan.join(" → ")}</span></span>}
            {(callCount > 0 || elapsedMs > 0) && (
              <span className="ml-auto inline-flex items-center gap-2 font-mono text-[10px] text-ink-500">
                {callCount > 0 && (
                  <span className="px-1.5 py-0.5 rounded bg-cyan-50 border border-cyan-200 text-cyan-800">
                    {callCount} call{callCount === 1 ? "" : "s"}
                  </span>
                )}
                {elapsedMs > 0 && (
                  <span className="px-1.5 py-0.5 rounded bg-ink-100 border border-ink-200 text-ink-700">
                    {(elapsedMs / 1000).toFixed(1)}s
                  </span>
                )}
              </span>
            )}
            {error && <span className="text-red-600">⚠ {error}</span>}
          </div>

          {/* Decoded search plan — exactly what we sent to the data source */}
          {queryPlan && (
            <div className="flex items-center gap-1.5 mb-1 flex-wrap">
              <span className="text-[10px] uppercase tracking-wide text-ink-400 mr-1">search plan</span>
              {queryPlan.search_keyword && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-cyan-50 border border-cyan-200 text-cyan-800 text-[10px] font-mono">
                  keyword<span className="text-cyan-500">·</span>{queryPlan.search_keyword}
                </span>
              )}
              {queryPlan.location_name && (
                <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-mono border ${
                  queryPlan.location_geo_id ? "bg-emerald-50 border-emerald-200 text-emerald-800" : "bg-ink-50 border-ink-200 text-ink-600"
                }`}>
                  loc<span className="opacity-50">·</span>{queryPlan.location_name}
                  {queryPlan.location_geo_id && <span className="opacity-60">#{queryPlan.location_geo_id}</span>}
                  {!queryPlan.location_geo_id && <span title="not in static map — passed unresolved" className="opacity-60">?</span>}
                </span>
              )}
              {queryPlan.industry_name && (
                <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-mono border ${
                  queryPlan.industry_id ? "bg-violet-50 border-violet-200 text-violet-800" : "bg-ink-50 border-ink-200 text-ink-600"
                }`}>
                  industry<span className="opacity-50">·</span>{queryPlan.industry_name}
                  {queryPlan.industry_id && <span className="opacity-60">#{queryPlan.industry_id}</span>}
                  {!queryPlan.industry_id && <span title="not in static map — filter omitted" className="opacity-60">?</span>}
                </span>
              )}
              {queryPlan.technology && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-indigo-50 border border-indigo-200 text-indigo-800 text-[10px] font-mono">
                  tech<span className="opacity-50">·</span>{queryPlan.technology}
                </span>
              )}
              {(queryPlan.employee_min != null || queryPlan.employee_max != null) && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-amber-50 border border-amber-200 text-amber-800 text-[10px] font-mono">
                  size<span className="opacity-50">·</span>{queryPlan.employee_min ?? "?"}–{queryPlan.employee_max ?? "?"}
                </span>
              )}
            </div>
          )}

          {/* Producer-empty banner with hint */}
          {emptyHint && (
            <div className="mb-1 px-2 py-1 rounded bg-amber-50 border border-amber-200 text-amber-800 text-[11px]">
              <span className="font-semibold">No primary results.</span> {emptyHint}
            </div>
          )}

          {/* Stage timeline */}
          {stageLog.length > 0 && (
            <div className="flex items-center gap-1.5 overflow-x-auto pb-1">
              {stageLog.map((s, i) => (
                <span key={i} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-white border border-ink-200 text-[10px] font-mono whitespace-nowrap shadow-sm">
                  <span className={s.label.startsWith("✓") ? "text-accent-700" :
                                   s.label.startsWith("∅") ? "text-amber-700" :
                                   s.label.startsWith("↻") || s.label.startsWith("→") ? "text-violet-700" :
                                   "text-ink-600"}>{s.label}</span>
                  {s.count != null && <span className="text-ink-400">· {s.count}</span>}
                </span>
              ))}
            </div>
          )}

          {/* Live research console — auto-open while generating so the user
              actually sees the firehose of upstream API calls. Color-coded:
              ◆ = source_call, ⚠ = error, ✓ = milestone. */}
          {events.length > 0 && (
            <details className="mt-1" open={isGenerating}>
              <summary className="cursor-pointer text-[10px] uppercase tracking-wide text-ink-400 hover:text-ink-600 select-none">
                <span className="inline-flex items-center gap-2">
                  <span>research console</span>
                  <span className="px-1 rounded bg-ink-200 text-ink-700">{events.length}</span>
                  {isGenerating && (
                    <span className="inline-flex items-center gap-1 text-emerald-600">
                      <span className="relative flex h-1.5 w-1.5">
                        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                        <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-emerald-500"></span>
                      </span>
                      streaming
                    </span>
                  )}
                </span>
              </summary>
              <div className="mt-1 max-h-56 overflow-y-auto bg-ink-900/95 text-ink-100 rounded p-2 font-mono text-[10px] leading-relaxed border border-ink-800">
                {events.map((e, i) => {
                  const isCall = e.startsWith("◆");
                  const isWarn = e.startsWith("⚠");
                  const isOk = e.startsWith("✓");
                  const cls = isWarn ? "text-amber-300"
                            : isOk ? "text-emerald-300"
                            : isCall ? "text-cyan-300"
                            : "text-ink-100";
                  return (
                    <div key={i} className="whitespace-pre-wrap break-all">
                      <span className="text-ink-500 mr-2">{String(events.length - i).padStart(3, "0")}</span>
                      <span className={cls}>{e}</span>
                    </div>
                  );
                })}
              </div>
            </details>
          )}
        </div>
      )}

      {/* Grid */}
      <div className="flex-1 min-h-0 grid-wrap bg-white">
        {sheet.rows.length === 0 ? (
          <div className="h-full grid place-items-center text-center p-8">
            <div>
              <div className="text-3xl mb-2">玄</div>
              <h2 className="text-lg font-semibold text-ink-900 mb-1">Empty sheet</h2>
              <p className="text-sm text-ink-500 mb-5">Click Generate to fill 15 rows from your headers and query.</p>
              <button className="btn-primary" onClick={generate} disabled={isGenerating}>
                {isGenerating ? "Working…" : "Generate rows"}
              </button>
            </div>
          </div>
        ) : (
          <DataEditor
            columns={columns}
            rows={sheet.rows.length}
            getCellContent={getCell}
            onCellEdited={onCellEdited}
            onGridSelectionChange={(sel) => {
              const c = sel.current?.cell;
              if (c) setSelectedCell({ col: c[0], row: c[1] });
              else setSelectedCell(null);
            }}
            rowMarkers="number"
            smoothScrollX
            smoothScrollY
            width="100%"
            height="100%"
          />
        )}
      </div>

      {/* Footer / inspector */}
      <div className="border-t border-ink-200 bg-white px-3 sm:px-6 py-2 sm:py-3 flex flex-wrap items-center gap-2 sm:gap-3 shrink-0 text-xs">
        <div className="flex gap-1 sm:gap-2">
          <button className="btn-ghost px-2 sm:px-3" onClick={addRow}>+ <span className="hidden sm:inline">Add</span> row</button>
          <button className="btn-ghost px-2 sm:px-3" onClick={deleteSelectedRow} disabled={!selectedCell}><span className="hidden sm:inline">Delete row</span><span className="sm:hidden">Delete</span></button>
          <button className="btn-ghost px-2 sm:px-3" onClick={reEnrichSelected} disabled={!selectedCell}>Re-enrich<span className="hidden sm:inline"> cell</span></button>
          <button className="btn-ghost px-2 sm:px-3" onClick={fillBlanks} disabled={filling || !sheet.rows.length} title="Run SERP through Netrows for every blank cell">
            {filling ? "Filling…" : <>Fill <span className="hidden sm:inline">blanks</span><span className="sm:hidden">▢</span></>}
          </button>
        </div>
        <div className="ml-auto flex items-center flex-wrap gap-2 sm:gap-3 text-ink-600">
          {selectedCell ? (
            <>
              <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-cyan-50 border border-cyan-200 text-cyan-800 font-mono text-[11px]">
                {colLabel(selectedCell.col)}{selectedCell.row + 1}
              </span>
              <span className="font-mono text-ink-700">{sheet.headers[selectedCell.col]}</span>
              {selectedCellMeta && (
                <>
                  <span className={`chip ${confidenceClass(selectedCellMeta.confidence || "low")}`}>{selectedCellMeta.confidence || "low"}</span>
                  <span>source: {selectedCellMeta.source || "—"}</span>
                  {selectedCellMeta.verification && (
                    <span>verify: {selectedCellMeta.verification.status}{selectedCellMeta.verification.reason ? ` (${selectedCellMeta.verification.reason})` : ""}</span>
                  )}
                </>
              )}
            </>
          ) : <span>Click a cell for details</span>}
          <span className="ml-3">{sheet.rows.length} rows · {sheet.headers.length} cols</span>
        </div>
      </div>
    </div>
  );
}

function colLabel(col: number) {
  let s = ""; let n = col;
  while (n >= 0) { s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26) - 1; }
  return s;
}

function confidenceClass(c: string) {
  switch (c) {
    case "verified": return "bg-accent-100 text-accent-700";
    case "high": return "bg-emerald-100 text-emerald-700";
    case "medium": return "bg-ink-100 text-ink-700";
    case "low": return "bg-stone-100 text-stone-600";
    case "uncertain": return "bg-amber-100 text-amber-800";
    case "invalid": return "bg-red-100 text-red-700";
    case "user": return "bg-blue-100 text-blue-700";
    default: return "bg-ink-100 text-ink-700";
  }
}
