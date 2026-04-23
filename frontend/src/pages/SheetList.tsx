import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api";
import type { SheetSummary } from "../types";

export default function SheetList() {
  const [sheets, setSheets] = useState<SheetSummary[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = () => api.listSheets().then((s) => { setSheets(s); setLoading(false); });
  useEffect(() => { refresh(); }, []);

  const remove = async (id: string) => {
    if (!confirm("Delete this sheet?")) return;
    await api.deleteSheet(id);
    refresh();
  };

  return (
    <div className="p-4 sm:p-8 overflow-auto h-full">
      <div className="max-w-5xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-ink-900">Your sheets</h1>
            <p className="text-sm text-ink-500 mt-1">Headers in. Real data out.</p>
          </div>
          <Link to="/new" className="btn-primary">+ New sheet</Link>
        </div>

        {loading ? (
          <div className="text-ink-500 text-sm">Loading…</div>
        ) : sheets.length === 0 ? (
          <div className="card p-12 text-center">
            <div className="text-3xl mb-3">玄</div>
            <h2 className="text-lg font-semibold text-ink-900 mb-1">No sheets yet</h2>
            <p className="text-sm text-ink-500 mb-6">Pick a template or define your own headers to get started.</p>
            <div className="flex gap-3 justify-center">
              <Link to="/templates" className="btn-secondary">Browse templates</Link>
              <Link to="/new" className="btn-primary">Create blank sheet</Link>
            </div>
          </div>
        ) : (
          <div className="card divide-y divide-ink-100">
            {sheets.map((s) => (
              <div key={s.id} className="px-5 py-4 flex items-center gap-4 hover:bg-ink-50">
                <Link to={`/s/${s.id}`} className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <div className="font-medium text-ink-900 truncate">{s.name}</div>
                    <StatusChip status={s.status} />
                  </div>
                  <div className="text-xs text-ink-500 mt-0.5 truncate">
                    {s.row_count} rows · {s.headers.slice(0, 6).join(" · ")}
                    {s.headers.length > 6 ? ` · +${s.headers.length - 6}` : ""}
                  </div>
                </Link>
                <div className="text-xs text-ink-400">{new Date(s.updated_at).toLocaleDateString()}</div>
                <button onClick={() => remove(s.id)} className="btn-ghost text-ink-500 hover:text-red-600">Delete</button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function StatusChip({ status }: { status: SheetSummary["status"] }) {
  const styles: Record<string, string> = {
    draft: "bg-ink-100 text-ink-700",
    generating: "bg-amber-100 text-amber-800",
    ready: "bg-accent-100 text-accent-700",
    error: "bg-red-100 text-red-700",
  };
  return <span className={`chip ${styles[status] || ""}`}>{status}</span>;
}
