import { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api } from "../api";
import type { Template } from "../types";

const SUGGESTED_HEADERS = [
  "Company Name", "Contact Name", "Title", "Email", "Phone", "Website",
  "LinkedIn URL", "GitHub URL", "Industry", "Location", "Employee Count",
  "Funding", "Tech Stack", "Languages", "Bio", "Rating", "Address",
];

export default function NewSheet() {
  const nav = useNavigate();
  const [params] = useSearchParams();
  const tplId = params.get("template");
  const [name, setName] = useState("");
  const [query, setQuery] = useState("");
  const [headers, setHeaders] = useState<string[]>(["Company Name", "Website", "Email", "Industry"]);
  const [draft, setDraft] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [generateNow, setGenerateNow] = useState(true);

  useEffect(() => {
    if (!tplId) return;
    api.templates().then((ts) => {
      const t: Template | undefined = ts.find((x) => x.id === tplId);
      if (t) {
        setName(t.name);
        setHeaders(t.headers);
        if (t.suggested_query) setQuery(t.suggested_query);
      }
    });
  }, [tplId]);

  const addHeader = (h: string) => {
    const v = h.trim();
    if (!v || headers.includes(v)) return;
    setHeaders([...headers, v]);
    setDraft("");
  };
  const removeHeader = (h: string) => setHeaders(headers.filter((x) => x !== h));

  const submit = async () => {
    if (submitting) return;
    if (!name.trim()) { alert("Give the sheet a name"); return; }
    if (headers.length === 0) { alert("Add at least one header"); return; }
    setSubmitting(true);
    try {
      const sheet = await api.createSheet(name.trim(), headers, query.trim());
      if (generateNow) {
        await api.generate(sheet.id, { row_limit: 15 });
      }
      nav(`/s/${sheet.id}`);
    } catch (e: any) {
      alert(e.message || String(e));
      setSubmitting(false);
    }
  };

  return (
    <div className="p-4 sm:p-8 overflow-auto h-full">
      <div className="max-w-3xl mx-auto">
        <h1 className="text-2xl font-semibold tracking-tight text-ink-900 mb-1">Create a sheet</h1>
        <p className="text-sm text-ink-500 mb-8">Define your columns and an optional natural-language query. Genshi fills the rest.</p>

        <div className="space-y-6">
          <div>
            <label className="label">Sheet name</label>
            <input className="input" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Texas SaaS founders" />
          </div>

          <div>
            <label className="label">Query (optional)</label>
            <textarea
              className="input min-h-[80px]"
              value={query} onChange={(e) => setQuery(e.target.value)}
              placeholder='e.g. "Series A B2B SaaS founders in Austin, TX with public GitHub"'
            />
            <p className="text-xs text-ink-500 mt-1.5">The smarter your query, the smarter the source routing.</p>
          </div>

          <div>
            <label className="label">Columns ({headers.length})</label>
            <div className="card p-3 space-y-3">
              <div className="flex flex-wrap gap-1.5">
                {headers.map((h) => (
                  <span key={h} className="chip bg-ink-100 text-ink-800 pr-1">
                    {h}
                    <button onClick={() => removeHeader(h)} className="ml-1 px-1 text-ink-500 hover:text-red-600">×</button>
                  </span>
                ))}
                {headers.length === 0 && <span className="text-xs text-ink-500">No columns yet</span>}
              </div>
              <div className="flex gap-2">
                <input
                  className="input"
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); addHeader(draft); } }}
                  placeholder="Add a column header…"
                />
                <button className="btn-secondary" onClick={() => addHeader(draft)}>Add</button>
              </div>
              <div className="flex flex-wrap gap-1.5">
                {SUGGESTED_HEADERS.filter((h) => !headers.includes(h)).map((h) => (
                  <button key={h} onClick={() => addHeader(h)} className="chip bg-white border border-ink-200 text-ink-600 hover:bg-ink-50">
                    + {h}
                  </button>
                ))}
              </div>
            </div>
          </div>

          <label className="flex items-center gap-2 text-sm text-ink-700">
            <input type="checkbox" checked={generateNow} onChange={(e) => setGenerateNow(e.target.checked)} />
            Generate 15 rows immediately
          </label>

          <div className="flex justify-end gap-3 pt-2">
            <button className="btn-secondary" onClick={() => nav(-1)}>Cancel</button>
            <button className="btn-primary" onClick={submit} disabled={submitting}>
              {submitting ? "Creating…" : generateNow ? "Create & generate" : "Create"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
