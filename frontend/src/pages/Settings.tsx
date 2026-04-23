import { useEffect, useState } from "react";
import { api } from "../api";

interface ModelInfo { id: string; name: string; context_window?: number; max_output?: number; provider?: string }
interface Provider { id: string; name: string; is_default?: boolean; models: ModelInfo[] }

const COMBO_KEY = "genshi.aiassist_combo"; // "<provider>::<model>"

function loadCombo(): { provider: string; model: string } {
  const raw = localStorage.getItem(COMBO_KEY);
  if (raw && raw.includes("::")) {
    const [provider, model] = raw.split("::");
    return { provider, model };
  }
  // Backward compat
  return {
    provider: localStorage.getItem("genshi.aiassist_provider") || "",
    model: localStorage.getItem("genshi.aiassist_model") || "",
  };
}

function saveCombo(provider: string, model: string) {
  if (provider && model) {
    localStorage.setItem(COMBO_KEY, `${provider}::${model}`);
    localStorage.setItem("genshi.aiassist_provider", provider);
    localStorage.setItem("genshi.aiassist_model", model);
  } else {
    localStorage.removeItem(COMBO_KEY);
    localStorage.removeItem("genshi.aiassist_provider");
    localStorage.removeItem("genshi.aiassist_model");
  }
}

export default function Settings() {
  const initial = loadCombo();
  const [netrows, setNetrows] = useState(localStorage.getItem("genshi.netrows_key") || "");
  const [aiassist, setAiassist] = useState(localStorage.getItem("genshi.aiassist_key") || "");
  const [combo, setCombo] = useState(initial.provider && initial.model ? `${initial.provider}::${initial.model}` : "");
  const [server, setServer] = useState<{ has_netrows_key: boolean; has_aiassist_key: boolean } | null>(null);
  const [providers, setProviders] = useState<Provider[] | null>(null);
  const [loadingModels, setLoadingModels] = useState(false);
  const [modelsError, setModelsError] = useState("");
  const [savedAt, setSavedAt] = useState(0);

  useEffect(() => { api.health().then(setServer).catch(() => setServer(null)); }, []);

  const loadModels = async () => {
    setLoadingModels(true);
    setModelsError("");
    try {
      const data = await api.providers(aiassist || undefined);
      setProviders(data.providers || []);
    } catch (e: any) {
      setModelsError(e.message || String(e));
      setProviders(null);
    } finally {
      setLoadingModels(false);
    }
  };

  useEffect(() => {
    if (aiassist || server?.has_aiassist_key) loadModels();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [server?.has_aiassist_key]);

  const save = () => {
    if (netrows) localStorage.setItem("genshi.netrows_key", netrows); else localStorage.removeItem("genshi.netrows_key");
    if (aiassist) localStorage.setItem("genshi.aiassist_key", aiassist); else localStorage.removeItem("genshi.aiassist_key");
    if (combo && combo.includes("::")) {
      const [p, m] = combo.split("::");
      saveCombo(p, m);
    } else {
      saveCombo("", "");
    }
    setSavedAt(Date.now());
  };

  const totalModels = providers ? providers.reduce((n, p) => n + p.models.length, 0) : 0;

  return (
    <div className="p-4 sm:p-8 overflow-auto h-full">
      <div className="max-w-2xl mx-auto">
        <h1 className="text-2xl font-semibold tracking-tight text-ink-900 mb-1">Settings</h1>
        <p className="text-sm text-ink-500 mb-8">
          Bring your own keys. They live only in <span className="font-mono text-ink-700">localStorage</span> on this device — never stored server-side, only forwarded per-request to the relevant API.
          {server && (
            <>
              {" "}Server fallback:{" "}
              <span className={server.has_netrows_key ? "text-accent-700" : "text-ink-500"}>Netrows</span>
              {", "}
              <span className={server.has_aiassist_key ? "text-accent-700" : "text-ink-500"}>AiAssist</span>.
            </>
          )}
        </p>

        <div className="card p-6 space-y-5">
          <div>
            <label className="label">Netrows API key</label>
            <input type="password" className="input font-mono" value={netrows} onChange={(e) => setNetrows(e.target.value)} placeholder="nrk_live_…" />
            <p className="text-xs text-ink-500 mt-1.5">Drives all data scraping. Get one at <a className="underline" href="https://netrows.com" target="_blank" rel="noreferrer">netrows.com</a>.</p>
          </div>
          <div>
            <label className="label">AiAssist API key</label>
            <div className="flex gap-2">
              <input type="password" className="input font-mono" value={aiassist} onChange={(e) => setAiassist(e.target.value)} placeholder="aai_…" />
              <button className="btn-secondary whitespace-nowrap" onClick={loadModels} disabled={loadingModels}>
                {loadingModels ? "Loading…" : "Reload models"}
              </button>
            </div>
            <p className="text-xs text-ink-500 mt-1.5">Drives source routing, normalization, and signal scanning. Get one at <a className="underline" href="https://aiassist.net" target="_blank" rel="noreferrer">aiassist.net</a>.</p>
          </div>

          <div>
            <label className="label">Provider · Model</label>
            {providers && providers.length > 0 ? (
              <select
                className="input font-mono"
                value={combo}
                onChange={(e) => setCombo(e.target.value)}
              >
                <option value="">— Server default —</option>
                {providers.map((p) => (
                  <optgroup key={p.id} label={`${p.name}${p.is_default ? "  ★ default" : ""}`}>
                    {p.models.map((m) => (
                      <option key={`${p.id}::${m.id}`} value={`${p.id}::${m.id}`}>
                        {m.name} — {m.id}{m.context_window ? ` · ${(m.context_window / 1000).toFixed(0)}K ctx` : ""}
                      </option>
                    ))}
                  </optgroup>
                ))}
              </select>
            ) : (
              <input className="input font-mono" placeholder="Add an AiAssist key and reload models…" disabled />
            )}
            <div className="flex items-center justify-between mt-1.5">
              <p className="text-xs text-ink-500">
                {providers
                  ? `${totalModels} models across ${providers.length} providers · provider sent as X-AiAssist-Provider header`
                  : "Add an AiAssist key to load the live model list."}
              </p>
              {modelsError && <p className="text-xs text-red-600">{modelsError}</p>}
            </div>
          </div>

          <div className="flex items-center justify-between pt-2">
            <div className="text-xs text-ink-500">
              {savedAt ? "Saved." : "Changes apply on save."}
            </div>
            <button className="btn-primary" onClick={save}>Save</button>
          </div>
        </div>
      </div>
    </div>
  );
}
