import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api";
import type { Template } from "../types";

export default function Templates() {
  const [templates, setTemplates] = useState<Template[]>([]);
  useEffect(() => { api.templates().then(setTemplates); }, []);

  return (
    <div className="p-4 sm:p-8 overflow-auto h-full">
      <div className="max-w-5xl mx-auto">
        <h1 className="text-2xl font-semibold tracking-tight text-ink-900 mb-1">Templates</h1>
        <p className="text-sm text-ink-500 mb-8">Pre-tuned header sets for common lead-gen plays. Click one to start.</p>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {templates.map((t) => (
            <Link key={t.id} to={`/new?template=${t.id}`} className="card p-5 block hover:border-ink-400 transition-colors">
              <div className="flex items-center gap-2 mb-1">
                <h2 className="font-semibold text-ink-900">{t.name}</h2>
                {t.builtin ? <span className="chip bg-accent-100 text-accent-700">builtin</span> : null}
              </div>
              <p className="text-sm text-ink-600 mb-3">{t.description}</p>
              <div className="flex flex-wrap gap-1">
                {t.headers.map((h) => (
                  <span key={h} className="chip bg-ink-100 text-ink-700">{h}</span>
                ))}
              </div>
              {t.suggested_query && (
                <div className="text-xs text-ink-500 mt-3 font-mono truncate">"{t.suggested_query}"</div>
              )}
            </Link>
          ))}
        </div>
      </div>
    </div>
  );
}
