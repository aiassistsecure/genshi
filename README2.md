# GenShi — Generate Sheets

> AI-powered spreadsheet generator. Define your columns, describe what you need, and watch 15 rows of real-world B2B data appear — verified, enriched, and ready to export.

---

## What it does

GenShi connects to **Netrows** (280+ B2B data endpoints) to pull verified real-world records and then uses **AiAssist** (LLM enrichment layer) to fill in the gaps — all streamed live into an in-browser spreadsheet. No copy-pasting, no manual research, no hallucinated data.

- Type a query like *"SaaS companies in Austin with fewer than 200 employees"*
- Define your column headers (or pick a template)
- Hit **Generate** — 15 rows stream in, cell by cell, via SSE
- Click **Fill blanks** to run a SERP pass on every empty cell using real search results
- Export to CSV or XLSX

---

## Features

| Feature | Details |
|---|---|
| Live SSE streaming | Rows appear cell-by-cell as they're enriched — no waiting for a full batch |
| Netrows integration | 280+ B2B endpoints: companies, people, emails, funding, firmographics, and more |
| LLM enrichment | AiAssist LLM fills columns that Netrows can't source structurally |
| Fill blanks | One Netrows SERP per row → LLM extracts all missing fields from snippets in one shot |
| Re-enrich cell | Click any cell and re-run enrichment with updated context |
| Email verification | DNS + SMTP verification baked in |
| CSV / XLSX export | One-click export of the full sheet |
| Templates | Pre-built column sets for common use cases (lead gen, market research, hiring, etc.) |
| BYOK settings | Bring your own Netrows + AiAssist API keys; swap model / provider per-session |
| Cell provenance | Every cell tagged with `source` and `confidence` for full transparency |

---

## Stack

**Backend**
- Python 3.11 + FastAPI
- SQLite via SQLAlchemy (swap to Postgres in one env var)
- SSE via `sse-starlette`
- XLSX export via `openpyxl`
- Email verification via `dnspython` + `email-validator`

**Frontend**
- React 18 + Vite + TypeScript
- Glide Data Grid (high-performance spreadsheet renderer)
- Tailwind CSS

---

## Setup

### 1. Clone & install backend dependencies

```bash
git clone https://github.com/aiassistsecure/genshi.git
cd genshi

# Using uv (recommended)
uv sync

# Or pip
pip install -e .
```

### 2. Install frontend dependencies

```bash
cd frontend
npm install
```

### 3. Set environment variables

```env
NETROWS_API_KEY=your_netrows_key
AIASSIST_API_KEY=your_aiassist_key
SESSION_SECRET=a_long_random_string
```

> You can also set these per-session in the **Settings** panel inside the app — useful for BYOK workflows.

### 4. Run

```bash
# Terminal 1 — backend
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 — frontend
cd frontend && npm run dev
```

Open `http://localhost:5000`.

---

## How Fill Blanks works

1. For each row, GenShi builds an *identity string* from the strongest available values (company name, domain, contact name, etc.)
2. It appends the names of blank column headers to form a targeted SERP query
3. One **Netrows google_search** fires per row (concurrency-limited to 5)
4. The top 10 snippets are sent to the LLM in a single call — it extracts values for **all** blank fields at once
5. Filled cells are tagged `source: "serp"`, `confidence: "medium"` and persisted immediately

This design minimizes both API calls and LLM tokens while keeping data grounded in real web results.

---

## API overview

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/sheets` | List all sheets |
| `POST` | `/api/sheets` | Create sheet + start generation |
| `GET` | `/api/sheets/{id}` | Get sheet with all rows |
| `GET` | `/api/sheets/{id}/stream` | SSE stream of enrichment events |
| `POST` | `/api/sheets/{id}/fill-blanks` | Fill all blank cells via SERP + LLM |
| `PATCH` | `/api/sheets/{id}/rows/{rowId}/cells/{header}` | Update / re-enrich a single cell |
| `GET` | `/api/sheets/{id}/export?format=csv` | Export as CSV or XLSX |
| `GET` | `/api/templates` | List available column templates |

---

## License

This project is licensed under the **Business Source License 1.1**.

- **Licensor:** Interchained LLC - Mark Allen Evans
- **Licensed Work:** GenShi | Generate Sheets
- **Change Date:** 2030-04-09
- **Change License:** Apache License 2.0

Free for non-commercial and non-competing use. On the Change Date, this software converts to Apache 2.0 for everyone.

For commercial licensing or partnership inquiries: dev@interchained.org

See [LICENSE](./LICENSE) for the full terms.

---

## Contributing

Pull requests are welcome for bug fixes and non-competing improvements. For major features or commercial integrations, please open an issue first to discuss.

---

*Built by [Interchained LLC](https://interchained.org) · Engineered with Replit AI (April 2026)*

---

> Want to build something like this? Start on Replit — [replit.com/refer/interchained](https://replit.com/refer/interchained)
