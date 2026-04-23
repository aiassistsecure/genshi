from .db import SessionLocal
from .models import Template

# Built-in templates. Each `suggested_query` is hand-tuned to give the LLM
# query planner (backend/enrichment/query_planner.py) the strongest possible
# decomposition signal: a concrete role/keyword + a real location + an
# industry/tech anchor + an employee-size bracket where applicable. Vague
# queries dilute results because the planner falls back to broad keyword-only
# searches. Concrete queries narrow the LinkedIn filter set without throwing
# away coverage (the orchestrator still paginates 3 pages and falls back to
# Google search if the structured filters return nothing).
BUILTIN = [
    # ------------------------------------------------------------------ B2B
    {
        "name": "B2B Lead List",
        "description": "Companies + decision-makers + verified emails for B2B outbound.",
        "headers": ["Company Name", "Contact Name", "Title", "Email", "Phone", "LinkedIn URL", "Website"],
        "suggested_query": "B2B SaaS companies in San Francisco, 50-200 employees",
    },
    {
        "name": "B2B Outbound (Targeted)",
        "description": "Sharper B2B list scoped to a specific tech stack + funding stage. Pairs the keyword with industry, geo, size band, and tech so the planner emits structured filters rather than a generic keyword search.",
        "headers": [
            "Company Name", "Decision Maker", "Title", "Email", "Phone",
            "LinkedIn URL", "Website", "Industry", "Employee Count",
            "Tech Stack", "City", "Funding Stage",
        ],
        "suggested_query": "Series A-B B2B SaaS companies in California using AWS, 50-200 employees, target VP Engineering or CTO",
    },
    # --------------------------------------------------------- Job planning
    {
        "name": "Job Market Intel",
        "description": "Open roles + comp + team shape for headcount planning, salary benchmarking, and competitive hiring research. Sources from LinkedIn Jobs.",
        "headers": [
            "Role / Title", "Company", "Location", "Seniority", "Required Skills",
            "Tech Stack", "Posted Date", "Salary Range", "Job URL", "Company Size",
        ],
        "suggested_query": "Senior platform engineer roles at Series B fintech companies in New York, 50-500 employees, posted in the last 30 days",
    },
    # ----------------------------------------------------- Hiring / sourcing
    {
        "name": "Talent Sourcing",
        "description": "Active and passive candidates with verified contact info + developer signals (GitHub, repos, languages). Use a concrete role + location + tech stack to keep the funnel tight.",
        "headers": [
            "Name", "Current Title", "Current Company", "Location",
            "LinkedIn URL", "Email", "GitHub URL", "Years Experience",
            "Top Skills", "Notable Repos",
        ],
        "suggested_query": "Senior React engineers in Austin with open-source contributions, 5+ years experience, currently at a Series A-C startup",
    },
    # --------------------------------------------------- existing originals
    {
        "name": "Tech Founders",
        "description": "Founders and CTOs with GitHub + LinkedIn + funding context.",
        "headers": ["Name", "Company", "Title", "GitHub URL", "LinkedIn URL", "Email", "Funding", "Tech Stack"],
        "suggested_query": "Series A AI startup founders with active GitHub",
    },
    {
        "name": "Developer Leads",
        "description": "Open-source developers with public emails for tooling outreach.",
        "headers": ["Name", "GitHub URL", "Email", "Location", "Languages", "Repos", "Bio"],
        "suggested_query": "Python backend developers in Berlin open to work",
    },
    {
        "name": "Company Research",
        "description": "Deep company profile for account research.",
        "headers": ["Company Name", "Industry", "HQ", "Employee Count", "Revenue", "Website", "LinkedIn URL", "Funding"],
        "suggested_query": "Mid-market fintech companies in NYC",
    },
    {
        "name": "Agency Finder",
        "description": "Dev agencies & consultancies sized for partnership.",
        "headers": ["Company Name", "Website", "GitHub URL", "Employee Count", "Tech Stack", "Location", "Email"],
        "suggested_query": "React Native development agencies in North America",
    },
    {
        "name": "Local Business",
        "description": "Brick-and-mortar businesses with phone + address from Maps.",
        "headers": ["Company Name", "Address", "Phone", "Website", "Rating", "Industry"],
        "suggested_query": "Specialty coffee roasters in downtown Chicago",
    },
]


def seed_templates():
    """Upsert built-in templates by name. Existing user-edited rows are left
    alone; missing built-ins are inserted; built-ins whose canonical
    description/headers/query have been updated here are refreshed in place
    (only when `builtin=1`, so we never clobber a user's customizations)."""
    with SessionLocal() as db:
        existing = {t.name: t for t in db.query(Template).all()}
        changed = False
        for t in BUILTIN:
            row = existing.get(t["name"])
            if row is None:
                db.add(Template(
                    name=t["name"], description=t["description"],
                    headers=t["headers"], suggested_query=t["suggested_query"],
                    builtin=1,
                ))
                changed = True
            elif row.builtin == 1:
                # Refresh canonical built-ins so prompt-tuning improvements
                # ship without requiring a fresh DB.
                if (row.description != t["description"]
                        or row.headers != t["headers"]
                        or row.suggested_query != t["suggested_query"]):
                    row.description = t["description"]
                    row.headers = t["headers"]
                    row.suggested_query = t["suggested_query"]
                    changed = True
        if changed:
            db.commit()
