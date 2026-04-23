from .db import SessionLocal
from .models import Template

BUILTIN = [
    {
        "name": "B2B Lead List",
        "description": "Companies + decision-makers + verified emails for B2B outbound.",
        "headers": ["Company Name", "Contact Name", "Title", "Email", "Phone", "LinkedIn URL", "Website"],
        "suggested_query": "B2B SaaS companies in San Francisco, 50-200 employees",
    },
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
    with SessionLocal() as db:
        if db.query(Template).count() > 0:
            return
        for t in BUILTIN:
            db.add(Template(name=t["name"], description=t["description"], headers=t["headers"], suggested_query=t["suggested_query"], builtin=1))
        db.commit()
