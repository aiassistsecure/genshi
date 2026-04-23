"""Source planner. Given headers + query, decide which Netrows endpoints to call."""
from __future__ import annotations
from typing import Optional
from ..sources.header_map import sources_for_headers, COMPANY_PRODUCERS, PERSON_PRODUCERS, needs_email
from .llm import chat_json, LLMError


AVAILABLE = [
    "linkedin_people", "linkedin_companies",
    "github_user", "github_user_repos", "github_org", "github_repo",
    "email_finder", "google_search", "google_maps",
    "indeed_jobs", "indeed_company", "indeed_company_reviews", "indeed_company_salaries",
    "crunchbase_company", "crunchbase_person",
    "yc_search",
]


def _normalize(h: str) -> str:
    return (h or "").lower().replace("_", " ").strip()


# Headers that strongly imply a person-shaped row.
PERSON_HEADERS = {
    "name", "full name", "first name", "last name", "title", "headline",
    "job title", "role", "bio", "linkedin url", "school", "education",
    "summary",
}

# Headers that strongly imply a company/org-shaped row.
COMPANY_HEADERS = {
    "company name", "website", "domain", "employees", "employee count",
    "industry", "headquarters", "hq", "year founded", "founded",
    "team size", "company size", "tagline", "logo", "logo url",
    "funding", "investors", "operating status", "ipo status",
}

# When ≥2 of these are present, the user wants GitHub data. The primary depends
# on whether the row shape is person- or company-oriented (GitHub has no
# user-search endpoint — github_enrich then hydrates per-row from the primary).
GITHUB_HEAVY_HEADERS = {
    "github url", "github", "repos", "public repos", "stars", "followers",
    "languages", "tech stack", "bio", "avatar",
}

# Place-shaped headers — primary should be google_maps, not search/linkedin.
PLACE_HEAVY_HEADERS = {
    "address", "phone", "phone number", "hours", "opening hours",
    "rating", "review count", "categories", "latitude", "longitude",
}


def _shape_hint(headers: list[str]) -> Optional[str]:
    """Return a forced-primary source when headers strongly imply a shape."""
    nh = {_normalize(h) for h in headers}
    if len(nh & GITHUB_HEAVY_HEADERS) >= 2:
        # Pick person- vs company-primary based on which signal set wins.
        # We can safely assume any branded "Company Name" implies an org.
        company_score = len(nh & COMPANY_HEADERS)
        person_score = len(nh & PERSON_HEADERS)
        if company_score > person_score:
            return "linkedin_companies"  # github_enrich → org branch
        return "linkedin_people"         # github_enrich → user branch
    if len(nh & PLACE_HEAVY_HEADERS) >= 2:
        return "google_maps"
    return None


async def plan_sources(headers: list[str], query: str, api_key: Optional[str] = None, model: Optional[str] = None, provider: Optional[str] = None) -> list[str]:
    """Returns ordered list of Netrows source IDs to call. Falls back to header map if LLM fails."""
    base = sources_for_headers(headers)
    forced = _shape_hint(headers)

    if not query.strip():
        if forced: base = [forced] + [s for s in base if s != forced]
        return _ensure_producer(base)

    sys = (
        "You are a data source router for a B2B lead generation tool. "
        "Given the user's column headers and natural-language query, pick which data sources to call. "
        f"Available sources: {AVAILABLE}. "
        "ROUTING RULES:\n"
        "- If headers include github_url/repos/bio/languages/tech_stack, the FIRST source MUST be linkedin_people "
        "(GitHub has no user search; we hydrate GitHub data per-row from the LinkedIn results).\n"
        "- If headers include phone/address/hours/rating, the FIRST source MUST be google_maps.\n"
        "- If headers describe individual people (name, title, email, linkedin), prefer linkedin_people over google_search.\n"
        "- Use google_search ONLY when headers are about generic web entities (urls, news, articles).\n"
        "- For company-level B2B data, use linkedin_companies as primary.\n"
        "Respond with strict JSON: {\"sources\": [\"source_id\", ...]} ordered by importance, max 6 sources."
    )
    user = f"Headers: {headers}\nQuery: {query}\nReturn only valid sources from the list."
    try:
        out = await chat_json(
            [{"role": "system", "content": sys}, {"role": "user", "content": user}],
            api_key=api_key, model=model, provider=provider,
        )
        if isinstance(out, dict): picked = out.get("sources", [])
        elif isinstance(out, list): picked = out
        else: picked = []
        picked = [s for s in picked if s in AVAILABLE]
        if not picked: picked = base
    except LLMError:
        picked = base

    # Hard override: even if the LLM ignored the routing rules, force the
    # shape-matched primary to the front of the list.
    if forced:
        picked = [forced] + [s for s in picked if s != forced]

    # Email finder is implied if email header present
    if needs_email(headers) and "email_finder" not in picked:
        picked.append("email_finder")
    return _ensure_producer(picked)


def _ensure_producer(sources: list[str]) -> list[str]:
    """Make sure at least one company/person producer is in the list."""
    if not any(s in COMPANY_PRODUCERS or s in PERSON_PRODUCERS for s in sources):
        sources = ["google_search"] + sources
    return sources
