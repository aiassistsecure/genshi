"""Query planner — decompose a natural-language sheet query into provider-native parameters.

The orchestrator used to send the raw sentence (e.g. "Video Game Producers in
California working with Unity") straight into Netrows' `keyword` field. That
field is matched against company names + taglines, not semantically — so niche
queries returned 0 results while still consuming credits.

This module runs ONE LLM call per generation (cheap) to produce:

    {
      "search_keyword": "video game studio Unity",   # tokens that actually appear in names/taglines
      "location_name":  "California",
      "industry_name":  "Computer Games",
      "technology":     "Unity",
      "employee_min":   None,
      "employee_max":   None,
    }

Then resolves location/industry against a static LinkedIn ID map. Anything we
can't resolve falls back to the raw query — so this is strictly additive: a
miss can't be worse than the previous behavior.
"""
from __future__ import annotations
from typing import Optional, Any
from .llm import chat_json, LLMError


# LinkedIn geo IDs. Curated subset — covers the cases that matter for B2B
# prospecting (US states + major metros + top 25 countries). Lookup is
# case-insensitive on the normalized name. Add to this map as needed.
LINKEDIN_GEO_IDS: dict[str, str] = {
    # Countries
    "united states": "103644278", "usa": "103644278", "us": "103644278",
    "united kingdom": "101165590", "uk": "101165590", "england": "102299470",
    "canada": "101174742", "germany": "101282230", "france": "105015875",
    "japan": "101355337", "australia": "101452733", "india": "102713980",
    "netherlands": "102890719", "spain": "105646813", "italy": "103350119",
    "brazil": "106057199", "mexico": "103323778", "singapore": "102454443",
    "ireland": "104738515", "sweden": "105117694", "switzerland": "106693272",
    "poland": "105072130", "israel": "101620260", "south korea": "105149562",
    "china": "102890883", "uae": "104305776", "united arab emirates": "104305776",
    # US states
    "california": "102095887", "new york": "105080838", "new york state": "105080838",
    "texas": "102748797", "florida": "103655310", "illinois": "100027090",
    "washington": "100127156", "washington state": "100127156",
    "massachusetts": "101909856", "georgia": "100494997",
    "colorado": "104129202", "oregon": "100541821", "michigan": "101082330",
    "pennsylvania": "100126835", "ohio": "100395916", "north carolina": "104100866",
    "virginia": "104137919", "arizona": "102013126", "new jersey": "104354833",
    "minnesota": "102320960", "utah": "104251826", "nevada": "100184138",
    # US metros
    "san francisco bay area": "90000084", "bay area": "90000084", "sf bay area": "90000084",
    "greater new york city area": "90000070", "nyc": "90000070",
    "greater los angeles area": "90000049", "los angeles": "90000049", "la": "90000049",
    "greater seattle area": "90000079", "seattle": "90000079",
    "greater boston area": "90000007", "boston": "90000007",
    "greater chicago area": "90000031", "chicago": "90000031",
    "austin texas area": "90000005", "austin": "90000005",
    "denver metropolitan area": "90000037", "denver": "90000037",
    "miami fort lauderdale area": "90000056", "miami": "90000056",
    "atlanta metropolitan area": "90000004", "atlanta": "90000004",
    "washington dc-baltimore area": "90000097", "washington dc": "90000097",
    "san diego metropolitan area": "90000077", "san diego": "90000077",
    "portland oregon metropolitan area": "90000071", "portland": "90000071",
    # International metros
    "london area, united kingdom": "90009496", "london": "90009496",
    "paris, ile-de-france": "104246759", "paris": "104246759",
    "berlin metropolitan area": "100935308", "berlin": "100935308",
    "tokyo, japan": "90010046", "tokyo": "90010046",
    "toronto, ontario, canada": "90009551", "toronto": "90009551",
    "amsterdam area": "90009659", "amsterdam": "90009659",
    "remote": "92000000", "worldwide": "92000000",
}


# LinkedIn industry IDs. Subset covering ~80% of B2B searches we see.
LINKEDIN_INDUSTRY_IDS: dict[str, str] = {
    "computer games": "109", "video games": "109", "gaming": "109", "game development": "109",
    "computer software": "4", "software development": "4", "saas": "4",
    "internet": "6", "information technology and services": "96", "it services": "96",
    "information technology": "96", "it": "96",
    "computer hardware": "3", "consumer electronics": "24", "semiconductors": "7",
    "telecommunications": "8", "computer networking": "5",
    "financial services": "43", "banking": "41", "investment banking": "45",
    "venture capital & private equity": "106", "venture capital": "106",
    "insurance": "42", "accounting": "47",
    "marketing and advertising": "80", "marketing": "80", "advertising": "80",
    "public relations and communications": "98", "pr": "98",
    "design": "99", "graphic design": "140",
    "media production": "126", "entertainment": "28", "broadcast media": "36",
    "animation": "117", "motion pictures and film": "29", "film": "29",
    "music": "115", "publishing": "82",
    "e-learning": "132", "online media": "113", "elearning": "132",
    "education management": "69", "higher education": "68", "primary/secondary education": "67",
    "research": "70", "biotechnology": "12", "pharmaceuticals": "15",
    "hospital & health care": "14", "healthcare": "14", "medical devices": "17",
    "mental health care": "139", "alternative medicine": "125",
    "construction": "48", "civil engineering": "51", "architecture & planning": "50",
    "real estate": "44", "commercial real estate": "128",
    "retail": "27", "apparel & fashion": "19", "luxury goods & jewelry": "143",
    "consumer goods": "25", "food & beverages": "23", "wine and spirits": "142",
    "restaurants": "32", "hospitality": "31", "leisure, travel & tourism": "30",
    "automotive": "53", "aviation & aerospace": "52",
    "logistics and supply chain": "116", "transportation/trucking/railroad": "92",
    "manufacturing": "55", "machinery": "54", "industrial automation": "135",
    "chemicals": "62", "oil & energy": "59", "renewables & environment": "86",
    "utilities": "59", "mining & metals": "56",
    "human resources": "137", "staffing and recruiting": "104",
    "professional training & coaching": "105", "management consulting": "11",
    "law practice": "10", "legal services": "9",
    "non-profit organization management": "100", "nonprofit": "100",
    "government administration": "75", "defense & space": "1",
    "agriculture": "63", "farming": "65",
    "events services": "110", "sports": "33", "fitness": "124", "wellness and fitness": "124",
}


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def resolve_geo(name: str) -> Optional[str]:
    """Return a LinkedIn geo ID for a place name, or None if unknown."""
    if not name:
        return None
    return LINKEDIN_GEO_IDS.get(_norm(name))


def resolve_industry(name: str) -> Optional[str]:
    """Return a LinkedIn industry ID for an industry name, or None if unknown."""
    if not name:
        return None
    return LINKEDIN_INDUSTRY_IDS.get(_norm(name))


_PLANNER_SYSTEM = (
    "You are a query planner for a B2B data lookup pipeline. The downstream API "
    "(LinkedIn-style company/people search) matches its `keyword` field against "
    "company names and taglines — NOT semantic descriptions — so a long natural "
    "language sentence as a keyword returns 0 results.\n\n"
    "Your job: extract structured search parameters from the user's query.\n\n"
    "Return STRICT JSON with exactly these keys (use null for unknowns):\n"
    "  search_keyword: short token string likely to appear IN COMPANY NAMES or "
    "taglines (e.g. for 'Video Game Producers in California working with Unity', "
    "use 'game studio' or 'video games', NOT the full sentence). 1-4 words.\n"
    "  location_name: a place name (country, US state, or major metro) if mentioned, else null\n"
    "  industry_name: the most specific LinkedIn industry category if implied "
    "(e.g. 'Computer Games', 'Software Development', 'Marketing and Advertising'), else null\n"
    "  technology: a specific tech/tool keyword if mentioned (e.g. 'Unity', 'React'), else null\n"
    "  employee_min: integer lower bound on company size if mentioned, else null\n"
    "  employee_max: integer upper bound on company size if mentioned, else null\n\n"
    "Examples:\n"
    "Q: 'Video Game Producers in California working with Unity'\n"
    "A: {\"search_keyword\":\"game studio\",\"location_name\":\"California\","
    "\"industry_name\":\"Computer Games\",\"technology\":\"Unity\","
    "\"employee_min\":null,\"employee_max\":null}\n\n"
    "Q: 'SaaS companies in Austin with fewer than 200 employees'\n"
    "A: {\"search_keyword\":\"saas\",\"location_name\":\"Austin\","
    "\"industry_name\":\"Computer Software\",\"technology\":null,"
    "\"employee_min\":null,\"employee_max\":200}\n\n"
    "Q: 'dev agencies'\n"
    "A: {\"search_keyword\":\"development agency\",\"location_name\":null,"
    "\"industry_name\":\"Information Technology and Services\",\"technology\":null,"
    "\"employee_min\":null,\"employee_max\":null}"
)


async def plan_query(
    query: str,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    provider: Optional[str] = None,
) -> dict[str, Any]:
    """Decompose `query` into provider-native parameters.

    Returns dict with keys: search_keyword, location_name, industry_name,
    technology, employee_min, employee_max, location_geo_id, industry_id,
    raw_query. On any failure, returns a degenerate plan that just echoes the
    raw query as the keyword (preserving prior behavior — never worse).
    """
    fallback: dict[str, Any] = {
        "search_keyword": query, "location_name": None, "industry_name": None,
        "technology": None, "employee_min": None, "employee_max": None,
        "location_geo_id": None, "industry_id": None, "raw_query": query,
    }
    if not (query or "").strip() or not api_key:
        return fallback
    try:
        out = await chat_json(
            [{"role": "system", "content": _PLANNER_SYSTEM},
             {"role": "user", "content": query.strip()}],
            api_key=api_key, model=model, provider=provider,
        )
    except LLMError:
        return fallback
    if not isinstance(out, dict):
        return fallback

    plan: dict[str, Any] = dict(fallback)
    sk = (out.get("search_keyword") or "").strip()
    plan["search_keyword"] = sk if sk else query
    for k in ("location_name", "industry_name", "technology"):
        v = out.get(k)
        plan[k] = v.strip() if isinstance(v, str) and v.strip() else None
    for k in ("employee_min", "employee_max"):
        v = out.get(k)
        plan[k] = int(v) if isinstance(v, (int, float)) and v else None

    plan["location_geo_id"] = resolve_geo(plan["location_name"] or "")
    plan["industry_id"] = resolve_industry(plan["industry_name"] or "")

    # If we have a technology hint and the search_keyword doesn't include it,
    # append it — companies working with a specific tech often mention it in
    # their tagline (e.g. "Unity Studio", "React Shop").
    tech = plan["technology"]
    if tech and tech.lower() not in plan["search_keyword"].lower():
        plan["search_keyword"] = f"{plan['search_keyword']} {tech}".strip()

    return plan
