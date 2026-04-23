"""Header → source mapping. Used by router to pick which Netrows endpoints to call."""

# Each header (normalized lowercase) maps to ordered list of source IDs.
HEADER_TO_SOURCES: dict[str, list[str]] = {
    "company name": ["linkedin_companies", "yc_search", "google_search"],
    "company": ["linkedin_companies", "yc_search", "google_search"],
    # YC-specific signals — strong hint to use YC as primary producer
    "batch": ["yc_search"],
    "yc batch": ["yc_search"],
    "yc": ["yc_search"],
    "ycombinator": ["yc_search"],
    "y combinator": ["yc_search"],
    "founder": ["yc_search"],
    "founders": ["yc_search"],
    "founder name": ["yc_search"],
    "one liner": ["yc_search"],
    "tagline": ["yc_search", "linkedin_companies"],
    "year founded": ["yc_search", "crunchbase_company"],
    "founded": ["yc_search", "linkedin_companies", "crunchbase_company"],
    "is hiring": ["yc_search"],
    "hiring": ["yc_search"],
    "top company": ["yc_search"],
    "yc url": ["yc_search"],
    "ycombinator url": ["yc_search"],
    "twitter": ["yc_search"],
    "twitter url": ["yc_search"],
    "name": ["linkedin_people"],
    "contact name": ["linkedin_people"],
    "title": ["linkedin_people"],
    "email": ["email_finder"],
    "phone": ["google_maps_place", "google_maps"],
    "phone number": ["google_maps_place", "google_maps"],
    "hours": ["google_maps_place"],
    "opening hours": ["google_maps_place"],
    "linkedin url": ["linkedin_people", "linkedin_companies"],
    "linkedin": ["linkedin_people", "linkedin_companies"],
    # GitHub fields trigger enrichment (handled separately, not as a primary producer)
    "github url": ["github_org", "github_user"],
    "github": ["github_org", "github_user"],
    "website": ["linkedin_companies", "google_search", "crunchbase_company"],
    "domain": ["linkedin_companies", "google_search"],
    "industry": ["linkedin_companies", "crunchbase_company"],
    "categories": ["crunchbase_company"],
    "technologies": ["crunchbase_company"],
    "tech": ["crunchbase_company"],
    "location": ["google_maps", "linkedin_people", "linkedin_companies"],
    "address": ["google_maps"],
    "hq": ["linkedin_companies", "crunchbase_company"],
    "employee count": ["linkedin_companies", "crunchbase_company"],
    "size": ["linkedin_companies"],
    "team size": ["linkedin_companies"],
    "funding": ["crunchbase_company"],
    "funding total": ["crunchbase_company"],
    "last funding": ["crunchbase_company"],
    "investors": ["crunchbase_company"],
    "num investors": ["crunchbase_company"],
    "stage": ["yc_search", "crunchbase_company"],
    "tags": ["yc_search"],
    "regions": ["yc_search"],
    "revenue": ["crunchbase_company"],
    "monthly visits": ["crunchbase_company"],
    "heat score": ["crunchbase_company"],
    "operating status": ["crunchbase_company"],
    "ipo status": ["crunchbase_company"],
    "rank": ["crunchbase_company"],
    "acquisitions": ["crunchbase_company"],
    "exits": ["crunchbase_person", "crunchbase_company"],
    "investments": ["crunchbase_person"],
    "portfolio": ["crunchbase_person"],
    "is investor": ["crunchbase_person"],
    "board roles": ["crunchbase_person"],
    "rating": ["google_maps"],
    "review count": ["google_maps"],
    "reviews": ["google_maps_reviews", "google_maps"],
    "latitude": ["google_maps"],
    "longitude": ["google_maps"],
    "feature id": ["google_maps"],
    "bio": ["github_user", "linkedin_people"],
    "tech stack": ["github_org", "github_user"],
    "languages": ["github_user", "github_repo"],
    "repos": ["github_user"],
    "public repos": ["github_user", "github_org"],
    "stars": ["github_user_repos", "github_repo"],
    "followers": ["github_user", "github_org"],
    "open roles": ["linkedin_jobs", "indeed_jobs"],
    "role": ["linkedin_jobs", "indeed_jobs"],
    "job openings": ["linkedin_jobs", "indeed_jobs"],
    "job title": ["linkedin_jobs", "linkedin_people"],
    "description": ["google_search", "linkedin_companies"],
    "indeed url": ["indeed_jobs"],
    "job key": ["indeed_jobs"],
    "snippet": ["indeed_jobs"],
    "city": ["indeed_jobs", "linkedin_people"],
    "state": ["indeed_jobs"],
    "is remote": ["indeed_jobs"],
    "salary": ["linkedin_jobs", "indeed_jobs"],
    "salary range": ["linkedin_jobs", "indeed_jobs"],
    "salary min": ["indeed_job_details"],
    "salary max": ["indeed_job_details"],
    "benefits": ["indeed_job_details"],
    "description text": ["indeed_job_details"],
    # Indeed company-level signals (enricher phase)
    "company rating": ["indeed_company"],
    "ceo approval": ["indeed_company"],
    "happiness score": ["indeed_company"],
    "rating breakdown": ["indeed_company"],
    "interview difficulty": ["indeed_company"],
    "interview duration": ["indeed_company"],
    "interview experience": ["indeed_company"],
    "popular job titles": ["indeed_company"],
    "pros": ["indeed_company_reviews"],
    "cons": ["indeed_company_reviews"],
    "review insights": ["indeed_company_reviews"],
    "salary satisfaction": ["indeed_company_salaries"],
    "total salary reports": ["indeed_company_salaries"],
    "compensation": ["linkedin_jobs"],
    "posted date": ["linkedin_jobs", "indeed_jobs"],
    "posted at": ["linkedin_jobs"],
    "workplace type": ["linkedin_jobs"],
    "remote": ["linkedin_jobs"],
    "experience level": ["linkedin_jobs"],
    "employment type": ["linkedin_jobs"],
    "applicants": ["linkedin_jobs"],
    "apply url": ["linkedin_jobs"],
    "job id": ["linkedin_jobs"],
}


# Source → primary "company entity" producer (yields company-like rows)
COMPANY_PRODUCERS = {
    "linkedin_companies",
    "yc_search",
    "google_search",
    "google_maps",
    "linkedin_jobs",
    "indeed_jobs",
}

# Sources that benefit from a deep per-record enrichment pass
YC_ENRICHERS = {"yc_company"}
CRUNCHBASE_ENRICHERS = {"crunchbase_company", "crunchbase_person"}

# Source → primary "person entity" producer
PERSON_PRODUCERS = {
    "linkedin_people",
}

# GitHub sources are enrichers — they take a username/org we already have,
# so the orchestrator handles them in a separate phase, not via _call_endpoint.
GITHUB_ENRICHERS = {"github_user", "github_user_repos", "github_org", "github_repo"}


def normalize_header(h: str) -> str:
    return (h or "").strip().lower()


def sources_for_headers(headers: list[str]) -> list[str]:
    """Return deduped ordered list of source IDs needed for a header set."""
    out: list[str] = []
    for h in headers:
        for s in HEADER_TO_SOURCES.get(normalize_header(h), []):
            if s not in out:
                out.append(s)
    return out


def needs_email(headers: list[str]) -> bool:
    return any(normalize_header(h) == "email" for h in headers)
