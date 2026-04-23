"""Netrows API client. Wraps the 9 Tier-1 endpoints we use.

Netrows base URL is configurable via NETROWS_BASE_URL env var. Default attempts
the public API host. All endpoint paths follow Netrows' /v1/<platform>/<resource>
convention; if any path 404s in production, override via NETROWS_PATH_<KEY> env vars.
"""
from __future__ import annotations
import os
import asyncio
import httpx
from typing import Any, Optional

NETROWS_BASE = os.environ.get("NETROWS_BASE_URL", "https://api.netrows.com")

# Default endpoint paths. Override at runtime via env if Netrows changes them.
DEFAULT_PATHS = {
    "linkedin_people": "/v1/people/search",
    "linkedin_people_profile": "/v1/people/profile",
    "linkedin_people_profile_by_url": "/v1/people/profile-by-url",
    "linkedin_companies": "/v1/companies/search",
    "linkedin_company_details": "/v1/companies/details",
    "linkedin_company_by_domain": "/v1/companies/by-domain",
    "linkedin_jobs": "/v1/jobs/search",
    "linkedin_job_details": "/v1/jobs/details",
    "linkedin_job_hiring_team": "/v1/jobs/hiring-team",
    # GitHub uses single-record lookups (no search). These are enrichers, not producers.
    "github_user": "/v1/github/user",
    "github_user_repos": "/v1/github/user-repos",
    "github_org": "/v1/github/org",
    "github_repo": "/v1/github/repo",
    # Email finder family (4 endpoints, each 5 credits)
    "email_finder_by_name": "/v1/email-finder/by-name",
    "email_finder_by_domain": "/v1/email-finder/by-domain",
    "email_finder_decision_maker": "/v1/email-finder/decision-maker",
    "email_finder_by_linkedin": "/v1/email-finder/by-linkedin",
    "google_search": "/v1/google/search",
    # Google Maps lives under /v1/google-maps/* (hyphen, not slash). Three endpoints.
    "google_maps": "/v1/google-maps/search",
    "google_maps_place": "/v1/google-maps/place",
    "google_maps_reviews": "/v1/google-maps/reviews",
    # Indeed family — search + details + company profile + reviews + salaries
    "indeed_jobs": "/v1/indeed/job-search",
    "indeed_job_details": "/v1/indeed/job-details",
    "indeed_company": "/v1/indeed/company",
    "indeed_company_reviews": "/v1/indeed/company-reviews",
    "indeed_company_salaries": "/v1/indeed/company-salaries",
    "indeed_salary_detail": "/v1/indeed/salary-detail",
    # YC (Y Combinator) — search + per-company deep details (founders, jobs, socials)
    "yc_search": "/v1/ycombinator/search",
    "yc_company": "/v1/ycombinator/company",
    # Crunchbase BETA — both endpoints take a `permalink`. Enrichers, not searchers.
    "crunchbase_company": "/v1/crunchbase/company",
    "crunchbase_person": "/v1/crunchbase/person",
}


def _path(key: str) -> str:
    return os.environ.get(f"NETROWS_PATH_{key.upper()}", DEFAULT_PATHS[key])


# LinkedIn company size buckets. The companies/search endpoint filters by these
# letter codes, not by numeric employee counts.
_LINKEDIN_SIZE_BUCKETS = [
    ("A", 1, 10), ("B", 11, 50), ("C", 51, 200), ("D", 201, 500),
    ("E", 501, 1000), ("F", 1001, 5000), ("G", 5001, 10000),
    ("H", 10001, 10_000_000),
]


def _employee_bounds_to_size_codes(emp_min: Optional[int], emp_max: Optional[int]) -> list[str]:
    """Map a numeric employee range to overlapping LinkedIn size-code buckets.
    Returns [] when both bounds are None so the filter is omitted entirely."""
    if emp_min is None and emp_max is None:
        return []
    lo = emp_min if emp_min is not None else 0
    hi = emp_max if emp_max is not None else 10_000_000
    return [code for (code, b_lo, b_hi) in _LINKEDIN_SIZE_BUCKETS if b_lo <= hi and b_hi >= lo]


class NetrowsError(Exception):
    pass


class NetrowsClient:
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None, timeout: float = 30.0):
        self.api_key = api_key or os.environ.get("NETROWS_API_KEY", "")
        self.base_url = (base_url or NETROWS_BASE).rstrip("/")
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "X-API-Key": self.api_key,
                "Accept": "application/json",
                "User-Agent": "Genshi/0.1",
            },
        )
        return self

    async def __aexit__(self, *a):
        if self._client:
            await self._client.aclose()

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        assert self._client is not None
        try:
            r = await self._client.get(path, params={k: v for k, v in params.items() if v is not None})
        except httpx.HTTPError as e:
            raise NetrowsError(f"Netrows network error on {path}: {e}") from e
        if r.status_code == 401:
            raise NetrowsError("Invalid Netrows API key")
        if r.status_code == 402:
            raise NetrowsError("Netrows: out of credits")
        ctype = (r.headers.get("content-type") or "").lower()
        is_json = "json" in ctype
        if r.status_code == 404:
            # Netrows returns 404 + JSON {code:"NOT_FOUND", message:"..."} when an endpoint
            # exists but has no results for the query. Treat that as empty, not as a missing path.
            if is_json:
                try:
                    body = r.json()
                    if isinstance(body, dict) and body.get("code") == "NOT_FOUND":
                        return {"data": [], "_empty": True, "_message": body.get("message", "")}
                except Exception:
                    pass
            raise NetrowsError(f"Netrows endpoint not found: {path}")
        if r.status_code >= 400:
            raise NetrowsError(f"Netrows {r.status_code}: {r.text[:200]}")
        if not is_json:
            raise NetrowsError(f"Netrows non-JSON response: {r.text[:200]}")
        try:
            return r.json()
        except Exception:
            raise NetrowsError(f"Netrows non-JSON response: {r.text[:200]}")

    # ---- Endpoints ----
    async def linkedin_people(self, query: str, location: str = "", limit: int = 15,
                              geo_id: str = "", industry_id: str = "", company: str = ""):
        # /v1/people/search supports: firstName, lastName, keywords, geo, keywordTitle,
        # schoolId, keywordSchool, company, start. People search has NO industry filter
        # (industry_id is accepted for API symmetry but ignored — kept here so callers
        # can pass the same plan dict to every endpoint).
        params: dict[str, Any] = {"keywords": query, "keywordTitle": query, "start": 0}
        if geo_id and str(geo_id).isdigit():
            params["geo"] = str(geo_id)
        elif location and location.isdigit():
            params["geo"] = location
        if company:
            params["company"] = company
        return await self._get(_path("linkedin_people"), params)

    async def linkedin_people_profile(self, username: str):
        # /v1/people/profile?username=satyanadella → deep profile w/ position[], educations[], skills[], geo
        return await self._get(_path("linkedin_people_profile"), {"username": username})

    async def linkedin_people_profile_by_url(self, url: str):
        return await self._get(_path("linkedin_people_profile_by_url"), {"url": url})

    async def linkedin_companies(self, query: str, location: str = "", limit: int = 15,
                                 geo_id: str = "", industry_id: str = "",
                                 employee_min: Optional[int] = None,
                                 employee_max: Optional[int] = None):
        # /v1/companies/search — Netrows-documented filter names:
        #   keyword, page, locations (csv geo IDs), industries (csv industry IDs),
        #   companySizes (csv letter codes A-H), hasJobs (bool)
        # Employee count is filtered via LinkedIn's letter-coded size buckets, not a
        # numeric range — we map our employee_min/max bounds to the overlapping codes.
        params: dict[str, Any] = {"keyword": query, "page": 1}
        if geo_id and str(geo_id).isdigit():
            params["locations"] = str(geo_id)
        elif location and location.isdigit():
            params["locations"] = location
        if industry_id and str(industry_id).isdigit():
            params["industries"] = str(industry_id)
        sizes = _employee_bounds_to_size_codes(employee_min, employee_max)
        if sizes:
            params["companySizes"] = ",".join(sizes)
        return await self._get(_path("linkedin_companies"), params)

    async def linkedin_company_details(self, username: str):
        # /v1/companies/details?username=microsoft → returns flat company object (no `data` wrapper).
        return await self._get(_path("linkedin_company_details"), {"username": username})

    async def linkedin_company_by_domain(self, domain: str):
        # /v1/companies/by-domain?domain=microsoft.com → returns {success, domain, company:{…}}.
        return await self._get(_path("linkedin_company_by_domain"), {"domain": domain})

    async def github_user(self, username: str):
        # /v1/github/user?username=torvalds → flat profile (no wrapper)
        return await self._get(_path("github_user"), {"username": username})

    async def github_user_repos(self, username: str, sort: str = "stars", type_: str = "all", page: int = 1):
        return await self._get(_path("github_user_repos"), {"username": username, "sort": sort, "type": type_, "page": page})

    async def github_org(self, org: str):
        # /v1/github/org?org=vercel → flat org profile
        return await self._get(_path("github_org"), {"org": org})

    async def github_repo(self, owner: str, repo: str):
        return await self._get(_path("github_repo"), {"owner": owner, "repo": repo})

    # ---- Email finder ---------------------------------------------------
    async def email_by_name(self, full_name: str = "", first_name: str = "", last_name: str = "",
                            domain: str = "", company_name: str = ""):
        params: dict = {}
        if full_name: params["full_name"] = full_name
        if first_name: params["first_name"] = first_name
        if last_name: params["last_name"] = last_name
        if domain: params["domain"] = domain
        if company_name: params["company_name"] = company_name
        return await self._get(_path("email_finder_by_name"), params)

    async def email_by_domain(self, domain: str = "", company_name: str = ""):
        params: dict = {}
        if domain: params["domain"] = domain
        if company_name: params["company_name"] = company_name
        return await self._get(_path("email_finder_by_domain"), params)

    async def email_decision_maker(self, category: str, domain: str = "", company_name: str = ""):
        params: dict = {"category": category}
        if domain: params["domain"] = domain
        if company_name: params["company_name"] = company_name
        return await self._get(_path("email_finder_decision_maker"), params)

    async def email_by_linkedin(self, linkedin_url: str):
        return await self._get(_path("email_finder_by_linkedin"), {"linkedin_url": linkedin_url})

    async def google_search(self, query: str, region: str = "", limit: int = 15):
        # /v1/google/search → {success, results:[{url, title, description}]}
        # Only `query` is required; optional `region` is a 2-letter country code.
        params = {"query": query}
        if region: params["region"] = region
        return await self._get(_path("google_search"), params)

    async def google_maps(self, query: str, location: str = "", limit: int = 15,
                          gl: str = "", hl: str = ""):
        # /v1/google-maps/search → {query, results:[{name, feature_id, rating, review_count,
        #   categories, address, website, latitude, longitude, image}], total_results}
        # gl = country code (e.g. 'us','jp','de'), hl = UI language (e.g. 'en','ja')
        params = {"query": query}
        if location: params["location"] = location
        if gl: params["gl"] = gl
        if hl: params["hl"] = hl
        return await self._get(_path("google_maps"), params)

    async def google_maps_place(self, query: str, gl: str = "", hl: str = ""):
        # /v1/google-maps/place → single best-match place with phone, hours, description
        params = {"query": query}
        if gl: params["gl"] = gl
        if hl: params["hl"] = hl
        return await self._get(_path("google_maps_place"), params)

    async def google_maps_reviews(self, query: str = "", feature_id: str = ""):
        # /v1/google-maps/reviews → {feature_id, overall_rating, total_reviews,
        #   rating_distribution, reviews:[…]}
        params = {}
        if query: params["query"] = query
        if feature_id: params["feature_id"] = feature_id
        return await self._get(_path("google_maps_reviews"), params)

    async def linkedin_jobs(self, query: str, location: str = "", limit: int = 15,
                            geo_id: str = "", industry_id: str = "", **filters):
        # /v1/jobs/search — `keywords` is required. Optional: locationId, datePosted,
        # experienceLevel, jobType, onsiteRemote, salary, start, sort, industryIds, etc.
        params: dict[str, Any] = {"keywords": query, "start": 0, "sort": "mostRelevant"}
        if geo_id and str(geo_id).isdigit():
            params["locationId"] = str(geo_id)
        elif location and location.isdigit():
            params["locationId"] = location
        if industry_id and str(industry_id).isdigit():
            params["industryIds"] = str(industry_id)
        for k, v in filters.items():
            if v not in (None, ""):
                params[k] = v
        return await self._get(_path("linkedin_jobs"), params)

    async def linkedin_job_details(self, job_id: str):
        return await self._get(_path("linkedin_job_details"), {"id": job_id})

    async def linkedin_job_hiring_team(self, job_id: str = "", url: str = ""):
        params: dict[str, Any] = {}
        if job_id: params["id"] = job_id
        if url: params["url"] = url
        return await self._get(_path("linkedin_job_hiring_team"), params)

    async def indeed_jobs(self, query: str, location: str = "", page: int = 1,
                          job_type: str = "", remote: bool | None = None, limit: int = 15):
        # /v1/indeed/job-search — `query` required, optional location/page/job_type/remote
        # job_type ∈ {fulltime, parttime, contract, internship, temporary}
        params = {"query": query, "page": page}
        if location: params["location"] = location
        if job_type: params["job_type"] = job_type
        if remote is not None: params["remote"] = str(remote).lower()
        return await self._get(_path("indeed_jobs"), params)

    async def indeed_job_details(self, job_key: str):
        return await self._get(_path("indeed_job_details"), {"job_key": job_key})

    async def indeed_company(self, slug: str):
        # /v1/indeed/company — slug e.g. 'Google', 'Amazon', 'Ramp'
        return await self._get(_path("indeed_company"), {"slug": slug})

    async def indeed_company_reviews(self, slug: str, page: int = 1, sort: str = "helpfulness"):
        return await self._get(_path("indeed_company_reviews"), {"slug": slug, "page": page, "sort": sort})

    async def indeed_company_salaries(self, slug: str):
        return await self._get(_path("indeed_company_salaries"), {"slug": slug})

    async def indeed_salary_detail(self, slug: str, job_title: str):
        return await self._get(_path("indeed_salary_detail"), {"slug": slug, "job_title": job_title})

    # ---- Y Combinator ---------------------------------------------------
    async def yc_search(self, query: str = "", batch: str = "", industry: str = "",
                        status: str = "", region: str = "", tag: str = "",
                        is_hiring: bool | None = None, top_companies: bool | None = None,
                        page: int = 1, per_page: int = 20):
        params = {"query": query, "page": page, "per_page": per_page}
        if batch: params["batch"] = batch
        if industry: params["industry"] = industry
        if status: params["status"] = status
        if region: params["region"] = region
        if tag: params["tag"] = tag
        if is_hiring is not None: params["is_hiring"] = str(is_hiring).lower()
        if top_companies is not None: params["top_companies"] = str(top_companies).lower()
        return await self._get(_path("yc_search"), params)

    async def yc_company(self, slug: str):
        return await self._get(_path("yc_company"), {"slug": slug})

    async def crunchbase_company(self, permalink: str):
        # /v1/crunchbase/company?permalink=stripe → flat enriched org
        return await self._get(_path("crunchbase_company"), {"permalink": permalink})

    async def crunchbase_person(self, permalink: str):
        # /v1/crunchbase/person?permalink=patrick-collison → flat enriched person
        return await self._get(_path("crunchbase_person"), {"permalink": permalink})


# ---- Result normalization ----
# Netrows wraps results inconsistently across endpoints. Normalize to a flat list of dicts.
def _flatten_job(it: dict) -> dict:
    """Jobs have nested company:{name,companyId,logo} and salary:{min,max,currency}.
    Promote useful nested fields to top level for the row mapper."""
    if isinstance(it, dict):
        co = it.get("company")
        if isinstance(co, dict):
            it.setdefault("companyName", co.get("name"))
            it.setdefault("companyId", co.get("companyId") or co.get("id"))
            it.setdefault("companyUsername", co.get("username"))
            it.setdefault("companyLogo", co.get("logo"))
        sal = it.get("salary")
        if isinstance(sal, dict):
            mn, mx, cur = sal.get("min"), sal.get("max"), sal.get("currency") or ""
            if mn and mx:
                it.setdefault("salaryRange", f"{cur} {mn:,}–{mx:,}".strip())
            elif mn:
                it.setdefault("salaryRange", f"{cur} {mn:,}+".strip())
    return it


def extract_items(payload: Any) -> list[dict]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        # Single-object wrappers (companies/by-domain → {company:{...}}, similar shapes)
        for solo in ("company", "user", "organization", "profile"):
            v = payload.get(solo)
            if isinstance(v, dict):
                return [v]
        for key in ("data", "results", "items", "people", "companies", "users", "organizations", "emails", "jobs", "places"):
            v = payload.get(key)
            if isinstance(v, list):
                items = [x for x in v if isinstance(x, dict)]
                # Auto-flatten job-shaped records
                if items and any(("jobId" in x or ("company" in x and isinstance(x.get("company"), dict))) for x in items[:3]):
                    items = [_flatten_job(x) for x in items]
                return items
            if isinstance(v, dict):
                for k2 in ("results", "items", "list", "data"):
                    if isinstance(v.get(k2), list):
                        return [x for x in v[k2] if isinstance(x, dict)]
                # `data` is a single-record dict (e.g. companies/details, companies/by-domain)
                if any(k in v for k in ("name", "id", "username", "fullName", "company", "headline", "domain")):
                    return [v]
        # single object
        return [payload] if any(k in payload for k in ("name", "username", "firstName", "company", "email", "url", "title", "headline")) else []
    return []
