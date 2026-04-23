"""The main pipeline: take headers + query, return enriched, verified rows."""
from __future__ import annotations
import asyncio
import json
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional

from ..sources.netrows import NetrowsClient, NetrowsError, extract_items
from ..sources.intelligence import scan_signals
from ..sources.header_map import normalize_header, needs_email, COMPANY_PRODUCERS
from .router import plan_sources
from .query_planner import plan_query
from .llm import chat_json, chat_text, LLMError
from ..verification.email_verify import verify_email


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cell(value: Any, source: str, confidence: str = "medium", **extra) -> dict:
    d = {"value": value, "source": source, "confidence": confidence, "fetched_at": _now()}
    d.update(extra)
    return d


def _domain_of(url: str) -> str:
    if not url:
        return ""
    s = url.strip().lower()
    if "://" in s:
        s = s.split("://", 1)[1]
    s = s.split("/", 1)[0]
    if s.startswith("www."):
        s = s[4:]
    return s


async def _safe_call(coro, label: str, events: list[dict], queue: Optional[asyncio.Queue] = None):
    """Run a Netrows coroutine, swallowing any error into a `source_error` event,
    and emit a `source_call` event on success so the SSE stream surfaces every
    upstream API hit (label + elapsed ms + rough item count). Without this, the
    deep-enrichment phases (which fan out 10-30 parallel calls) look completely
    silent to the frontend even though heavy work is in flight."""
    import time
    t0 = time.monotonic()
    try:
        result = await coro
    except Exception as e:
        ev = {"type": "source_error", "source": label, "error": str(e)[:200],
              "ms": int((time.monotonic() - t0) * 1000)}
        events.append(ev)
        if queue is not None:
            await queue.put(ev)
        return None
    # Best-effort: count items if the payload looks like a list/dict-with-items
    n: Optional[int] = None
    try:
        if isinstance(result, list):
            n = len(result)
        elif isinstance(result, dict):
            for k in ("items", "results", "data", "valid_emails", "emails"):
                v = result.get(k)
                if isinstance(v, list):
                    n = len(v); break
            if n is None and result.get("name"):
                n = 1  # single-record response (profile/company)
    except Exception:
        pass
    ev = {"type": "source_call", "source": label,
          "ms": int((time.monotonic() - t0) * 1000), "count": n}
    events.append(ev)
    if queue is not None:
        await queue.put(ev)
    return result


async def generate_rows(
    headers: list[str],
    query: str,
    row_limit: int = 15,
    sources_override: Optional[list[str]] = None,
    netrows_key: Optional[str] = None,
    aiassist_key: Optional[str] = None,
    aiassist_model: Optional[str] = None,
    aiassist_provider: Optional[str] = None,
    progress: Optional[asyncio.Queue] = None,
) -> list[dict]:
    """Run the full pipeline. Returns list of row dicts (each row = {header: cell_meta})."""
    events: list[dict] = []
    async def emit(ev: dict):
        events.append(ev)
        if progress is not None:
            await progress.put(ev)

    # 1. Plan sources
    if sources_override:
        sources = sources_override
    else:
        sources = await plan_sources(headers, query, api_key=aiassist_key, model=aiassist_model, provider=aiassist_provider)
    await emit({"type": "plan", "sources": sources})

    # 2. Pick a "primary" company/entity producer to seed rows
    primary = next((s for s in sources if s in COMPANY_PRODUCERS), sources[0] if sources else "google_search")
    await emit({"type": "stage", "stage": "fetching", "primary": primary})

    # 2a. Decompose the natural-language query into provider-native parameters
    # (geo IDs, industry IDs, employee bands). One LLM call, reused across all
    # producer fetches in this generation. Falls back to raw query on miss.
    plan = await plan_query(
        query, api_key=aiassist_key, model=aiassist_model, provider=aiassist_provider,
    )
    await emit({
        "type": "query_plan",
        "search_keyword": plan["search_keyword"],
        "location_name": plan["location_name"],
        "location_geo_id": plan["location_geo_id"],
        "industry_name": plan["industry_name"],
        "industry_id": plan["industry_id"],
        "technology": plan["technology"],
        "employee_min": plan["employee_min"],
        "employee_max": plan["employee_max"],
    })

    raw_items: list[dict] = []
    async with NetrowsClient(api_key=netrows_key) as nc:
        # Primary fetch — uses the structured plan, paginating up to 3 pages
        # so we have a real shot at filling row_limit. Each LinkedIn page is
        # ~10 results; without pagination, niche queries returned ~5 rows even
        # when matches existed on page 2.
        items: list[dict] = []
        max_pages = 3
        for pg in range(1, max_pages + 1):
            page_payload = await _safe_call(
                _call_endpoint(nc, primary, query, row_limit, plan, page=pg),
                f"{primary}:p{pg}" if pg > 1 else primary,
                events, progress,
            )
            page_items = extract_items(page_payload)
            if not page_items:
                break
            await emit({"type": "page_fetched", "source": primary, "page": pg, "count": len(page_items)})
            items.extend(page_items)
            if len(items) >= row_limit:
                break
        items = items[:row_limit]

        # Filter-too-narrow retry: if the structured filters returned 0 hits and
        # we actually applied any (geo or industry), retry once with just the
        # keyword. Protects against stale geo IDs, wrong industry mappings, or
        # over-restrictive combos.
        applied_filters = bool((plan.get("location_geo_id") or plan.get("industry_id"))
                               and primary in ("linkedin_companies", "linkedin_people", "linkedin_jobs"))
        if not items and applied_filters:
            await emit({"type": "primary_retry", "source": primary,
                        "reason": "structured filters returned 0; retrying without filters"})
            unfiltered_plan = {**plan, "location_geo_id": None, "industry_id": None,
                               "employee_min": None, "employee_max": None}
            for pg in range(1, max_pages + 1):
                pp = await _safe_call(
                    _call_endpoint(nc, primary, query, row_limit, unfiltered_plan, page=pg),
                    f"{primary}:retry:p{pg}", events, progress,
                )
                pi = extract_items(pp)
                if not pi:
                    break
                items.extend(pi)
                if len(items) >= row_limit:
                    break
            items = items[:row_limit]

        # Cross-source fallback: if LinkedIn produced nothing even unfiltered,
        # try google_search with the original NL query (semantic SERP). Surfaces
        # *something* useful instead of a blank sheet for queries LinkedIn can't
        # match (e.g. very niche descriptors).
        if not items and primary != "google_search":
            await emit({"type": "fallback", "from": primary, "to": "google_search"})
            gs_payload = await _safe_call(nc.google_search(query, limit=row_limit),
                                          "google_search:fallback", events, progress)
            items = extract_items(gs_payload)[:row_limit]
            if items:
                primary = "google_search"  # downstream enrichers key off `primary`

        raw_items.extend(items)
        await emit({"type": "primary_done", "source": primary, "count": len(items)})

        # Producer-empty short-circuit: if the primary returned nothing even
        # after the unfiltered retry, there are no anchor entities to enrich.
        # Skip the dependent person fetch so we don't burn more credits chasing
        # zero anchors. Surface the plan and a hint so the user can refine.
        if not items:
            await emit({
                "type": "producer_empty",
                "source": primary,
                "plan": plan,
                "hint": (
                    "Primary producer returned 0 results. The query may be too "
                    "specific for keyword-style company search. Try a broader "
                    "keyword (e.g. 'game studio' instead of 'video game producers')."
                ),
            })

        # Person enrichment if "person" headers present and person source not yet used
        # (only when we actually have anchors to enrich)
        person_sources = [s for s in sources if s == "linkedin_people" and s != primary] if items else []
        for ps in person_sources[:1]:
            extra = await _safe_call(_call_endpoint(nc, ps, query, row_limit, plan), ps, events, progress)
            extra_items = extract_items(extra)
            if extra_items:
                raw_items.extend(extra_items[:row_limit])
                await emit({"type": "secondary_done", "source": ps, "count": len(extra_items)})

        # Deep-profile enrichment for LinkedIn people: hydrate each shallow search hit
        # via /people/profile?username=… when the requested headers ask for fields the
        # search response doesn't carry (company, title, position, school, education, geo).
        deep_keys = {"company", "company name", "current company", "title", "position",
                     "job title", "role", "school", "education", "university",
                     "country", "city"}
        wants_deep = any(normalize_header(h) in deep_keys for h in headers)
        person_hits = [it for it in raw_items if isinstance(it, dict) and it.get("username") and (it.get("fullName") or it.get("firstName") or it.get("headline"))]
        if wants_deep and person_hits:
            await emit({"type": "stage", "stage": "deep_profiles", "count": len(person_hits)})
            async def hydrate(it):
                u = it.get("username")
                payload = await _safe_call(nc.linkedin_people_profile(u), f"people_profile:{u}", events, progress)
                deep = extract_items(payload)
                if deep:
                    # Merge deep fields into shallow record so downstream mapping sees them.
                    d0 = deep[0]
                    for k, v in d0.items():
                        if v not in (None, "", [], {}):
                            it.setdefault(k, v)
                    # Flatten the most useful nested bits onto the top level
                    pos = (d0.get("position") or [{}])[0] if isinstance(d0.get("position"), list) else {}
                    if pos:
                        it.setdefault("currentCompany", pos.get("companyName"))
                        it.setdefault("currentCompanyURL", pos.get("companyURL"))
                        it.setdefault("currentTitle", pos.get("title"))
                        it.setdefault("currentLocation", pos.get("location"))
                    edu = (d0.get("educations") or [{}])[0] if isinstance(d0.get("educations"), list) else {}
                    if edu:
                        it.setdefault("school", edu.get("schoolName"))
                    geo = d0.get("geo") if isinstance(d0.get("geo"), dict) else {}
                    if geo:
                        it.setdefault("country", geo.get("country"))
                        it.setdefault("city", geo.get("city"))
                return it
            await asyncio.gather(*[hydrate(it) for it in person_hits[:row_limit]])
            await emit({"type": "deep_profiles_done", "count": len(person_hits)})

        # YC deep enrichment: when YC was used (or YC-only fields like founders/jobs/socials
        # are requested), hydrate each row with /ycombinator/company?slug=… in parallel.
        yc_keys = {"founder", "founders", "founder name", "linkedin url", "twitter",
                   "twitter url", "github url", "ycombinator url", "yc url",
                   "long description", "open jobs", "facebook url", "crunchbase url"}
        wants_yc_deep = (primary == "yc_search") or any(normalize_header(h) in yc_keys for h in headers)
        yc_hits = [it for it in raw_items if isinstance(it, dict) and it.get("slug") and (it.get("batch") or it.get("one_liner") or it.get("logo_url"))]
        if wants_yc_deep and yc_hits:
            await emit({"type": "stage", "stage": "yc_deep", "count": len(yc_hits)})
            async def yc_hydrate(it):
                slug = it.get("slug")
                payload = await _safe_call(nc.yc_company(slug), f"yc_company:{slug}", events, progress)
                if not isinstance(payload, dict) or payload.get("error"):
                    return
                # Merge any non-empty top-level fields
                for k, v in payload.items():
                    if v not in (None, "", [], {}) and k not in ("founders", "open_jobs", "launches", "company_photos"):
                        it.setdefault(k, v)
                # Flatten founders → comma list of names + first founder details
                founders = payload.get("founders") or []
                if isinstance(founders, list) and founders:
                    names = [f.get("name") for f in founders if isinstance(f, dict) and f.get("name")]
                    if names:
                        it.setdefault("founderNames", ", ".join(names))
                    f0 = founders[0] if isinstance(founders[0], dict) else {}
                    it.setdefault("founderName", f0.get("name"))
                    it.setdefault("founderTitle", f0.get("title"))
                    it.setdefault("founderLinkedIn", f0.get("linkedin_url"))
                    it.setdefault("founderTwitter", f0.get("twitter_url"))
                    it.setdefault("founderAvatar", f0.get("avatar_url"))
                # Open jobs → count + first title
                jobs = payload.get("open_jobs") or []
                if isinstance(jobs, list):
                    it.setdefault("openJobsCount", len(jobs))
                    if jobs and isinstance(jobs[0], dict):
                        it.setdefault("openJobTitle", jobs[0].get("title"))
            await asyncio.gather(*[yc_hydrate(it) for it in yc_hits[:row_limit]])
            await emit({"type": "yc_deep_done", "count": len(yc_hits)})

        # Google Maps place enrichment: when phone/hours headers are present and the
        # primary was google_maps, hydrate each row via /google-maps/place (which
        # returns phone, hours, description — fields the search response lacks).
        place_keys = {"phone", "phone number", "hours", "opening hours", "description"}
        wants_place = primary == "google_maps" and any(normalize_header(h) in place_keys for h in headers)
        place_hits = [it for it in raw_items if isinstance(it, dict) and it.get("name") and it.get("address")]
        if wants_place and place_hits:
            await emit({"type": "stage", "stage": "maps_place_enrich", "count": len(place_hits)})
            async def place_hydrate(it):
                # Use "name + address" as the most specific query
                q = f"{it.get('name')} {it.get('address')}"
                payload = await _safe_call(nc.google_maps_place(q), f"maps_place:{(it.get('name') or '')[:25]}", events, progress)
                if not isinstance(payload, dict) or payload.get("error"): return
                for k in ("phone", "hours", "description"):
                    v = payload.get(k)
                    if v not in (None, "", [], {}): it.setdefault(k, v)
            await asyncio.gather(*[place_hydrate(it) for it in place_hits[:row_limit]])
            await emit({"type": "maps_place_done"})

        # Crunchbase deep enrichment (BETA): when crunchbase-only fields are present
        # (funding, investors, revenue, technologies, monthly_visits, …), derive a
        # permalink per row and call /v1/crunchbase/company in parallel. We also enrich
        # founder rows via /v1/crunchbase/person when the row exposes a person permalink.
        cb_company_keys = {"funding", "funding total", "last funding", "investors",
                           "num investors", "revenue", "monthly visits", "heat score",
                           "technologies", "tech", "categories", "operating status",
                           "ipo status", "rank", "acquisitions"}
        cb_person_keys = {"investments", "portfolio", "is investor", "exits",
                          "board roles", "num exits", "num portfolio organizations"}
        wants_cb_co = any(normalize_header(h) in cb_company_keys for h in headers)
        wants_cb_pp = any(normalize_header(h) in cb_person_keys for h in headers)

        def _cb_permalink_from_url(u: str) -> str:
            if not isinstance(u, str): return ""
            if "crunchbase.com/organization/" in u:
                return u.split("crunchbase.com/organization/")[-1].strip("/").split("/")[0]
            if "crunchbase.com/person/" in u:
                return u.split("crunchbase.com/person/")[-1].strip("/").split("/")[0]
            return ""

        def _cb_company_permalink(it: dict) -> str:
            # Prefer explicit crunchbase_url (YC sets this), then slug, then derived from name
            for k in ("crunchbase_url", "crunchbaseUrl"):
                p = _cb_permalink_from_url(it.get(k) or "")
                if p: return p
            for k in ("slug", "permalink", "universalName", "companyUsername"):
                v = it.get(k)
                if isinstance(v, str) and v: return v
            cn = it.get("companyName") or it.get("name") or it.get("currentCompany")
            if isinstance(cn, str) and cn:
                s = cn.strip().lower().replace(" ", "-")
                if s.replace("-", "").isalnum(): return s
            return ""

        if wants_cb_co and raw_items:
            await emit({"type": "stage", "stage": "crunchbase_company_enrich"})
            async def cb_co_hydrate(it):
                p = _cb_company_permalink(it)
                if not p: return
                payload = await _safe_call(nc.crunchbase_company(p), f"crunchbase_company:{p}", events, progress)
                if not isinstance(payload, dict) or payload.get("error") or not payload.get("name"):
                    return
                # Merge non-empty scalars
                for k, v in payload.items():
                    if v in (None, "", [], {}): continue
                    if k in ("founders", "employees", "investors", "acquisitions",
                             "similar_companies", "products", "technologies",
                             "funding_rounds_list", "locations", "headquarters",
                             "categories"):
                        continue
                    it.setdefault(f"cb_{k}", v)
                # Friendly fallbacks for common fields
                it.setdefault("fundingTotalUsd", payload.get("funding_total_usd"))
                it.setdefault("lastFundingType", payload.get("last_funding_type"))
                it.setdefault("lastFundingAt", payload.get("last_funding_at"))
                it.setdefault("numInvestors", payload.get("num_investors"))
                it.setdefault("operatingStatus", payload.get("operating_status"))
                it.setdefault("ipoStatus", payload.get("ipo_status"))
                it.setdefault("monthlyVisits", payload.get("monthly_visits"))
                it.setdefault("heatScore", payload.get("heat_score"))
                hq = payload.get("headquarters") or {}
                if isinstance(hq, dict):
                    loc = ", ".join(x for x in [hq.get("city"), hq.get("region"), hq.get("country")] if x)
                    if loc: it.setdefault("cbHeadquarters", loc)
                cats = payload.get("categories") or []
                if isinstance(cats, list) and cats:
                    it.setdefault("cbCategories", ", ".join(c for c in cats if isinstance(c, str)))
                techs = payload.get("technologies") or []
                if isinstance(techs, list) and techs:
                    names = [t.get("name") for t in techs if isinstance(t, dict) and t.get("name")]
                    if names: it.setdefault("cbTechnologies", ", ".join(names[:10]))
                invs = payload.get("investors") or []
                if isinstance(invs, list) and invs:
                    names = [i.get("name") for i in invs if isinstance(i, dict) and i.get("name")]
                    if names: it.setdefault("cbInvestors", ", ".join(names[:8]))
                acqs = payload.get("acquisitions") or []
                if isinstance(acqs, list):
                    it.setdefault("cbAcquisitionsCount", len(acqs))
                fnd = payload.get("founders") or []
                if isinstance(fnd, list) and fnd:
                    names = [f.get("name") for f in fnd if isinstance(f, dict) and f.get("name")]
                    if names: it.setdefault("cbFounders", ", ".join(names))
            await asyncio.gather(*[cb_co_hydrate(it) for it in raw_items[:row_limit]])
            await emit({"type": "crunchbase_company_done"})

        if wants_cb_pp and raw_items:
            await emit({"type": "stage", "stage": "crunchbase_person_enrich"})
            async def cb_pp_hydrate(it):
                # Person permalink: from crunchbase_url, then linkedin slug, then name slug
                p = _cb_permalink_from_url(it.get("crunchbase_url") or "")
                if not p:
                    name = (it.get("fullName") or it.get("name")
                            or " ".join(x for x in [it.get("firstName"), it.get("lastName")] if x).strip())
                    if name:
                        p = name.strip().lower().replace(" ", "-")
                if not p: return
                payload = await _safe_call(nc.crunchbase_person(p), f"crunchbase_person:{p}", events, progress)
                if not isinstance(payload, dict) or payload.get("error") or not payload.get("name"):
                    return
                it.setdefault("isInvestor", payload.get("is_investor"))
                it.setdefault("numPortfolio", payload.get("num_portfolio_organizations"))
                it.setdefault("numExits", payload.get("num_exits"))
                invs = payload.get("investments") or []
                if isinstance(invs, list) and invs:
                    names = [i.get("company") for i in invs if isinstance(i, dict) and i.get("company")]
                    if names: it.setdefault("portfolio", ", ".join(names[:10]))
                exits = payload.get("exits") or []
                if isinstance(exits, list) and exits:
                    names = [e.get("company") for e in exits if isinstance(e, dict) and e.get("company")]
                    if names: it.setdefault("exitCompanies", ", ".join(names[:6]))
                board = payload.get("board_and_advisory_roles") or []
                if isinstance(board, list) and board:
                    names = [b.get("company") for b in board if isinstance(b, dict) and b.get("company")]
                    if names: it.setdefault("boardRoles", ", ".join(names[:6]))
            await asyncio.gather(*[cb_pp_hydrate(it) for it in raw_items[:row_limit]])
            await emit({"type": "crunchbase_person_done"})

        # Indeed company-deep enrichment: when company-level Indeed signals are
        # requested (rating, ceo_approval, interview difficulty, pros/cons,
        # salary satisfaction…), derive a slug per company row and call
        # /v1/indeed/company (+ /company-reviews, /company-salaries) in parallel.
        idd_co_keys = {"company rating", "ceo approval", "happiness score",
                       "rating breakdown", "interview difficulty",
                       "interview duration", "interview experience",
                       "popular job titles"}
        idd_rev_keys = {"pros", "cons", "review insights"}
        idd_sal_keys = {"salary satisfaction", "total salary reports"}
        wants_idd_co = any(normalize_header(h) in idd_co_keys for h in headers)
        wants_idd_rev = any(normalize_header(h) in idd_rev_keys for h in headers)
        wants_idd_sal = any(normalize_header(h) in idd_sal_keys for h in headers)

        def _indeed_slug(it: dict) -> str:
            # Indeed slugs are typically the company name with hyphens (e.g. 'Google',
            # 'Amazon', 'Y-Combinator'). Empirically slug = name as-typed works for
            # most well-known companies. Fall back to title-case hyphenated name.
            for k in ("companyName", "name", "currentCompany", "company"):
                v = it.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip().replace(" ", "-")
            return ""

        if (wants_idd_co or wants_idd_rev or wants_idd_sal) and raw_items:
            await emit({"type": "stage", "stage": "indeed_company_enrich"})
            async def idd_hydrate(it):
                slug = _indeed_slug(it)
                if not slug: return
                tasks = []
                if wants_idd_co:  tasks.append(("co",  nc.indeed_company(slug)))
                if wants_idd_rev: tasks.append(("rev", nc.indeed_company_reviews(slug)))
                if wants_idd_sal: tasks.append(("sal", nc.indeed_company_salaries(slug)))
                payloads = await asyncio.gather(
                    *[_safe_call(t[1], f"indeed_{t[0]}:{slug[:25]}", events, progress) for t in tasks],
                    return_exceptions=True,
                )
                for (kind, _), p in zip(tasks, payloads):
                    if not isinstance(p, dict) or p.get("error"): continue
                    if kind == "co":
                        for k in ("rating", "review_count", "ceo_approval", "industry"):
                            v = p.get(k)
                            if v not in (None, "", [], {}): it.setdefault(f"indeed_{k}", v)
                        ie = p.get("interview_experience") or {}
                        if isinstance(ie, dict):
                            it.setdefault("interviewDifficulty", ie.get("difficulty"))
                            it.setdefault("interviewDuration", ie.get("duration"))
                            it.setdefault("interviewExperience", ie.get("experience"))
                        rb = p.get("rating_breakdown") or {}
                        if isinstance(rb, dict) and rb:
                            it.setdefault("ratingBreakdown",
                                "; ".join(f"{k.replace('_',' ')}: {v}" for k, v in rb.items()))
                        pjt = p.get("popular_job_titles") or []
                        if isinstance(pjt, list) and pjt:
                            it.setdefault("popularJobTitles",
                                ", ".join(j.get("title", "") for j in pjt[:5] if isinstance(j, dict)))
                    elif kind == "rev":
                        ins = p.get("insights") or {}
                        pros = ins.get("pros") or []
                        cons = ins.get("cons") or []
                        if pros: it.setdefault("indeedPros", "; ".join(str(x) for x in pros[:5]))
                        if cons: it.setdefault("indeedCons", "; ".join(str(x) for x in cons[:5]))
                        it.setdefault("indeedTotalReviews", p.get("total_reviews"))
                    elif kind == "sal":
                        it.setdefault("salarySatisfaction", p.get("salary_satisfaction_ratio"))
                        it.setdefault("totalSalaryReports", p.get("total_salary_reports"))
            await asyncio.gather(*[idd_hydrate(it) for it in raw_items[:row_limit]])
            await emit({"type": "indeed_company_done"})

        # Indeed job-details deep enrichment: when salary_min/salary_max/benefits/
        # description text are requested and primary was indeed_jobs, fan out
        # /v1/indeed/job-details per job_key in parallel — search response lacks
        # those fields. Same fault-tolerant pattern as place-deep.
        jd_keys = {"salary min", "salary max", "salary range", "benefits", "description text"}
        wants_jd = primary == "indeed_jobs" and any(normalize_header(h) in jd_keys for h in headers)
        jd_hits = [it for it in raw_items if isinstance(it, dict) and it.get("job_key")]
        if wants_jd and jd_hits:
            await emit({"type": "stage", "stage": "indeed_job_details_enrich", "count": len(jd_hits)})
            async def jd_hydrate(it):
                jk = it.get("job_key")
                payload = await _safe_call(nc.indeed_job_details(jk),
                                           f"indeed_job_details:{jk}", events, progress)
                if not isinstance(payload, dict) or payload.get("error"): return
                for k in ("salary_min", "salary_max", "salary_currency", "salary_period",
                         "benefits", "description_text", "date_posted"):
                    v = payload.get(k)
                    if v not in (None, "", [], {}): it.setdefault(k, v)
            await asyncio.gather(*[jd_hydrate(it) for it in jd_hits[:row_limit]])
            await emit({"type": "indeed_job_details_done"})

        # GitHub enrichment: when github-related headers are present, enrich rows that
        # have a derivable github login. We try `github` URL fields, `companyUsername`
        # (LinkedIn often matches), or company name slug as a fallback.
        gh_keys = {"github", "github url", "tech stack", "languages", "repos",
                   "public repos", "stars", "followers", "bio"}
        wants_gh = any(normalize_header(h) in gh_keys for h in headers)
        if wants_gh and raw_items:
            await emit({"type": "stage", "stage": "github_enrich"})
            def _gh_login(it: dict) -> tuple[str, str] | None:
                # Returns (kind, login) where kind ∈ {"user","org"}.
                gh = it.get("github") or it.get("github_url") or it.get("githubUrl")
                if isinstance(gh, str) and "github.com/" in gh:
                    slug = gh.split("github.com/")[-1].strip("/").split("/")[0].split("?")[0]
                    if slug: return ("org" if it.get("companyName") or it.get("name") else "user", slug)
                # LinkedIn companies sometimes share their slug with github org
                slug = it.get("companyUsername") or it.get("universalName") or it.get("username")
                if slug and (it.get("companyName") or it.get("name") or it.get("companyId")):
                    return ("org", slug)
                # As a last resort, try the company-name slug
                cn = it.get("companyName") or it.get("name") or it.get("currentCompany")
                if isinstance(cn, str) and cn:
                    s = cn.strip().lower().replace(" ", "-")
                    if s.replace("-", "").isalnum():
                        return ("org", s)
                return None

            async def gh_hydrate(it):
                tag = _gh_login(it)
                if not tag: return
                kind, login = tag
                payload = await _safe_call(
                    nc.github_org(login) if kind == "org" else nc.github_user(login),
                    f"github_{kind}:{login}", events, progress,
                )
                if not isinstance(payload, dict): return
                # Sanity: if the API returned an error-shaped payload, skip
                if payload.get("error") or (payload.get("login") is None and payload.get("username") is None):
                    return
                it.setdefault("githubLogin", login)
                it.setdefault("githubUrl", payload.get("org_url") or payload.get("profile_url") or f"https://github.com/{login}")
                it.setdefault("githubBio", payload.get("bio") or payload.get("description"))
                it.setdefault("githubFollowers", payload.get("followers"))
                it.setdefault("githubPublicRepos", payload.get("public_repos"))
                it.setdefault("githubLocation", payload.get("location"))
                it.setdefault("githubEmail", payload.get("email"))
                it.setdefault("githubWebsite", payload.get("website"))
                it.setdefault("githubAvatar", payload.get("avatar_url"))
                # Languages from pinned repos
                pinned = payload.get("pinned_repos") or []
                langs = sorted({r.get("language") for r in pinned if isinstance(r, dict) and r.get("language")})
                if langs: it.setdefault("githubLanguages", ", ".join(langs))

            await asyncio.gather(*[gh_hydrate(it) for it in raw_items[:row_limit]])
            await emit({"type": "github_enrich_done"})

        # Email finder — multi-strategy per-row enrichment:
        #   1. /by-linkedin if we have a LinkedIn URL → highest accuracy
        #   2. /by-name if we have name + domain
        #   3. /by-domain bulk fallback → returns up to 20 emails per company
        # Person-only rows (no company) skip; company-only rows use /by-domain.
        domain_emails: dict[str, list[dict]] = {}
        if needs_email(headers):
            await emit({"type": "stage", "stage": "emails"})

            def _person_name(it: dict) -> str:
                return (it.get("fullName") or it.get("full_name") or it.get("name")
                        or " ".join(x for x in [it.get("firstName"), it.get("lastName")] if x).strip()
                        or "")

            def _person_domain(it: dict) -> str:
                return _domain_of(
                    it.get("website") or it.get("domain") or it.get("url")
                    or (it.get("currentCompanyURL") or "")
                    or it.get("companyWebsite") or ""
                )

            def _linkedin_url(it: dict) -> str:
                u = it.get("linkedinURL") or it.get("linkedinUrl") or it.get("linkedin") or it.get("linkedin_url")
                if u: return u
                un = it.get("username")
                if un and (it.get("fullName") or it.get("firstName") or it.get("headline")):
                    return f"https://www.linkedin.com/in/{un}"
                return ""

            async def find_email_for_row(it: dict):
                # 1. LinkedIn URL path
                li = _linkedin_url(it)
                if li:
                    p = await _safe_call(nc.email_by_linkedin(li), f"email_by_linkedin:{li[-30:]}", events, progress)
                    if isinstance(p, dict) and (p.get("valid_email") or p.get("email")):
                        it.setdefault("email", p.get("valid_email") or p.get("email"))
                        it.setdefault("emailStatus", p.get("email_status"))
                        return
                # 2. Name + domain path
                name = _person_name(it)
                dom = _person_domain(it)
                if name and (dom or it.get("currentCompany") or it.get("companyName")):
                    p = await _safe_call(
                        nc.email_by_name(full_name=name, domain=dom,
                                         company_name=it.get("currentCompany") or it.get("companyName") or ""),
                        f"email_by_name:{name[:20]}", events, progress)
                    if isinstance(p, dict) and (p.get("valid_email") or p.get("email")):
                        it.setdefault("email", p.get("valid_email") or p.get("email"))
                        it.setdefault("emailStatus", p.get("email_status"))
                        return

            # Fire per-row name/linkedin lookups in parallel
            await asyncio.gather(*[find_email_for_row(it) for it in raw_items[:row_limit]])

            # Bulk by-domain: covers company-only rows + any person rows that didn't resolve
            domains: list[str] = []
            for it in raw_items[:row_limit]:
                if it.get("email"): continue  # already resolved per-row
                d = _person_domain(it)
                if d and d not in domains and "." in d and " " not in d:
                    domains.append(d)
            async def fetch_domain(d):
                p = await _safe_call(nc.email_by_domain(domain=d), f"email_by_domain:{d}", events, progress)
                if not isinstance(p, dict): return d, []
                emails = p.get("valid_emails") or p.get("emails") or []
                return d, [{"email": e, "email_status": p.get("email_status", "valid")} for e in emails if isinstance(e, str)]
            if domains:
                results = await asyncio.gather(*[fetch_domain(d) for d in domains])
                for d, items in results:
                    if items: domain_emails[d] = items

            await emit({"type": "emails_fetched",
                        "per_row": sum(1 for it in raw_items[:row_limit] if it.get("email")),
                        "domains": list(domain_emails.keys())})

    # 3. Optional: intelligence scan for additional signal
    intel: list[dict] = []
    if query and aiassist_key:
        await emit({"type": "stage", "stage": "intelligence"})
        intel = await scan_signals([query], api_key=aiassist_key, limit=10)
        await emit({"type": "intel_done", "count": len(intel)})

    # 4. LLM batch normalization → into our rows
    await emit({"type": "stage", "stage": "llm_enrich"})
    try:
        rows = await _llm_normalize(headers, query, raw_items, domain_emails, intel, row_limit, aiassist_key, aiassist_model, aiassist_provider)
    except LLMError as e:
        events.append({"type": "llm_error", "error": str(e)})
        rows = _fallback_rows(headers, raw_items, domain_emails, row_limit)

    # 4b. FAILSAFE: post-LLM backfill — the LLM occasionally drops booleans
    # (False), zeros, and fields it "didn't trust". Walk every cell that
    # came back null and try to rescue it from the matching raw item.
    rescued = _backfill_nulls(rows, raw_items, headers, domain_emails)
    if rescued:
        await emit({"type": "backfill", "rescued_cells": rescued})

    # 5. Email verification on any email cells
    if needs_email(headers):
        await emit({"type": "stage", "stage": "verifying"})
        rows = await _verify_email_cells(rows)

    await emit({"type": "done", "rows": len(rows)})
    return rows


async def _call_endpoint(nc: NetrowsClient, source: str, query: str, limit: int,
                         plan: Optional[dict] = None, page: int = 1):
    """Call a Netrows producer endpoint. When `plan` is provided, use the
    decomposed search_keyword + resolved geo/industry IDs so structured filters
    actually narrow the result set. `page` is 1-indexed; only the LinkedIn
    sources paginate — others ignore it. Falls back to raw query when plan is
    None or the source doesn't support structured params."""
    p = plan or {}
    kw = p.get("search_keyword") or query
    geo_id = p.get("location_geo_id") or ""
    industry_id = p.get("industry_id") or ""
    emp_min = p.get("employee_min")
    emp_max = p.get("employee_max")

    if source == "linkedin_people":
        # /people/search is offset-based: ~10 per page, so start = (page-1) * 10
        return await nc.linkedin_people(kw, limit=limit, geo_id=geo_id, industry_id=industry_id,
                                        start=max(0, (page - 1) * 10))
    if source == "linkedin_companies":
        return await nc.linkedin_companies(
            kw, limit=limit, geo_id=geo_id, industry_id=industry_id,
            employee_min=emp_min, employee_max=emp_max, page=page,
        )
    if source == "google_search":
        # Google search benefits from the FULL natural query (semantic SERP).
        if page > 1:
            return None
        return await nc.google_search(query, limit=limit)
    if source == "google_maps":
        if page > 1:
            return None
        return await nc.google_maps(query, limit=limit)
    if source == "linkedin_jobs":
        if page > 1:
            return None
        return await nc.linkedin_jobs(kw, limit=limit, geo_id=geo_id, industry_id=industry_id)
    if source == "indeed_jobs":
        if page > 1:
            return None
        return await nc.indeed_jobs(kw, location=p.get("location_name") or "", limit=limit)
    if source == "yc_search":
        if page > 1:
            return None
        return await nc.yc_search(query=kw, per_page=max(limit, 20))
    # GitHub sources are enrichers handled separately — not callable as primary producer
    return None


def _fallback_cell(h: str, it: dict, domain_emails: dict[str, list[dict]]) -> Any:
    """Best-effort map a single header to a value from a raw item.

    Centralized so both the no-LLM path AND the post-LLM backfill pass
    use identical logic. Returns None if no mapping found.
    Important: preserves booleans (False) and zeros — does NOT use truthiness.
    """
    nh = normalize_header(h)
    # Direct key hits — distinguish 'missing' from 'False/0'
    for k in (nh, nh.replace(" ", "_"), nh.replace(" ", "")):
        if k in it and it[k] not in (None, ""):
            return it[k]
    v = None
    if nh in ("company name", "company", "current company"):
        v = it.get("currentCompany") or it.get("companyName") or it.get("name")
        if v is None:
            c = it.get("company")
            v = c if isinstance(c, str) else (c.get("name") if isinstance(c, dict) else None)
    elif nh == "website": v = it.get("website") or it.get("url") or it.get("domain")
    elif nh == "industry": v = it.get("industry")
    elif nh in ("location", "headquarters", "hq"): v = it.get("headquarters") or it.get("location") or it.get("address")
    elif nh in ("year founded", "founded", "founded year"): v = it.get("year_founded") or it.get("founded")
    elif nh == "tagline": v = it.get("tagline")
    elif nh in ("size", "team size", "company size"): v = it.get("team_size") or it.get("companySize") or it.get("size")
    elif nh in ("employee count", "employees"): v = it.get("employeeCount") or it.get("employee_count")
    elif nh in ("logo", "logo url"): v = it.get("logo_url") or it.get("small_logo_url") or it.get("logo") or it.get("image")
    elif nh in ("description",): v = it.get("description") or it.get("tagline")
    elif nh in ("name", "contact name", "full name"):
        v = it.get("fullName") or it.get("name") or it.get("full_name")
        if not v and (it.get("firstName") or it.get("lastName")):
            v = " ".join(x for x in [it.get("firstName"), it.get("lastName")] if x)
    elif nh in ("first name",): v = it.get("firstName")
    elif nh in ("last name",): v = it.get("lastName")
    elif nh in ("title", "headline", "job title", "role", "position"):
        v = it.get("currentTitle") or it.get("headline") or it.get("title") or it.get("position") or it.get("summary")
    elif nh in ("school", "education", "university"): v = it.get("school")
    elif nh in ("country",):
        geo = it.get("geo") if isinstance(it.get("geo"), dict) else {}
        v = it.get("country") or geo.get("country")
    elif nh in ("city",):
        geo = it.get("geo") if isinstance(it.get("geo"), dict) else {}
        v = it.get("city") or geo.get("city")
    elif nh in ("skills",):
        sk = it.get("skills") or []
        if sk and isinstance(sk[0], str): v = ", ".join(sk[:8])
        else: v = ", ".join(s.get("name") for s in sk[:8] if isinstance(s, dict) and s.get("name")) or None
    elif nh in ("job id", "jobid"): v = it.get("jobId") or it.get("id")
    elif nh in ("salary", "compensation", "pay"):
        v = it.get("salaryRange") or it.get("salary")
        if not isinstance(v, (str, int, float)): v = None
    elif nh in ("workplace", "workplace type", "remote"): v = it.get("workplaceType")
    elif nh in ("employment type", "type"): v = it.get("employmentType")
    elif nh in ("experience", "experience level", "level"): v = it.get("experienceLevel")
    elif nh in ("posted", "posted at", "posted date", "date posted"): v = it.get("postedAt") or it.get("posted_date")
    elif nh in ("applicants",): v = it.get("applicants")
    elif nh in ("apply url", "apply", "application url"): v = it.get("applyUrl")
    elif nh in ("github", "github url"):
        v = it.get("githubUrl") or it.get("github_url") or it.get("github")
        if not v and it.get("githubLogin"): v = f"https://github.com/{it['githubLogin']}"
    elif nh in ("followers",): v = it.get("githubFollowers") or it.get("followers")
    elif nh in ("public repos", "repos"): v = it.get("githubPublicRepos") or it.get("public_repos")
    elif nh in ("tech stack", "languages"): v = it.get("githubLanguages")
    elif nh in ("avatar",): v = it.get("githubAvatar") or it.get("avatar_url") or it.get("founderAvatar")
    # ---- YC ----
    elif nh in ("batch", "yc batch"): v = it.get("batch") or it.get("batch_name")
    elif nh in ("one liner",): v = it.get("one_liner") or it.get("tagline") or it.get("headline")
    elif nh in ("yc url", "ycombinator url"):
        v = it.get("ycombinator_url") or (f"https://www.ycombinator.com/companies/{it.get('slug')}" if it.get("slug") else None)
    elif nh in ("twitter", "twitter url"): v = it.get("twitter_url") or it.get("twitter") or it.get("founderTwitter")
    elif nh in ("facebook", "facebook url"): v = it.get("facebook_url")
    elif nh in ("crunchbase url",): v = it.get("crunchbase_url") or it.get("cb_source_url")
    # ---- Crunchbase company ----
    elif nh in ("funding", "funding total"):
        ft = it.get("fundingTotalUsd") or it.get("cb_funding_total_usd")
        v = (f"${ft:,}" if isinstance(ft, (int, float)) else ft)
    elif nh in ("last funding",):
        lft, lfa = it.get("lastFundingType"), it.get("lastFundingAt")
        v = " ".join(x for x in [lft, lfa] if x) or None
    elif nh in ("investors",): v = it.get("cbInvestors")
    elif nh in ("num investors",): v = it.get("numInvestors")
    elif nh in ("operating status",): v = it.get("operatingStatus")
    elif nh in ("ipo status",): v = it.get("ipoStatus")
    elif nh in ("monthly visits",): v = it.get("monthlyVisits")
    elif nh in ("heat score",): v = it.get("heatScore")
    elif nh in ("technologies", "tech"): v = it.get("cbTechnologies")
    elif nh in ("acquisitions",): v = it.get("cbAcquisitionsCount")
    elif nh in ("rank",): v = it.get("cb_rank") or it.get("rank")
    # ---- Crunchbase person ----
    elif nh in ("is investor",):
        iv = it.get("isInvestor")
        v = ("Yes" if iv else "No") if iv is not None else None
    elif nh in ("portfolio", "investments"): v = it.get("portfolio")
    elif nh in ("exits",): v = it.get("exitCompanies") or it.get("numExits")
    elif nh in ("board roles",): v = it.get("boardRoles")
    elif nh in ("status",): v = it.get("status")
    elif nh in ("is hiring", "hiring"):
        h_ = it.get("is_hiring")
        v = ("Yes" if h_ else "No") if h_ is not None else None
    elif nh in ("top company",):
        t = it.get("top_company")
        v = ("Yes" if t else "No") if t is not None else None
    elif nh in ("regions",):
        rs = it.get("regions"); v = ", ".join(rs) if isinstance(rs, list) else rs
    elif nh in ("tags",):
        ts = it.get("tags"); v = ", ".join(ts) if isinstance(ts, list) else ts
    elif nh in ("founder", "founders", "founder name"): v = it.get("founderNames") or it.get("founderName")
    elif nh in ("founder linkedin", "founder linkedin url"): v = it.get("founderLinkedIn")
    elif nh in ("founder twitter",): v = it.get("founderTwitter")
    elif nh in ("founder title",): v = it.get("founderTitle")
    elif nh in ("open jobs", "open jobs count"): v = it.get("openJobsCount")
    elif nh in ("long description",): v = it.get("long_description")
    # ---- Google Maps ----
    elif nh in ("phone", "phone number"): v = it.get("phone")
    elif nh in ("hours", "opening hours"):
        h_ = it.get("hours")
        if isinstance(h_, list): v = "; ".join(str(x) for x in h_)
        elif isinstance(h_, dict): v = "; ".join(f"{k}: {v_}" for k, v_ in h_.items())
        else: v = h_
    elif nh in ("rating",): v = it.get("rating")
    elif nh in ("review count", "reviews"): v = it.get("review_count") or it.get("total_reviews")
    elif nh in ("latitude", "lat"): v = it.get("latitude") or it.get("lat")
    elif nh in ("longitude", "lng", "lon"): v = it.get("longitude") or it.get("lng")
    elif nh in ("feature id", "place id"): v = it.get("feature_id") or it.get("place_id")
    elif nh in ("categories",):
        c = it.get("cbCategories") or it.get("categories")
        v = ", ".join(c) if isinstance(c, list) else c
    # ---- Indeed ----
    elif nh in ("job key",): v = it.get("job_key")
    elif nh in ("snippet",): v = it.get("snippet")
    elif nh in ("is remote",):
        ir = it.get("is_remote")
        v = ("Yes" if ir else "No") if ir is not None else None
    elif nh in ("indeed url",): v = it.get("indeed_url")
    elif nh in ("state",): v = it.get("state")
    elif nh in ("salary min",): v = it.get("salary_min")
    elif nh in ("salary max",): v = it.get("salary_max")
    elif nh in ("salary range",):
        lo, hi = it.get("salary_min"), it.get("salary_max")
        if lo and hi: v = f"${lo:,} - ${hi:,}"
        else: v = it.get("salary")
    elif nh in ("benefits",):
        b = it.get("benefits")
        v = ", ".join(b) if isinstance(b, list) else b
    elif nh in ("description text",): v = it.get("description_text")
    elif nh in ("company rating",): v = it.get("indeed_rating") or it.get("company_rating")
    elif nh in ("ceo approval",): v = it.get("indeed_ceo_approval")
    elif nh in ("interview difficulty",): v = it.get("interviewDifficulty")
    elif nh in ("interview duration",): v = it.get("interviewDuration")
    elif nh in ("interview experience",): v = it.get("interviewExperience")
    elif nh in ("rating breakdown",): v = it.get("ratingBreakdown")
    elif nh in ("popular job titles",): v = it.get("popularJobTitles")
    elif nh in ("pros",): v = it.get("indeedPros")
    elif nh in ("cons",): v = it.get("indeedCons")
    elif nh in ("review insights",):
        p_, c_ = it.get("indeedPros") or "", it.get("indeedCons") or ""
        v = (f"Pros: {p_} | Cons: {c_}" if (p_ or c_) else None)
    elif nh in ("salary satisfaction",): v = it.get("salarySatisfaction")
    elif nh in ("total salary reports",): v = it.get("totalSalaryReports")
    # ---- LinkedIn ----
    elif nh in ("linkedin url", "linkedin"):
        v = it.get("linkedinURL") or it.get("linkedinUrl") or it.get("linkedin")
        if not v:
            u = it.get("username"); un = it.get("universalName")
            if un: v = f"https://linkedin.com/company/{un}"
            elif u: v = f"https://linkedin.com/in/{u}"
    elif nh in ("profile picture", "picture", "avatar"): v = it.get("profilePicture")
    elif nh in ("summary", "bio"): v = it.get("summary") or it.get("bio")
    elif nh == "email":
        d = _domain_of(it.get("website") or it.get("url") or "")
        em = domain_emails.get(d, [])
        if em: v = em[0].get("email") or em[0].get("value")
    return v


def _fallback_rows(headers: list[str], items: list[dict], domain_emails: dict[str, list[dict]], row_limit: int) -> list[dict]:
    """Build rows from raw items without LLM, best-effort key matching."""
    rows: list[dict] = []
    for it in items[:row_limit]:
        row: dict[str, Any] = {}
        for h in headers:
            v = _fallback_cell(h, it, domain_emails)
            row[h] = _cell(v, "netrows", "medium" if v not in (None, "") else "low")
        rows.append(row)
    return rows


def _row_signature(row: dict, headers: list[str]) -> dict:
    """Extract identity-shaped values from an LLM-produced row for matching."""
    sig: dict[str, str] = {}
    for h in headers:
        cell = row.get(h)
        v = cell.get("value") if isinstance(cell, dict) else cell
        if v in (None, ""): continue
        nh = normalize_header(h)
        s = str(v).strip().lower()
        if nh in ("name", "full name", "contact name", "first name", "last name"):
            sig.setdefault("name", s)
        elif nh in ("company name", "company", "current company"):
            sig["company"] = s
        elif nh in ("website", "url", "domain"):
            sig["domain"] = _domain_of(s)
        elif nh in ("linkedin url", "linkedin"):
            sig["linkedin"] = s
        elif nh in ("github url", "github"):
            sig["github"] = s
        elif nh in ("email",):
            sig["email"] = s
        elif nh in ("slug", "yc url", "ycombinator url"):
            sig.setdefault("slug", s.rsplit("/", 1)[-1])
        elif nh in ("phone", "phone number"):
            sig["phone"] = s
        elif nh in ("address",):
            sig["address"] = s
        elif nh in ("title", "headline", "job title", "role"):
            sig.setdefault("title", s)
    return sig


def _item_signature(it: dict) -> dict:
    sig: dict[str, str] = {}
    nm = (it.get("fullName") or it.get("name") or it.get("full_name")
          or " ".join(x for x in [it.get("firstName"), it.get("lastName")] if x).strip())
    if nm and not (it.get("companyName") and not it.get("fullName") and not it.get("firstName")):
        sig["name"] = str(nm).strip().lower()
    co = it.get("currentCompany") or it.get("companyName") or (it.get("name") if it.get("companyName") or it.get("companyId") or it.get("website") else None)
    if co: sig["company"] = str(co).strip().lower()
    web = it.get("website") or it.get("url") or it.get("domain") or it.get("currentCompanyURL") or it.get("companyWebsite")
    if web: sig["domain"] = _domain_of(str(web))
    li = it.get("linkedinURL") or it.get("linkedinUrl") or it.get("linkedin") or it.get("linkedin_url")
    if li: sig["linkedin"] = str(li).strip().lower()
    elif it.get("username") and (it.get("fullName") or it.get("firstName")):
        sig["linkedin"] = f"https://www.linkedin.com/in/{it['username']}".lower()
    gh = it.get("githubUrl") or it.get("github_url") or it.get("github")
    if gh: sig["github"] = str(gh).strip().lower()
    elif it.get("githubLogin"): sig["github"] = f"https://github.com/{it['githubLogin']}".lower()
    em = it.get("email")
    if em: sig["email"] = str(em).strip().lower()
    sl = it.get("slug") or it.get("permalink") or it.get("companyUsername") or it.get("universalName")
    if sl: sig["slug"] = str(sl).strip().lower()
    ph = it.get("phone")
    if ph: sig["phone"] = str(ph).strip().lower()
    addr = it.get("address")
    if addr: sig["address"] = str(addr).strip().lower()
    t = it.get("currentTitle") or it.get("headline") or it.get("title")
    if t: sig["title"] = str(t).strip().lower()
    return sig


def _match_score(row_sig: dict, item_sig: dict) -> int:
    """Higher = better. Strong identity matches weighted heavily."""
    if not row_sig or not item_sig: return 0
    score = 0
    # Strong identity keys
    for k, w in (("linkedin", 10), ("github", 10), ("email", 9), ("slug", 8),
                 ("domain", 6), ("phone", 6), ("address", 5)):
        a, b = row_sig.get(k), item_sig.get(k)
        if a and b and a == b: score += w
    # Fuzzy name/company: substring either direction
    for k, w in (("name", 5), ("company", 4), ("title", 2)):
        a, b = row_sig.get(k), item_sig.get(k)
        if a and b and (a == b or a in b or b in a): score += w
    return score


def _match_items_to_rows(rows: list[dict], items: list[dict], headers: list[str]) -> list[Optional[dict]]:
    """For each LLM-produced row, return the best-matching raw item (or None).
    Items can be reused across rows when the LLM legitimately splits one (rare),
    but we prefer 1:1 by tracking used indices and only allowing reuse when no
    unused item scores >0.
    """
    item_sigs = [_item_signature(it) if isinstance(it, dict) else {} for it in items]
    used: set[int] = set()
    out: list[Optional[dict]] = []
    for r in rows:
        rsig = _row_signature(r, headers)
        best_i, best_s = -1, 0
        # Pass 1: prefer unused items
        for i, isig in enumerate(item_sigs):
            if i in used: continue
            s = _match_score(rsig, isig)
            if s > best_s: best_s, best_i = s, i
        # Pass 2: fallback to any item if nothing matched
        if best_i < 0 or best_s == 0:
            for i, isig in enumerate(item_sigs):
                s = _match_score(rsig, isig)
                if s > best_s: best_s, best_i = s, i
        if best_i >= 0 and best_s > 0:
            used.add(best_i)
            out.append(items[best_i])
        else:
            out.append(None)
    return out


def _backfill_nulls(rows: list[dict], items: list[dict], headers: list[str],
                    domain_emails: dict[str, list[dict]]) -> int:
    """Failsafe: after LLM normalization, walk every cell and rescue nulls
    by re-deriving from the matched raw item. The LLM occasionally drops
    booleans (False), zeros, or fields it 'didn't trust'. We trust the raw
    Netrows payload more than the LLM's selective omission.

    Critically, we IDENTITY-MATCH rows to items (not positional) — raw_items
    mixes primary + secondary producers and the LLM re-orders/dedupes, so
    items[i] is rarely the right source for rows[i].

    Returns: count of cells backfilled (for telemetry).
    """
    matched = _match_items_to_rows(rows, items, headers)
    rescued = 0
    for row, it in zip(rows, matched):
        if not isinstance(it, dict): continue
        for h in headers:
            cell = row.get(h)
            cur = cell.get("value") if isinstance(cell, dict) else cell
            if cur not in (None, ""): continue
            v = _fallback_cell(h, it, domain_emails)
            if v in (None, ""): continue
            if isinstance(cell, dict):
                row[h] = {**cell, "value": v, "source": cell.get("source") or "netrows",
                          "confidence": cell.get("confidence") or "medium"}
            else:
                row[h] = _cell(v, "netrows", "medium")
            rescued += 1
    return rescued


async def _llm_normalize(
    headers: list[str], query: str,
    items: list[dict], domain_emails: dict[str, list[dict]], intel: list[dict],
    row_limit: int, api_key: Optional[str], model: Optional[str], provider: Optional[str] = None,
) -> list[dict]:
    """Ask LLM to merge raw items into row_limit rows matching headers exactly."""
    # Trim payloads to keep prompt small
    trimmed = [_trim_item(x) for x in items[:max(row_limit * 3, 15)]]
    emails_compact = {d: [e.get("email") for e in lst[:5] if e.get("email")] for d, lst in domain_emails.items()}
    intel_compact = [{"source": x.get("source"), "title": x.get("title"), "url": x.get("url")} for x in intel[:10]]

    system = (
        "You are a data normalizer for a spreadsheet generator called Genshi. "
        "Take raw scraped items and produce exactly the requested rows matching the user's headers. "
        "Use ONLY information present in the raw data — do not invent companies, names, emails, or websites. "
        "PRESERVE booleans literally: false stays false (NOT null), 0 stays 0 (NOT null). "
        "PRESERVE numeric ratings/counts/salaries even if low — never round to null. "
        "Only set a field to null if the raw item truly contains no usable information for it. "
        "Match emails to companies by domain. "
        "Output strict JSON: {\"rows\": [{\"<header>\": <value>, ...}, ...]}. "
        f"Produce up to {row_limit} rows. The keys of each row object MUST be EXACTLY these headers (case-sensitive): {headers}."
    )
    user = json.dumps({
        "headers": headers,
        "query": query,
        "row_limit": row_limit,
        "raw_items": trimmed,
        "emails_by_domain": emails_compact,
        "signals": intel_compact,
    })
    out = await chat_json([
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ], api_key=api_key, model=model, provider=provider)

    rows_out = []
    raw_rows = out.get("rows") if isinstance(out, dict) else (out if isinstance(out, list) else [])

    def _row_get(r: dict, h: str):
        # Try exact, normalized, snake_case, lowercase, title-case variants.
        # LLMs frequently re-shape header keys despite instructions.
        if h in r and r[h] not in (None, ""): return r[h]
        nh = normalize_header(h)
        candidates = {nh, nh.replace(" ", "_"), nh.replace(" ", ""),
                      h.lower(), h.lower().replace(" ", "_"),
                      h.replace(" ", "_"), h.replace(" ", "")}
        # Build a case-insensitive index of the row's keys
        lower_idx = {k.lower(): k for k in r.keys() if isinstance(k, str)}
        for c in candidates:
            if c in r and r[c] not in (None, ""): return r[c]
            if c.lower() in lower_idx:
                v = r[lower_idx[c.lower()]]
                if v not in (None, ""): return v
        return None

    for r in (raw_rows or [])[:row_limit]:
        if not isinstance(r, dict):
            continue
        cells: dict[str, Any] = {}
        for h in headers:
            v = _row_get(r, h)
            src = "netrows" if v not in (None, "") else "ai"
            conf = "medium" if v not in (None, "") else "low"
            # Heuristic: if value came from an email finder result, mark it
            if normalize_header(h) == "email" and v:
                conf = "high"
                src = "email_finder"
            cells[h] = _cell(v, src, conf)
        rows_out.append(cells)
    return rows_out


def _trim_item(it: dict) -> dict:
    keep = {}
    for k, v in it.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            if isinstance(v, str) and len(v) > 300:
                v = v[:300]
            keep[k] = v
        elif isinstance(v, list) and len(v) <= 10:
            keep[k] = [x for x in v if isinstance(x, (str, int, float, bool))][:10]
    return keep


async def _verify_email_cells(rows: list[dict]) -> list[dict]:
    tasks = []
    locations: list[tuple[int, str]] = []
    for i, row in enumerate(rows):
        for h, cell in row.items():
            if "email" in h.lower() and isinstance(cell, dict) and cell.get("value"):
                locations.append((i, h))
                tasks.append(verify_email(str(cell["value"])))
    if not tasks:
        return rows
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for (i, h), res in zip(locations, results):
        if isinstance(res, Exception):
            rows[i][h]["verification"] = {"status": "unknown", "reason": str(res)[:80]}
            continue
        rows[i][h]["verification"] = {
            "status": res["status"],
            "reason": res.get("reason", ""),
            "catch_all": res.get("catch_all"),
        }
        # Adjust confidence
        if res["status"] == "verified":
            rows[i][h]["confidence"] = "verified"
        elif res["status"] == "invalid":
            rows[i][h]["confidence"] = "invalid"
        elif res["status"] == "uncertain":
            rows[i][h]["confidence"] = "uncertain"
    return rows


async def re_enrich_cell(row: dict, header: str, headers: list[str], query: str, aiassist_key: Optional[str] = None, model: Optional[str] = None, provider: Optional[str] = None) -> dict:
    """Re-ask LLM to fill a single cell using row context."""
    context = {h: (row.get(h, {}).get("value") if isinstance(row.get(h), dict) else row.get(h)) for h in headers}
    system = (
        "You re-fill a single missing/incorrect cell in a spreadsheet row using the rest of the row as context. "
        "Use only knowledge consistent with the other fields. Output strict JSON: {\"value\": <string-or-null>}. "
        "If you cannot find a confident answer, return null."
    )
    user = json.dumps({"row": context, "header_to_fill": header, "sheet_query": query, "all_headers": headers})
    out = await chat_json([
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ], api_key=aiassist_key, model=model, provider=provider)
    val = out.get("value") if isinstance(out, dict) else None
    cell = _cell(val, "ai", "low" if val else "low")
    if header.lower() == "email" and val:
        v = await verify_email(str(val))
        cell["verification"] = {"status": v["status"], "reason": v.get("reason", "")}
        cell["confidence"] = v["status"] if v["status"] in ("verified", "invalid", "uncertain") else "low"
    return cell
