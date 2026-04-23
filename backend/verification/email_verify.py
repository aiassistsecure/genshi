"""Email verification: syntax + MX + SMTP probe + catch-all + disposable.

Designed to fail gracefully — any step that errors returns "unknown" rather than blocking.
SMTP probe requires outbound port 25 (typically blocked on cloud hosts). Will degrade to
"uncertain" with a "smtp_blocked" reason on failure.
"""
from __future__ import annotations
import asyncio
import socket
import re
import random
import string
from typing import Optional

import dns.resolver  # type: ignore
import dns.exception  # type: ignore

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

DISPOSABLE_DOMAINS = {
    "mailinator.com", "guerrillamail.com", "10minutemail.com", "tempmail.com",
    "trashmail.com", "yopmail.com", "throwaway.email", "fakeinbox.com",
    "maildrop.cc", "getnada.com", "sharklasers.com",
}


def _syntax_ok(email: str) -> bool:
    return bool(EMAIL_RE.match(email or ""))


async def _resolve_mx(domain: str) -> list[str]:
    loop = asyncio.get_event_loop()
    def _q():
        try:
            answers = dns.resolver.resolve(domain, "MX", lifetime=5.0)
            return sorted([str(r.exchange).rstrip(".") for r in answers])
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.DNSException):
            try:
                answers = dns.resolver.resolve(domain, "A", lifetime=5.0)
                return [domain] if list(answers) else []
            except Exception:
                return []
    return await loop.run_in_executor(None, _q)


async def _smtp_probe(mx_host: str, email: str, from_addr: str = "verify@genshi.local", timeout: float = 8.0) -> tuple[Optional[bool], str]:
    """Returns (deliverable, reason)."""
    loop = asyncio.get_event_loop()
    def _probe():
        try:
            sock = socket.create_connection((mx_host, 25), timeout=timeout)
        except (socket.timeout, OSError) as e:
            return None, f"smtp_blocked:{e.__class__.__name__}"
        try:
            sock.settimeout(timeout)
            def recv():
                data = b""
                while True:
                    try:
                        chunk = sock.recv(4096)
                    except Exception:
                        break
                    if not chunk:
                        break
                    data += chunk
                    if b"\r\n" in data and not data.endswith(b"-"):
                        break
                    if len(data) > 8192:
                        break
                return data.decode("utf-8", errors="ignore")
            def send(line: str):
                sock.sendall((line + "\r\n").encode())
            recv()  # banner
            send("EHLO genshi.local"); recv()
            send(f"MAIL FROM:<{from_addr}>"); recv()
            send(f"RCPT TO:<{email}>")
            resp = recv()
            send("QUIT")
            try: sock.close()
            except: pass
            code = resp.strip().split(" ", 1)[0] if resp else ""
            if code.startswith("2"):
                return True, "smtp_ok"
            if code.startswith("5"):
                return False, f"smtp_reject:{code}"
            return None, f"smtp_inconclusive:{code or 'noresp'}"
        except Exception as e:
            try: sock.close()
            except: pass
            return None, f"smtp_error:{e.__class__.__name__}"
    return await loop.run_in_executor(None, _probe)


async def verify_email(email: str) -> dict:
    email = (email or "").strip()
    out = {"email": email, "status": "invalid", "reason": "", "mx": [], "catch_all": None}
    if not _syntax_ok(email):
        out["reason"] = "bad_syntax"
        return out
    domain = email.rsplit("@", 1)[1].lower()
    if domain in DISPOSABLE_DOMAINS:
        out.update(status="invalid", reason="disposable")
        return out
    mxs = await _resolve_mx(domain)
    out["mx"] = mxs
    if not mxs:
        out.update(status="invalid", reason="no_mx")
        return out
    # SMTP probe on first MX
    deliverable, reason = await _smtp_probe(mxs[0], email)
    out["reason"] = reason
    if deliverable is False:
        out["status"] = "invalid"
        return out
    if deliverable is True:
        # Check catch-all by probing a random address
        rnd = "".join(random.choices(string.ascii_lowercase + string.digits, k=14))
        bogus = f"{rnd}@{domain}"
        ca, _ = await _smtp_probe(mxs[0], bogus)
        out["catch_all"] = bool(ca)
        out["status"] = "uncertain" if ca else "verified"
        return out
    # Inconclusive (smtp blocked, etc.) — degrade gracefully
    out["status"] = "uncertain"
    return out
