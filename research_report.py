"""
Research report fetcher — called by the ML sidecar HTTP server.
Fetches institutional research reports directly from Eastmoney (no akshare dependency).
"""

import json
import logging
import time
import random
from datetime import datetime, timedelta

logger = logging.getLogger("sidecar.research_report")

REPORT_API = "https://reportapi.eastmoney.com/report/list"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# ── Lazy requests session for Eastmoney rate limiting ──
_session = None
_last_call = 0.0
_MIN_INTERVAL = 1.0


def _get_session():
    global _session
    if _session is None:
        import requests
        _session = requests.Session()
        _session.headers.update({"User-Agent": UA})
        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            _session.mount("https://", HTTPAdapter(max_retries=Retry(
                total=3, connect=3, backoff_factor=0.6,
                status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])))
        except Exception:
            pass
    return _session


def _em_throttle():
    global _last_call
    wait = _MIN_INTERVAL - (time.time() - _last_call)
    if wait > 0:
        time.sleep(wait + random.uniform(0.1, 0.5))
    _last_call = time.time()


def fetch_research_reports(codes):
    """Fetch research reports for a list of stock codes.

    Returns: dict[str, list[dict]] — map of stock_code -> list of report dicts
    """
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    result = {}

    for code in codes:
        code = str(code).strip()
        if not code:
            continue
        try:
            all_records = []
            for page in range(1, 6):  # max 5 pages
                _em_throttle()
                params = {
                    "industryCode": "*", "pageSize": "100", "industry": "*",
                    "rating": "*", "ratingChange": "*",
                    "beginTime": "2000-01-01", "endTime": "2030-01-01",
                    "pageNo": str(page), "fields": "", "qType": "0",
                    "orgCode": "", "code": code, "rcode": "",
                    "p": str(page), "pageNum": str(page), "pageNumber": str(page),
                }
                r = _get_session().get(REPORT_API, params=params,
                    headers={"Referer": "https://data.eastmoney.com/"}, timeout=30)
                d = r.json()
                rows = d.get("data") or []
                if not rows:
                    break
                all_records.extend(rows)
                if page >= (d.get("TotalPage", 1) or 1):
                    break

            # Filter by date
            filtered = []
            for r in all_records:
                pub_date = (r.get("publishDate") or "")[:10]
                if pub_date >= cutoff:
                    # Clean up for JSON serialization
                    clean = {}
                    for k, v in r.items():
                        if hasattr(v, 'item'):
                            clean[str(k)] = v.item()
                        else:
                            clean[str(k)] = v
                    filtered.append(clean)

            result[code] = filtered
            logger.info("research_report: %s -> %d reports", code, len(filtered))
        except Exception as e:
            logger.warning("research_report: skip %s: %s", code, e)
            result[code] = []

    return result
