import argparse
import base64
import concurrent.futures
import json
import random
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from district_casestatus_scraper import DistrictCaseStatusScraper
from benchmark_case_status import parse_case_detail_html


DISPLAY_PDF_RE = re.compile(
    r"displayPdf\('([^']*)','([^']*)','([^']*)','([^']*)','([^']*)'\)"
)
VIEW_HISTORY_RE = re.compile(
    r"viewHistory\((\d+),'([^']*)',(\d+),'([^']*)','([^']*)',(\d+),(\d+),(\d+),'([^']*)'\)"
)
PARTY_SPLIT_RE = re.compile(r"\s+(?:vs\.?|v/s|versus)\s+", re.IGNORECASE)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _normalize_case_type_text(text: str) -> str:
    cleaned = (text or "").strip().lower()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"[\.\-_/]+", " ", cleaned)
    return re.sub(r"[^a-z0-9 ]+", "", cleaned).strip()


def _build_case_type_lookup(case_types: Dict[str, str]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for code, label in case_types.items():
        label_text = str(label or "").strip()
        prefix = label_text.split("-", 1)[0].strip()
        out.append(
            {
                "code": str(code or ""),
                "label": label_text,
                "norm_full": _normalize_case_type_text(label_text),
                "norm_prefix": _normalize_case_type_text(prefix),
            }
        )
    return out


def _match_case_type_code(case_type_text: str, lookup: List[Dict[str, str]]) -> Optional[str]:
    normalized = _normalize_case_type_text(case_type_text)
    if not normalized:
        return None
    for entry in lookup:
        if normalized == entry.get("norm_prefix"):
            return str(entry.get("code") or "")
    for entry in lookup:
        if normalized == entry.get("norm_full"):
            return str(entry.get("code") or "")
    for entry in lookup:
        norm_prefix = str(entry.get("norm_prefix") or "")
        if norm_prefix.startswith(normalized) and norm_prefix:
            return str(entry.get("code") or "")
    for entry in lookup:
        norm_full = str(entry.get("norm_full") or "")
        if normalized in norm_full and norm_full:
            return str(entry.get("code") or "")
    return None


def _to_iso_date(date_text: str) -> Optional[str]:
    text = (date_text or "").strip()
    if not text:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _split_party_caption(party_caption: str) -> Tuple[str, str]:
    text = (party_caption or "").strip()
    if not text:
        return "", ""
    parts = PARTY_SPLIT_RE.split(text, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return text, ""


def _parse_display_pdf_onclick(onclick: str) -> Optional[Dict[str, str]]:
    match = DISPLAY_PDF_RE.search(onclick or "")
    if not match:
        return None
    normal_v, case_val, court_code, filename, app_flag = match.groups()
    return {
        "normal_v": normal_v,
        "case_val": case_val,
        "court_code": court_code,
        "filename": filename,
        "app_flag": app_flag,
    }


def _parse_view_history_onclick(onclick: str) -> Optional[Dict[str, str]]:
    match = VIEW_HISTORY_RE.search(onclick or "")
    if not match:
        return None
    case_no, cino, court_code, hideparty, search_flag, state_code, dist_code, complex_code, search_by = (
        match.groups()
    )
    return {
        "case_no": case_no,
        "cino": cino,
        "court_code": court_code,
        "hideparty": hideparty,
        "search_flag": search_flag,
        "state_code": state_code,
        "dist_code": dist_code,
        "court_complex_code": complex_code,
        "search_by": search_by,
    }


def _find_view_history_params_from_row(row: Dict[str, object]) -> Optional[Dict[str, str]]:
    for key in ("col_0_onclick", "col_1_onclick", "col_2_onclick", "col_3_onclick", "col_4_onclick"):
        vp = _parse_view_history_onclick(str(row.get(key) or ""))
        if vp and vp.get("cino"):
            return vp

    row_html = str(row.get("row_html") or "")
    if row_html:
        match = VIEW_HISTORY_RE.search(row_html)
        if match:
            case_no, cino, court_code, hideparty, search_flag, state_code, dist_code, complex_code, search_by = (
                match.groups()
            )
            return {
                "case_no": case_no,
                "cino": cino,
                "court_code": court_code,
                "hideparty": hideparty,
                "search_flag": search_flag,
                "state_code": state_code,
                "dist_code": dist_code,
                "court_complex_code": complex_code,
                "search_by": search_by,
            }
    return None


def _build_dcourt_pdf_url(dcourt_base: str, cino: str, order_no: str, order_date_iso: str) -> str:
    payload = {
        "cino": str(cino),
        "order_no": str(order_no),
        "order_date": str(order_date_iso),
    }
    token = base64.b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("ascii")
    return (
        f"{dcourt_base.rstrip('/')}/wp-admin/admin-ajax.php"
        f"?es_ajax_request=1&action=get_order_pdf&input_strings={requests.utils.quote(token, safe='')}"
    )


def _build_view_history_post(params: Dict[str, str]) -> str:
    return (
        f"court_code={params['court_code']}"
        f"&state_code={params['state_code']}"
        f"&dist_code={params['dist_code']}"
        f"&court_complex_code={params['court_complex_code']}"
        f"&case_no={params['case_no']}"
        f"&cino={params['cino']}"
        f"&hideparty={params['hideparty']}"
        f"&search_flag={params['search_flag']}"
        f"&search_by={params['search_by']}"
    )


def _fetch_view_history_with_retries(
    scraper: DistrictCaseStatusScraper,
    params: Dict[str, str],
    attempts: int = 2,
) -> Tuple[Dict[str, object], str, int]:
    post_data = _build_view_history_post(params)
    last_view: Dict[str, object] = {}
    for attempt in range(1, attempts + 1):
        if params.get("state_code") and params.get("dist_code"):
            scraper._ensure_session_state(params["state_code"], params["dist_code"])
        view = scraper._ajax_post("home/viewHistory", post_data)
        if isinstance(view, dict):
            last_view = view
        html = str(last_view.get("data_list") or "").strip()
        if html:
            return last_view, html, attempt
        if attempt < attempts:
            scraper._rotate_proxy_and_reinit()
            if params.get("state_code") and params.get("dist_code"):
                scraper._ensure_session_state(params["state_code"], params["dist_code"])
            time.sleep(min(0.15 * attempt, 0.35))
    return last_view, "", attempts


def _load_proxy_pool(proxy_file: str) -> List[Dict[str, str]]:
    pool: List[Dict[str, str]] = []
    with open(proxy_file, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            parts = line.split(":")
            if len(parts) != 4:
                continue
            ip, port, user, password = parts
            proxy_url = f"http://{user}:{password}@{ip}:{port}"
            pool.append({"http": proxy_url, "https": proxy_url})
    return pool


class SharedProxyRotator:
    def __init__(self, proxy_file: str):
        self._pool = _load_proxy_pool(proxy_file)
        self._idx = 0
        self._lock = threading.Lock()

    def next_proxy(self) -> Optional[Dict[str, str]]:
        if not self._pool:
            return None
        with self._lock:
            value = self._pool[self._idx % len(self._pool)]
            self._idx += 1
        return dict(value)


def _load_dcourt_lookup(path: Path) -> Dict[Tuple[str, str], str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    lookup: Dict[Tuple[str, str], str] = {}
    for state_name, districts in raw.items():
        if not isinstance(districts, dict):
            continue
        for district_name, url in districts.items():
            if not isinstance(url, str) or not url.strip():
                continue
            key = (_normalize_text(str(state_name)), _normalize_text(str(district_name)))
            lookup[key] = url.strip()
    return lookup


def _lookup_key_to_str(key: Tuple[str, str, str]) -> str:
    return "||".join([str(key[0]), str(key[1]), str(key[2])])


def _lookup_key_from_str(value: str) -> Optional[Tuple[str, str, str]]:
    parts = str(value or "").split("||", 2)
    if len(parts) != 3:
        return None
    return (parts[0], parts[1], parts[2])


def _read_case_ref_task(path: Path, input_root: Path) -> Optional[Dict[str, object]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    case_ref = payload.get("case_reference") or {}
    jurisdiction = payload.get("jurisdiction") or {}
    query = payload.get("query") or {}
    case_type_text = str(case_ref.get("case_type_text") or "").strip()
    case_no = str(case_ref.get("case_no") or "").strip()
    year = str(case_ref.get("year") or "").strip()
    state_code = str(jurisdiction.get("state_code") or query.get("state") or "").strip()
    dist_code = str(jurisdiction.get("district_code") or query.get("district") or "").strip()
    court_complex = str(query.get("court_complex") or jurisdiction.get("court_complex_code") or "").strip()
    if not all([case_type_text, case_no, year, state_code, dist_code, court_complex]):
        return None
    rel_path = path.relative_to(input_root)
    return {
        "input_path": str(path),
        "rel_path": str(rel_path),
        "case_reference": {
            "case_type_text": case_type_text,
            "case_no": case_no,
            "year": year,
        },
        "jurisdiction": jurisdiction,
        "query": query,
        "state_code": state_code,
        "dist_code": dist_code,
        "court_complex": court_complex,
        "case_type_text": case_type_text,
        "case_no": case_no,
        "year": year,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark case-number order extraction speed from saved case-ref JSONs (URL-only, no PDF downloads)."
    )
    parser.add_argument("--input-root", type=str, default="ecourt_output_json")
    parser.add_argument("--output-root", type=str, default="_tmp/url_only_case_lookup_output")
    parser.add_argument("--dcourt-map", type=str, default="full_ecourt_crawler.json")
    parser.add_argument("--proxy-file", type=str, default="webshare_proxies_1000.txt")
    parser.add_argument(
        "--case-type-cache-file",
        type=str,
        default="_tmp/case_type_lookup_cache.json",
        help="Persistent cache file for court-specific case-type lookup tables",
    )
    parser.add_argument("--case-limit", type=int, default=40000)
    parser.add_argument("--workers", type=int, default=144)
    parser.add_argument("--time-limit-sec", type=int, default=0, help="0 means no time cap")
    parser.add_argument("--progress-every", type=int, default=200)
    parser.add_argument(
        "--view-history-attempts",
        type=int,
        default=2,
        help="Retries for home/viewHistory per case when include-view-history is enabled",
    )
    parser.add_argument(
        "--ajax-transport-retries",
        type=int,
        default=3,
        help="Transport retry attempts inside DistrictCaseStatusScraper._ajax_post",
    )
    parser.add_argument(
        "--ajax-retry-wait-first",
        type=float,
        default=0.75,
        help="Sleep (sec) after first transport error in _ajax_post",
    )
    parser.add_argument(
        "--ajax-retry-wait-second",
        type=float,
        default=1.5,
        help="Sleep (sec) after second+ transport error in _ajax_post",
    )
    parser.add_argument(
        "--shuffle-input",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Shuffle discovered input JSON files before applying case-limit",
    )
    parser.add_argument("--shuffle-seed", type=int, default=42)
    parser.add_argument(
        "--prewarm-case-types",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Preload case-type lookup cache for all unique court keys before timed processing",
    )
    parser.add_argument(
        "--include-view-history",
        action="store_true",
        help="Fetch and parse home/viewHistory structured data for each case-number hit",
    )
    args = parser.parse_args()

    input_root = Path(args.input_root).resolve()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    dcourt_lookup = _load_dcourt_lookup(Path(args.dcourt_map).resolve())
    proxy_rotator = SharedProxyRotator(str(Path(args.proxy_file).resolve()))

    all_files = sorted(input_root.rglob("*.json"))
    if bool(args.shuffle_input):
        rng = random.Random(int(args.shuffle_seed))
        rng.shuffle(all_files)
    if args.case_limit > 0:
        all_files = all_files[: args.case_limit]

    tasks: List[Dict[str, object]] = []
    for path in all_files:
        parsed = _read_case_ref_task(path, input_root)
        if parsed:
            tasks.append(parsed)

    if not tasks:
        raise SystemExit("No valid case-ref JSON tasks found.")

    lookup_cache: Dict[Tuple[str, str, str], List[Dict[str, str]]] = {}
    lookup_inflight: Dict[Tuple[str, str, str], concurrent.futures.Future] = {}
    lookup_lock = threading.Lock()
    lookup_fetch_count = 0
    case_type_cache_loaded_entries = 0
    stats_lock = threading.Lock()
    thread_local = threading.local()

    case_type_cache_path = Path(args.case_type_cache_file).resolve()
    if case_type_cache_path.exists():
        try:
            cached_raw = json.loads(case_type_cache_path.read_text(encoding="utf-8"))
            if isinstance(cached_raw, dict):
                for raw_key, raw_lookup in cached_raw.items():
                    key = _lookup_key_from_str(str(raw_key))
                    if not key or not isinstance(raw_lookup, list):
                        continue
                    lookup_cache[key] = raw_lookup
                case_type_cache_loaded_entries = len(lookup_cache)
        except Exception:
            case_type_cache_loaded_entries = 0

    stats = {
        "processed": 0,
        "success": 0,
        "failed": 0,
        "orders_total": 0,
        "dcourt_urls_total": 0,
        "view_history_success": 0,
        "view_history_failure": 0,
        "lookup_elapsed_total": 0.0,
        "search_elapsed_total": 0.0,
        "view_history_fetch_elapsed_total": 0.0,
        "view_history_parse_elapsed_total": 0.0,
        "write_elapsed_total": 0.0,
        "task_elapsed_total": 0.0,
    }

    prewarm_elapsed = 0.0
    stop_at = None
    started = 0.0

    def get_scraper() -> DistrictCaseStatusScraper:
        scraper = getattr(thread_local, "scraper", None)
        if scraper is None:
            entry_mode = "casestatus" if bool(args.include_view_history) else "courtorder"
            scraper = DistrictCaseStatusScraper(
                captcha_api_key="",
                use_local_model=True,
                allow_2captcha=False,
                proxy_getter=proxy_rotator.next_proxy,
                entry_mode=entry_mode,
                verbose=False,
                ajax_transport_retries=max(1, int(args.ajax_transport_retries)),
                ajax_retry_wait_first=max(0.0, float(args.ajax_retry_wait_first)),
                ajax_retry_wait_second=max(0.0, float(args.ajax_retry_wait_second)),
            )
            initial = proxy_rotator.next_proxy()
            if initial:
                scraper.set_proxy(initial)
            thread_local.scraper = scraper
        return scraper

    def get_case_type_lookup(
        scraper: DistrictCaseStatusScraper,
        state_code: str,
        dist_code: str,
        court_complex: str,
    ) -> List[Dict[str, str]]:
        key = (state_code, dist_code, court_complex)
        fetch_owner = False
        future: Optional[concurrent.futures.Future] = None
        with lookup_lock:
            existing = lookup_cache.get(key)
            if existing is not None:
                return existing
            future = lookup_inflight.get(key)
            if future is None:
                future = concurrent.futures.Future()
                lookup_inflight[key] = future
                fetch_owner = True

        if fetch_owner:
            try:
                nonlocal lookup_fetch_count
                case_types = scraper.list_case_types(
                    state_code, dist_code, court_complex, search_type="c_no"
                )
                built = _build_case_type_lookup(case_types)
                with lookup_lock:
                    lookup_cache[key] = built
                    lookup_fetch_count += 1
                    lookup_inflight.pop(key, None)
                future.set_result(built)
                return built
            except Exception as exc:
                with lookup_lock:
                    lookup_inflight.pop(key, None)
                future.set_exception(exc)
                raise

        if future is None:
            raise RuntimeError("lookup_inflight_future_missing")
        result = future.result()
        if not isinstance(result, list):
            raise RuntimeError("lookup_inflight_invalid_result")
        return result

    if bool(args.prewarm_case_types):
        prewarm_started = time.perf_counter()
        unique_lookup_keys = sorted(
            {
                (
                    str(task["state_code"]),
                    str(task["dist_code"]),
                    str(task["court_complex"]),
                )
                for task in tasks
            }
        )

        prewarm_local = threading.local()

        def get_prewarm_scraper() -> DistrictCaseStatusScraper:
            scraper = getattr(prewarm_local, "scraper", None)
            if scraper is None:
                entry_mode = "casestatus" if bool(args.include_view_history) else "courtorder"
                scraper = DistrictCaseStatusScraper(
                    captcha_api_key="",
                    use_local_model=True,
                    allow_2captcha=False,
                    proxy_getter=proxy_rotator.next_proxy,
                    entry_mode=entry_mode,
                    verbose=False,
                    ajax_transport_retries=max(1, int(args.ajax_transport_retries)),
                    ajax_retry_wait_first=max(0.0, float(args.ajax_retry_wait_first)),
                    ajax_retry_wait_second=max(0.0, float(args.ajax_retry_wait_second)),
                )
                initial = proxy_rotator.next_proxy()
                if initial:
                    scraper.set_proxy(initial)
                prewarm_local.scraper = scraper
            return scraper

        def prewarm_one(key: Tuple[str, str, str]) -> bool:
            state_code, dist_code, court_complex = key
            scraper = get_prewarm_scraper()
            get_case_type_lookup(scraper, state_code, dist_code, court_complex)
            return True

        prewarm_workers = max(1, min(int(args.workers), 32))
        prewarm_failures = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=prewarm_workers) as prewarm_pool:
            future_to_key = {
                prewarm_pool.submit(prewarm_one, key): key for key in unique_lookup_keys
            }
            for future in concurrent.futures.as_completed(future_to_key):
                try:
                    future.result()
                except Exception:
                    prewarm_failures += 1
        prewarm_elapsed = time.perf_counter() - prewarm_started
        print(
            f"[*] case-type prewarm done: keys={len(unique_lookup_keys)} "
            f"failures={prewarm_failures} elapsed={prewarm_elapsed:.2f}s"
        )

    stop_at = time.perf_counter() + int(args.time_limit_sec) if int(args.time_limit_sec) > 0 else None
    started = time.perf_counter()

    def process_task(task: Dict[str, object]) -> Dict[str, object]:
        task_started = time.perf_counter()
        if stop_at and time.perf_counter() > stop_at:
            return {"skipped": True, "reason": "time_limit"}

        state_code = str(task["state_code"])
        dist_code = str(task["dist_code"])
        court_complex = str(task["court_complex"])
        case_type_text = str(task["case_type_text"])
        case_no = str(task["case_no"])
        year = str(task["year"])

        scraper = get_scraper()
        try:
            lookup_started = time.perf_counter()
            lookup = get_case_type_lookup(scraper, state_code, dist_code, court_complex)
            lookup_elapsed = time.perf_counter() - lookup_started
            case_type_code = _match_case_type_code(case_type_text, lookup)
            if not case_type_code:
                return {
                    "ok": False,
                    "error": "case_type_code_not_found",
                    "lookup_elapsed": lookup_elapsed,
                    "task_elapsed": time.perf_counter() - task_started,
                }

            search_started = time.perf_counter()
            if args.include_view_history:
                # For full structured data, use case-status case-number search,
                # which exposes viewHistory(...) params required for home/viewHistory.
                search = scraper.search_by_case_number(
                    case_type=case_type_code,
                    case_no=case_no,
                    year=year,
                    state_code=state_code,
                    dist_code=dist_code,
                    court_complex_code=court_complex,
                    retries=1,
                )
                search["benchmark_case_lookup_attempt"] = 1
            else:
                search = scraper.search_courtorder_by_case_number(
                    case_type=case_type_code,
                    case_no=case_no,
                    year=year,
                    state_code=state_code,
                    dist_code=dist_code,
                    court_complex_code=court_complex,
                    order_type="both",
                    retries=2,
                )
            search_elapsed = time.perf_counter() - search_started
        except Exception as exc:
            return {
                "ok": False,
                "error": f"lookup_exception:{exc}",
                "task_elapsed": time.perf_counter() - task_started,
            }

        rows = list(search.get("cases") or [])
        jurisdiction = task.get("jurisdiction") or {}
        state_name = str(jurisdiction.get("state_name") or "")
        district_name = str(jurisdiction.get("district_name") or "")
        dcourt_base = dcourt_lookup.get((_normalize_text(state_name), _normalize_text(district_name)))

        order_rows: List[Dict[str, object]] = []
        dcourt_url_count = 0
        selected_view_history_params: Optional[Dict[str, str]] = None
        for idx, row in enumerate(rows, start=1):
            order_no = str(row.get("col_0") or row.get("sr_no") or idx)
            case_ref_text = str(
                row.get("col_1")
                or row.get("case_type_case_number_case_year")
                or ""
            )
            party_caption = str(
                row.get("col_2")
                or row.get("petitioner_name_versus_respondent_name")
                or ""
            )
            petitioner, respondent = _split_party_caption(party_caption)
            order_date = str(row.get("order_date") or row.get("col_3") or "")
            onclick = str(row.get("col_4_onclick") or "")
            display_pdf = _parse_display_pdf_onclick(onclick)

            cino = None
            vp = _find_view_history_params_from_row(row)
            if vp and vp.get("cino"):
                cino = str(vp["cino"])
                if selected_view_history_params is None:
                    selected_view_history_params = vp

            dcourt_pdf_url = None
            order_date_iso = _to_iso_date(order_date)
            if dcourt_base and cino and order_date_iso and order_no:
                dcourt_pdf_url = _build_dcourt_pdf_url(
                    dcourt_base=dcourt_base,
                    cino=cino,
                    order_no=order_no,
                    order_date_iso=order_date_iso,
                )
                dcourt_url_count += 1

            order_rows.append(
                {
                    "row_index": idx,
                    "serial_no": order_no,
                    "case_type_case_number_case_year": case_ref_text,
                    "party_caption": party_caption,
                    "petitioner": petitioner,
                    "respondent": respondent,
                    "order_no": order_no,
                    "order_date": order_date,
                    "order_label": str(row.get("orders") or row.get("col_4") or ""),
                    "ecourt_display_pdf_params": display_pdf,
                    "dcourt_pdf_url": dcourt_pdf_url,
                    "cino": cino,
                    "raw_row": {
                        "col_0": str(row.get("col_0") or ""),
                        "col_1": str(row.get("col_1") or ""),
                        "col_2": str(row.get("col_2") or ""),
                        "col_3": str(row.get("col_3") or ""),
                        "col_4": str(row.get("col_4") or ""),
                    },
                }
            )

        view_history_payload: Dict[str, object] = {
            "enabled": bool(args.include_view_history),
            "ok": False,
            "attempts": 0,
            "error": None,
            "raw_html_path": None,
            "parsed": None,
        }
        vh_fetch_elapsed = 0.0
        vh_parse_elapsed = 0.0
        if args.include_view_history:
            if not selected_view_history_params:
                view_history_payload["error"] = "view_history_params_not_found"
            else:
                try:
                    vh_fetch_started = time.perf_counter()
                    view, html, vh_attempts = _fetch_view_history_with_retries(
                        scraper=scraper,
                        params=selected_view_history_params,
                        attempts=max(1, int(args.view_history_attempts)),
                    )
                    vh_fetch_elapsed = time.perf_counter() - vh_fetch_started
                    view_history_payload["attempts"] = vh_attempts
                    if html:
                        rel_path = Path(str(task["rel_path"]))
                        html_path = (output_root / "_viewhistory_html" / rel_path).with_suffix(".html").resolve()
                        html_path.parent.mkdir(parents=True, exist_ok=True)
                        html_path.write_text(html, encoding="utf-8")
                        vh_parse_started = time.perf_counter()
                        parsed = parse_case_detail_html(html)
                        vh_parse_elapsed = time.perf_counter() - vh_parse_started
                        parsed["cnr"] = str(selected_view_history_params.get("cino") or "")
                        parsed["raw_html_path"] = str(html_path)
                        view_history_payload["ok"] = True
                        view_history_payload["raw_html_path"] = str(html_path)
                        view_history_payload["parsed"] = parsed
                    else:
                        vh_parse_elapsed = 0.0
                        view_history_payload["error"] = str(
                            view.get("errormsg") or "empty_view_history_html"
                        )
                except Exception as exc:
                    view_history_payload["error"] = f"view_history_exception:{exc}"

        rel_path = Path(str(task["rel_path"]))
        output_path = (output_root / rel_path).resolve()
        write_started = time.perf_counter()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_payload = {
            "input_case_ref_path": str(task["input_path"]),
            "case_reference": task["case_reference"],
            "jurisdiction": task["jurisdiction"],
            "query": task["query"],
            "case_context": {
                "state_name": str(jurisdiction.get("state_name") or ""),
                "district_name": str(jurisdiction.get("district_name") or ""),
                "court_complex_name": str(
                    (task.get("query") or {}).get("court_complex_name")
                    or jurisdiction.get("court_complex_name")
                    or ""
                ),
                "state_code": state_code,
                "district_code": dist_code,
                "court_complex_code": court_complex,
            },
            "case_number_search": {
                "status": search.get("status"),
                "errormsg": search.get("errormsg"),
                "returned_row_count": len(rows),
                "lookup_attempt": search.get("benchmark_case_lookup_attempt"),
            },
            "order_urls": order_rows,
            "view_history": view_history_payload,
            "metrics": {
                "orders_found": len(rows),
                "dcourt_urls_reconstructed": dcourt_url_count,
            },
        }
        output_path.write_text(json.dumps(output_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        write_elapsed = time.perf_counter() - write_started

        return {
            "ok": True,
            "orders_found": len(rows),
            "dcourt_urls": dcourt_url_count,
            "view_history_ok": bool(view_history_payload.get("ok")),
            "lookup_elapsed": lookup_elapsed,
            "search_elapsed": search_elapsed,
            "view_history_fetch_elapsed": vh_fetch_elapsed,
            "view_history_parse_elapsed": vh_parse_elapsed,
            "write_elapsed": write_elapsed,
            "task_elapsed": time.perf_counter() - task_started,
        }

    total_tasks = len(tasks)
    submitted = 0
    futures: Dict[concurrent.futures.Future, int] = {}
    max_inflight = max(int(args.workers) * 4, int(args.workers))

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        while submitted < total_tasks and len(futures) < max_inflight:
            futures[executor.submit(process_task, tasks[submitted])] = submitted
            submitted += 1

        while futures:
            done, _ = concurrent.futures.wait(
                futures.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                futures.pop(future, None)
                try:
                    result = future.result()
                except Exception as exc:
                    result = {"ok": False, "error": f"worker_exception:{exc}"}
                if result.get("skipped"):
                    continue
                with stats_lock:
                    stats["processed"] += 1
                    if result.get("ok"):
                        stats["success"] += 1
                        stats["orders_total"] += int(result.get("orders_found") or 0)
                        stats["dcourt_urls_total"] += int(result.get("dcourt_urls") or 0)
                        stats["lookup_elapsed_total"] += float(result.get("lookup_elapsed") or 0.0)
                        stats["search_elapsed_total"] += float(result.get("search_elapsed") or 0.0)
                        stats["view_history_fetch_elapsed_total"] += float(
                            result.get("view_history_fetch_elapsed") or 0.0
                        )
                        stats["view_history_parse_elapsed_total"] += float(
                            result.get("view_history_parse_elapsed") or 0.0
                        )
                        stats["write_elapsed_total"] += float(result.get("write_elapsed") or 0.0)
                        if args.include_view_history:
                            if bool(result.get("view_history_ok")):
                                stats["view_history_success"] += 1
                            else:
                                stats["view_history_failure"] += 1
                    else:
                        stats["failed"] += 1
                    stats["task_elapsed_total"] += float(result.get("task_elapsed") or 0.0)
                    processed = stats["processed"]
                if processed % max(1, int(args.progress_every)) == 0:
                    elapsed = time.perf_counter() - started
                    rate = processed / elapsed if elapsed > 0 else 0.0
                    print(
                        f"[*] progress {processed}/{total_tasks} | "
                        f"rate={rate:.2f} cases/s | success={stats['success']} fail={stats['failed']}"
                    )

            if stop_at and time.perf_counter() > stop_at:
                break

            while submitted < total_tasks and len(futures) < max_inflight:
                futures[executor.submit(process_task, tasks[submitted])] = submitted
                submitted += 1

    case_type_cache_saved_entries = 0
    try:
        case_type_cache_path.parent.mkdir(parents=True, exist_ok=True)
        serializable_cache = {
            _lookup_key_to_str(key): value for key, value in lookup_cache.items()
        }
        case_type_cache_path.write_text(
            json.dumps(serializable_cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        case_type_cache_saved_entries = len(serializable_cache)
    except Exception:
        case_type_cache_saved_entries = 0

    elapsed_total = time.perf_counter() - started
    processed = int(stats["processed"])
    rate = processed / elapsed_total if elapsed_total > 0 else 0.0
    projected_40k_sec = (40000.0 / rate) if rate > 0 else None

    summary = {
        "input_root": str(input_root),
        "output_root": str(output_root),
        "dcourt_map": str(Path(args.dcourt_map).resolve()),
        "proxy_file": str(Path(args.proxy_file).resolve()),
        "case_type_cache_file": str(case_type_cache_path),
        "case_type_cache_loaded_entries": int(case_type_cache_loaded_entries),
        "case_type_cache_saved_entries": int(case_type_cache_saved_entries),
        "workers": int(args.workers),
        "view_history_attempts": max(1, int(args.view_history_attempts)),
        "ajax_transport_retries": max(1, int(args.ajax_transport_retries)),
        "ajax_retry_wait_first": max(0.0, float(args.ajax_retry_wait_first)),
        "ajax_retry_wait_second": max(0.0, float(args.ajax_retry_wait_second)),
        "shuffle_input": bool(args.shuffle_input),
        "shuffle_seed": int(args.shuffle_seed),
        "prewarm_case_types": bool(args.prewarm_case_types),
        "prewarm_elapsed_sec": round(prewarm_elapsed, 3),
        "requested_case_limit": int(args.case_limit),
        "time_limit_sec": int(args.time_limit_sec),
        "tasks_loaded": total_tasks,
        "processed": processed,
        "success": int(stats["success"]),
        "failed": int(stats["failed"]),
        "orders_total": int(stats["orders_total"]),
        "dcourt_urls_total": int(stats["dcourt_urls_total"]),
        "include_view_history": bool(args.include_view_history),
        "case_type_lookup_network_fetches": int(lookup_fetch_count),
        "case_type_lookup_cache_keys": int(len(lookup_cache)),
        "view_history_success": int(stats["view_history_success"]),
        "view_history_failure": int(stats["view_history_failure"]),
        "timing_breakdown_sec": {
            "lookup_total": round(float(stats["lookup_elapsed_total"]), 3),
            "search_total": round(float(stats["search_elapsed_total"]), 3),
            "view_history_fetch_total": round(float(stats["view_history_fetch_elapsed_total"]), 3),
            "view_history_parse_total": round(float(stats["view_history_parse_elapsed_total"]), 3),
            "write_total": round(float(stats["write_elapsed_total"]), 3),
            "task_total": round(float(stats["task_elapsed_total"]), 3),
        },
        "timing_breakdown_ms_per_processed_case": {
            "lookup_ms": round((float(stats["lookup_elapsed_total"]) * 1000.0 / processed), 3) if processed else 0.0,
            "search_ms": round((float(stats["search_elapsed_total"]) * 1000.0 / processed), 3) if processed else 0.0,
            "view_history_fetch_ms": round((float(stats["view_history_fetch_elapsed_total"]) * 1000.0 / processed), 3) if processed else 0.0,
            "view_history_parse_ms": round((float(stats["view_history_parse_elapsed_total"]) * 1000.0 / processed), 3) if processed else 0.0,
            "write_ms": round((float(stats["write_elapsed_total"]) * 1000.0 / processed), 3) if processed else 0.0,
            "task_ms": round((float(stats["task_elapsed_total"]) * 1000.0 / processed), 3) if processed else 0.0,
        },
        "elapsed_sec": round(elapsed_total, 3),
        "cases_per_sec": round(rate, 3),
        "projected_40k_sec": round(projected_40k_sec, 3) if projected_40k_sec is not None else None,
        "projected_40k_min": round(projected_40k_sec / 60.0, 3) if projected_40k_sec is not None else None,
    }

    summary_path = output_root / "benchmark_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"[+] Summary saved: {summary_path}")


if __name__ == "__main__":
    main()
