import argparse
import concurrent.futures
import json
import re
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from district_casestatus_scraper import (
    BASE_URL,
    ENTRY_URL,
    DistrictCaseStatusScraper,
)


VIEW_HISTORY_RE = re.compile(
    r"viewHistory\((\d+),'([^']*)',(\d+),'([^']*)','([^']*)',(\d+),(\d+),(\d+),'([^']*)'\)"
)
DISPLAY_PDF_RE = re.compile(
    r"displayPdf\('([^']*)','([^']*)','([^']*)','([^']*)','([^']*)'\)"
)
ENUM_RE = re.compile(r"(?<!\()(?<!\d)(\d{1,3})\)\s*")
_thread_local = threading.local()
_shared_proxy_pool: List[Dict[str, str]] = []
_shared_proxy_idx = 0
_shared_proxy_lock = threading.Lock()


def _get_next_shared_proxy() -> Optional[Dict[str, str]]:
    global _shared_proxy_idx
    if not _shared_proxy_pool:
        return None
    with _shared_proxy_lock:
        proxy = _shared_proxy_pool[_shared_proxy_idx]
        _shared_proxy_idx = (_shared_proxy_idx + 1) % len(_shared_proxy_pool)
    return proxy


def _load_shared_proxies(proxy_file: str) -> int:
    _shared_proxy_pool.clear()
    with open(proxy_file, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split(":")
            if len(parts) != 4:
                continue
            ip, port, user, password = parts
            proxy_url = f"http://{user}:{password}@{ip}:{port}"
            _shared_proxy_pool.append({"http": proxy_url, "https": proxy_url})
    global _shared_proxy_idx
    _shared_proxy_idx = 0
    return len(_shared_proxy_pool)


def _probe_proxy_latency(proxy: Dict[str, str], timeout_sec: float) -> Optional[float]:
    started = time.perf_counter()
    session = requests.Session()
    session.trust_env = False
    session.proxies.update(proxy)
    try:
        response = session.get(ENTRY_URL, timeout=timeout_sec)
        if response.status_code != 200:
            return None
        if "app_token" not in response.text:
            return None
        return time.perf_counter() - started
    except requests.RequestException:
        return None
    finally:
        session.close()


def _rank_shared_proxies(
    *,
    top_k: int,
    workers: int,
    timeout_sec: float,
) -> Dict[str, object]:
    before = len(_shared_proxy_pool)
    if before == 0:
        return {
            "loaded_count": 0,
            "ranked": False,
            "healthy_count": 0,
            "selected_count": 0,
            "probe_elapsed_sec": 0.0,
        }

    started = time.perf_counter()
    scored: List[Tuple[float, Dict[str, str]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(_probe_proxy_latency, proxy, max(1.0, float(timeout_sec))): proxy
            for proxy in _shared_proxy_pool
        }
        for future in concurrent.futures.as_completed(futures):
            latency = future.result()
            if latency is not None:
                proxy = futures[future]
                scored.append((latency, proxy))

    scored.sort(key=lambda entry: entry[0])
    healthy_count = len(scored)
    if top_k > 0:
        scored = scored[:top_k]
    selected = [entry[1] for entry in scored]

    if selected:
        _shared_proxy_pool.clear()
        _shared_proxy_pool.extend(selected)
        global _shared_proxy_idx
        _shared_proxy_idx = 0

    latencies = [entry[0] for entry in scored]
    return {
        "loaded_count": before,
        "ranked": True,
        "healthy_count": healthy_count,
        "selected_count": len(selected),
        "probe_elapsed_sec": round(time.perf_counter() - started, 3),
        "selected_latency_min_sec": round(min(latencies), 3) if latencies else None,
        "selected_latency_median_sec": round(latencies[len(latencies) // 2], 3) if latencies else None,
        "selected_latency_max_sec": round(max(latencies), 3) if latencies else None,
    }


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").strip().lower()).strip("_")


def _sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^\w\-.]+", "_", (name or "").strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "item"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, records: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _safe_label(name: str, fallback: str) -> str:
    cleaned = _sanitize_filename(name)
    return cleaned or _sanitize_filename(fallback) or "unknown"


def _split_enum_segments(text: str) -> List[str]:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if not cleaned:
        return []
    matches = list(ENUM_RE.finditer(cleaned))
    segments: List[str] = []
    for idx, match in enumerate(matches):
        if idx == 0 and match.start() > 0:
            prefix = cleaned[:match.start()].strip()
            if prefix:
                segments.append(prefix)
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(cleaned)
        segment = cleaned[start:end].strip()
        if segment:
            segments.append(segment)
    if not segments:
        segments.append(cleaned)
    return segments


def _parse_key_value_table(table) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for row in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
        if not cells:
            continue
        i = 0
        while i + 1 < len(cells):
            key = cells[i].rstrip(":").strip()
            val = cells[i + 1].strip()
            if key:
                data[key] = val
            i += 2
    return data


def _parse_grid_table(table) -> List[Dict[str, str]]:
    rows = table.find_all("tr")
    if not rows:
        return []
    headers = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
    headers = [header for header in headers if header]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
        if not cells or all(not cell for cell in cells):
            continue
        entry: Dict[str, str] = {}
        for idx, cell in enumerate(cells):
            if idx < len(headers):
                entry[headers[idx]] = cell
        if entry:
            data.append(entry)
    if data or not headers:
        return data

    # Fallback for malformed tables where data <td> cells are not wrapped in proper <tr> rows.
    flat_cells = [c.get_text(" ", strip=True) for c in table.find_all("td")]
    flat_cells = [cell for cell in flat_cells if cell]
    if not flat_cells:
        return data

    header_count = len(headers)
    for offset in range(0, len(flat_cells), header_count):
        chunk = flat_cells[offset : offset + header_count]
        if len(chunk) < header_count:
            break
        data.append({headers[idx]: chunk[idx] for idx in range(header_count)})
    return data


def _parse_fir_details_table(table) -> Dict[str, str]:
    details: Dict[str, str] = {}
    for row in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
        cells = [cell for cell in cells if cell]
        if len(cells) < 2:
            continue
        key = cells[0].rstrip(":").strip()
        value = cells[1].strip()
        if key.lower() in {"field", "details"}:
            continue
        if key:
            details[key] = value
    return details


def _parse_history_table(table) -> List[Dict[str, str]]:
    history: List[Dict[str, str]] = []
    headers: List[str] = []
    for row in table.find_all("tr"):
        ths = row.find_all("th")
        if ths:
            headers = [th.get_text(" ", strip=True) for th in ths]
            continue
        cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
        if not cells:
            continue
        if headers:
            entry = {headers[i]: cells[i] for i in range(min(len(headers), len(cells)))}
        else:
            padded = cells + [""] * (4 - len(cells))
            entry = {
                "court": padded[0],
                "business_date": padded[1],
                "next_date": padded[2],
                "purpose": padded[3],
            }
        history.append(entry)
    return history


def _parse_party_table(table) -> List[Dict[str, Optional[str]]]:
    parties: List[Dict[str, Optional[str]]] = []
    rows = []
    if getattr(table, "name", None) in {"ul", "ol"}:
        rows = table.find_all("li", recursive=False) or table.find_all("li")
    else:
        rows = table.find_all("tr")

    for row in rows:
        cell = row
        if getattr(table, "name", None) not in {"ul", "ol"}:
            cell = row.find("td") or row.find("th")
        if not cell:
            continue
        text = cell.get_text(" ", strip=True)
        if not text:
            continue
        for segment in _split_enum_segments(text):
            segment = re.sub(r"^\s*\d+\)\s*", "", segment)
            parts = re.split(r"Advocate\s*-", segment, maxsplit=1, flags=re.IGNORECASE)
            name = parts[0].strip()
            advocate = parts[1].strip() if len(parts) > 1 else None
            if name:
                parties.append({"name": name, "advocate": advocate})
            elif advocate and parties:
                parties[-1]["advocate"] = advocate
    return parties


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


def _parse_order_table(table) -> List[Dict[str, object]]:
    orders: List[Dict[str, object]] = []
    for row in table.find_all("tr"):
        if row.find_all("th"):
            continue
        cells = row.find_all("td")
        if not cells:
            continue
        first_cell = cells[0].get_text(" ", strip=True)
        if first_cell.lower().startswith("order number") or first_cell.lower().startswith("order no"):
            continue
        order_number = first_cell.strip()
        order_on = ""
        judge = ""
        order_date = ""
        details_cell = None
        if len(cells) >= 5:
            order_on = cells[1].get_text(" ", strip=True)
            judge = cells[2].get_text(" ", strip=True)
            order_date = cells[3].get_text(" ", strip=True)
            details_cell = cells[4]
        else:
            order_date = cells[1].get_text(" ", strip=True) if len(cells) > 1 else ""
            details_cell = cells[2] if len(cells) > 2 else None
        details = details_cell.get_text(" ", strip=True) if details_cell else ""
        pdf_params = None
        pdf_href = None
        if details_cell:
            for link in details_cell.find_all("a"):
                pdf_params = _parse_display_pdf_onclick(link.get("onclick", ""))
                if pdf_params:
                    break
                href = link.get("href")
                if href and "display_pdf.php" in href:
                    pdf_href = href
        orders.append(
            {
                "order_number": order_number,
                "order_on": order_on,
                "judge": judge,
                "order_date": order_date,
                "details": details,
                "pdf_params": pdf_params,
                "pdf_href": pdf_href,
            }
        )
    return orders


def _parse_view_history_onclick(onclick: str) -> Optional[Dict[str, str]]:
    match = VIEW_HISTORY_RE.search(onclick or "")
    if not match:
        return None
    case_no, cino, court_code, hideparty, search_flag, state_code, dist_code, complex_code, search_by = match.groups()
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


def _build_pdf_ref_signature(
    pdf_params: Optional[Dict[str, str]] = None,
    pdf_href: Optional[str] = None,
) -> Optional[str]:
    if pdf_params:
        return "params:" + "|".join(
            [
                pdf_params.get("normal_v", ""),
                pdf_params.get("case_val", ""),
                pdf_params.get("court_code", ""),
                pdf_params.get("filename", ""),
                pdf_params.get("app_flag", ""),
            ]
        )
    if pdf_href:
        return f"href:{pdf_href.strip()}"
    return None


def _extract_raw_pdf_refs(html: str) -> List[Dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    refs: List[Dict[str, object]] = []
    for link in soup.find_all("a"):
        onclick = link.get("onclick", "")
        href = (link.get("href") or "").strip()
        pdf_params = _parse_display_pdf_onclick(onclick)
        pdf_href = href if "display_pdf.php" in href else None
        signature = _build_pdf_ref_signature(pdf_params, pdf_href)
        if not signature:
            continue
        section_heading = link.find_previous(["h3", "h4"])
        refs.append(
            {
                "signature": signature,
                "section_title": section_heading.get_text(" ", strip=True) if section_heading else None,
                "link_text": link.get_text(" ", strip=True),
                "pdf_params": pdf_params,
                "pdf_href": pdf_href,
            }
        )
    return refs


def _extract_parsed_pdf_refs(case_detail: Dict[str, object]) -> List[Dict[str, object]]:
    refs: List[Dict[str, object]] = []
    for group in case_detail.get("order_groups", []) or []:
        items = group.get("items", []) if isinstance(group, dict) else []
        group_type = group.get("type", "orders") if isinstance(group, dict) else "orders"
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue
            pdf_params = item.get("pdf_params")
            pdf_href = item.get("pdf_href")
            signature = _build_pdf_ref_signature(pdf_params, pdf_href)
            if not signature:
                continue
            refs.append(
                {
                    "signature": signature,
                    "group_type": group_type,
                    "order_index": idx,
                    "order_number": item.get("order_number"),
                    "order_date": item.get("order_date"),
                    "details": item.get("details"),
                    "pdf_params": pdf_params,
                    "pdf_href": pdf_href,
                }
            )
    return refs


def _build_pdf_discovery_audit(html: str, case_detail: Dict[str, object]) -> Dict[str, object]:
    raw_refs = _extract_raw_pdf_refs(html)
    parsed_refs = _extract_parsed_pdf_refs(case_detail)

    raw_counts = Counter(str(ref["signature"]) for ref in raw_refs)
    parsed_counts = Counter(str(ref["signature"]) for ref in parsed_refs)

    missing_counts = raw_counts - parsed_counts
    extra_counts = parsed_counts - raw_counts

    missing_refs: List[Dict[str, object]] = []
    raw_used: Counter[str] = Counter()
    for ref in raw_refs:
        signature = str(ref["signature"])
        if raw_used[signature] >= missing_counts[signature]:
            continue
        raw_used[signature] += 1
        missing_refs.append({k: v for k, v in ref.items() if k != "signature"})

    extra_refs: List[Dict[str, object]] = []
    parsed_used: Counter[str] = Counter()
    for ref in parsed_refs:
        signature = str(ref["signature"])
        if parsed_used[signature] >= extra_counts[signature]:
            continue
        parsed_used[signature] += 1
        extra_refs.append({k: v for k, v in ref.items() if k != "signature"})

    return {
        "raw_pdf_ref_count": len(raw_refs),
        "parsed_pdf_ref_count": len(parsed_refs),
        "missing_pdf_ref_count": sum(missing_counts.values()),
        "extra_parsed_pdf_ref_count": sum(extra_counts.values()),
        "parser_miss_detected": bool(missing_refs),
        "missing_pdf_refs": missing_refs,
        "extra_parsed_pdf_refs": extra_refs,
        "raw_pdf_refs": [{k: v for k, v in ref.items() if k != "signature"} for ref in raw_refs],
        "parsed_pdf_refs": [{k: v for k, v in ref.items() if k != "signature"} for ref in parsed_refs],
    }


def _collect_section_nodes(heading) -> List[object]:
    nodes = []
    for tag in heading.find_all_next(["h2", "h3", "h4", "table", "ul", "ol"]):
        if tag is heading:
            continue
        if tag.name in {"h2", "h3", "h4"}:
            break
        nodes.append(tag)
    return nodes


def parse_case_detail_html(html: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    result: Dict[str, object] = {
        "court_name": "",
        "case_details": {},
        "case_status": {},
        "petitioners": [],
        "respondents": [],
        "acts": [],
        "processes": [],
        "fir_details": {},
        "subordinate_court_information": [],
        "case_transfer_details": [],
        "case_history": [],
        "order_groups": [],
        "sections_present": [],
    }
    heading = soup.find("h2")
    if heading:
        result["court_name"] = heading.get_text(" ", strip=True)

    for section_heading in soup.find_all(["h3", "h4"]):
        title = section_heading.get_text(" ", strip=True)
        lowered = title.lower()
        section_nodes = _collect_section_nodes(section_heading)
        if not section_nodes:
            continue
        tables = [node for node in section_nodes if getattr(node, "name", None) == "table"]
        cast_sections = result["sections_present"]
        if isinstance(cast_sections, list):
            cast_sections.append(title)
        if "case details" in lowered:
            if tables:
                result["case_details"] = _parse_key_value_table(tables[0])
        elif "case status" in lowered:
            if tables:
                result["case_status"] = _parse_key_value_table(tables[0])
        elif "petitioner" in lowered:
            result["petitioners"] = _parse_party_table(section_nodes[0])
        elif "respondent" in lowered:
            result["respondents"] = _parse_party_table(section_nodes[0])
        elif lowered == "acts":
            if tables:
                result["acts"] = _parse_grid_table(tables[0])
        elif "process" in lowered:
            if tables:
                result["processes"] = _parse_grid_table(tables[0])
        elif "fir details" in lowered:
            if tables:
                result["fir_details"] = _parse_fir_details_table(tables[0])
        elif "subordinate court information" in lowered:
            if tables:
                result["subordinate_court_information"] = _parse_grid_table(tables[0])
        elif "case transfer details" in lowered:
            if tables:
                result["case_transfer_details"] = _parse_grid_table(tables[0])
        elif "case history" in lowered:
            if tables:
                result["case_history"] = _parse_history_table(tables[0])
        elif "order" in lowered or "judgement" in lowered or "judgment" in lowered:
            if tables:
                items = _parse_order_table(tables[0])
                cast_groups = result["order_groups"]
                if isinstance(cast_groups, list):
                    cast_groups.append({"type": title, "items": items})
    return result


def _resolve_jurisdiction_metadata(
    scraper: DistrictCaseStatusScraper,
    state_code: str,
    district_code: str,
    court_complex_code: str,
) -> Dict[str, object]:
    def _fetch_with_retries(label: str, fetcher, attempts: int = 3) -> Dict[str, str]:
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                data = fetcher()
                return data if isinstance(data, dict) else {}
            except Exception as exc:
                last_error = exc
                if attempt < attempts:
                    try:
                        scraper._rotate_proxy_and_reinit()
                    except Exception:
                        pass
                    time.sleep(min(1.0 * attempt, 3.0))
        print(f"[!] Failed to fetch {label} after retries: {last_error}")
        return {}

    states = _fetch_with_retries("states", scraper.list_states)
    districts = _fetch_with_retries("districts", lambda: scraper.list_districts(state_code))
    courts = _fetch_with_retries(
        "court complexes",
        lambda: scraper.list_court_complexes(state_code, district_code),
    )
    complex_code = scraper._extract_complex_code(court_complex_code)

    state_name = states.get(state_code, state_code)
    district_name = districts.get(district_code, district_code)
    court_name = courts.get(court_complex_code) or courts.get(complex_code) or complex_code
    if court_name == complex_code:
        for key, value in courts.items():
            if scraper._extract_complex_code(key) == complex_code:
                court_name = value
                break

    return {
        "state_code": state_code,
        "state_name": state_name,
        "district_code": district_code,
        "district_name": district_name,
        "court_complex_code": complex_code,
        "court_complex_name": court_name,
        "path_parts": {
            "state": _safe_label(state_name, state_code),
            "district": _safe_label(district_name, district_code),
            "court_complex": _safe_label(str(court_name), complex_code),
        },
    }


def _build_output_layout(
    base_output_dir: Path,
    jurisdiction: Dict[str, object],
    run_id: str,
) -> Dict[str, Path]:
    jurisdiction_path = Path(
        str(jurisdiction["path_parts"]["state"]),
        str(jurisdiction["path_parts"]["district"]),
        str(jurisdiction["path_parts"]["court_complex"]),
    )
    base_output_dir = base_output_dir.resolve()
    json_case_dir = base_output_dir / "ecourt_output_json" / jurisdiction_path
    html_case_dir = base_output_dir / "ecourt_output_html" / jurisdiction_path
    pdf_case_dir = base_output_dir / "ecourt_output_pdf" / jurisdiction_path
    log_dir = base_output_dir / "ecourt_output_logs" / jurisdiction_path / f"run_{run_id}"

    for path in [json_case_dir, html_case_dir, pdf_case_dir, log_dir]:
        path.mkdir(parents=True, exist_ok=True)

    return {
        "base_output_dir": base_output_dir,
        "json_case_dir": json_case_dir,
        "html_case_dir": html_case_dir,
        "pdf_case_dir": pdf_case_dir,
        "log_dir": log_dir,
        "case_events_path": log_dir / "case_events.jsonl",
        "case_errors_path": log_dir / "case_errors.json",
        "retry_cases_path": log_dir / "retry_cases.json",
        "empty_cins_path": log_dir / "empty_cins.json",
        "pdf_discovery_audit_path": log_dir / "pdf_discovery_audit.json",
        "run_context_path": log_dir / "run_context.json",
        "summary_path": log_dir / "summary.json",
    }


def _case_summary_from_row(case: Dict[str, object]) -> Dict[str, object]:
    return {
        "case_title": str(case.get("case_type_case_number_case_year") or case.get("col_1") or ""),
        "party_names": str(
            case.get("petitioner_name_versus_respondent_name") or case.get("col_2") or ""
        ),
    }


def _prepare_case_work_items(
    cases: List[Dict[str, object]],
    layout: Dict[str, Path],
    refresh_existing: bool,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    work_items: List[Dict[str, object]] = []
    events: List[Dict[str, object]] = []
    seen_cnrs = set()

    for row_index, case in enumerate(cases, start=1):
        summary = _case_summary_from_row(case)
        onclick = str(case.get("col_3_onclick") or "")
        params = _parse_view_history_onclick(onclick)
        if not params:
            events.append(
                {
                    "timestamp_utc": _now_utc_iso(),
                    "row_index": row_index,
                    "cnr": None,
                    "status": "invalid_onclick",
                    "retry_recommended": False,
                    "error": "Missing or invalid viewHistory onclick",
                    "json_path": None,
                    "pdf_case_dir": None,
                    **summary,
                    "search_row": case,
                }
            )
            continue

        cnr = params["cino"]
        json_path = layout["json_case_dir"] / f"{cnr}.json"
        html_path = layout["html_case_dir"] / f"{cnr}.html"
        pdf_case_dir = layout["pdf_case_dir"] / cnr
        event_base = {
            "timestamp_utc": _now_utc_iso(),
            "row_index": row_index,
            "cnr": cnr,
            "json_path": str(json_path.resolve()),
            "html_path": str(html_path.resolve()),
            "pdf_case_dir": str(pdf_case_dir.resolve()),
            **summary,
        }

        if cnr in seen_cnrs:
            events.append(
                {
                    **event_base,
                    "status": "skipped_duplicate_cnr",
                    "retry_recommended": False,
                    "error": None,
                }
            )
            continue

        seen_cnrs.add(cnr)
        if json_path.exists() and not refresh_existing:
            events.append(
                {
                    **event_base,
                    "status": "skipped_existing_cnr",
                    "retry_recommended": False,
                    "error": None,
                }
            )
            continue

        work_items.append(
            {
                "row_index": row_index,
                "case": case,
                "params": params,
                "cnr": cnr,
                "json_path": json_path,
                "html_path": html_path,
                "pdf_case_dir": pdf_case_dir,
                **summary,
            }
        )

    return work_items, events


def _normalize_pdf_url(pdf_url: str) -> str:
    if not str(pdf_url).startswith("http"):
        return f"https://services.ecourts.gov.in{pdf_url}" if str(pdf_url).startswith("/") else BASE_URL + str(pdf_url)
    return str(pdf_url)


def _build_display_pdf_post(pdf_params: Dict[str, str]) -> str:
    return "&".join(
        [
            f"normal_v={requests.utils.quote(pdf_params.get('normal_v', ''), safe='')}",
            f"case_val={requests.utils.quote(pdf_params.get('case_val', ''), safe='')}",
            f"court_code={requests.utils.quote(pdf_params.get('court_code', ''), safe='')}",
            f"filename={requests.utils.quote(pdf_params.get('filename', ''), safe='')}",
            f"appFlag={requests.utils.quote(pdf_params.get('app_flag', ''), safe='')}",
        ]
    )


def _resolve_pdf_url_for_item(
    scraper: DistrictCaseStatusScraper,
    item: Dict[str, object],
    view_params: Optional[Dict[str, str]] = None,
    retries: int = 3,
    force: bool = False,
) -> Optional[str]:
    existing_pdf_url = item.get("pdf_url")
    if existing_pdf_url and not force:
        return str(existing_pdf_url)

    pdf_href = item.get("pdf_href")
    pdf_params = item.get("pdf_params")
    if not pdf_params:
        if pdf_href:
            normalized = _normalize_pdf_url(str(pdf_href))
            item["pdf_url"] = normalized
            return normalized
        if existing_pdf_url:
            normalized = _normalize_pdf_url(str(existing_pdf_url))
            item["pdf_url"] = normalized
            return normalized
        return None

    post = _build_display_pdf_post(pdf_params)
    last_error: Optional[str] = None
    for attempt in range(1, retries + 1):
        if view_params and view_params.get("state_code") and view_params.get("dist_code"):
            scraper._ensure_session_state(view_params["state_code"], view_params["dist_code"])
        result = scraper._ajax_post("home/display_pdf", post)
        candidate = result.get("order") or result.get("pdf_url") or result.get("url")
        if candidate:
            normalized = _normalize_pdf_url(str(candidate))
            item["pdf_url"] = normalized
            item["pdf_resolution_attempts"] = attempt
            item["pdf_resolution_error"] = None
            return normalized

        last_error = str(result.get("errormsg") or result.get("error") or "display_pdf returned no order")
        if attempt < retries:
            scraper._rotate_proxy_and_reinit()
            if view_params and view_params.get("state_code") and view_params.get("dist_code"):
                scraper._ensure_session_state(view_params["state_code"], view_params["dist_code"])
            time.sleep(min(1.0 * attempt, 3.0))

    item["pdf_resolution_attempts"] = retries
    item["pdf_resolution_error"] = last_error or "display_pdf returned no order"
    return None


def _resolve_case_pdf_urls(
    scraper: DistrictCaseStatusScraper,
    case_detail: Dict[str, object],
    view_params: Optional[Dict[str, str]] = None,
) -> None:
    for group in case_detail.get("order_groups", []) or []:
        items = group.get("items", []) if isinstance(group, dict) else []
        for item in items:
            _resolve_pdf_url_for_item(
                scraper,
                item,
                view_params=view_params,
                retries=3,
                force=False,
            )


def _get_worker_scraper() -> DistrictCaseStatusScraper:
    scraper = getattr(_thread_local, "scraper", None)
    if scraper is None:
        scraper = DistrictCaseStatusScraper(use_local_model=False, proxy_getter=_get_next_shared_proxy)
        proxy = _get_next_shared_proxy()
        if proxy:
            scraper.set_proxy(proxy)
        scraper.init_session()
        _thread_local.scraper = scraper
    return scraper


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


def _download_pdf_with_scraper(
    scraper: DistrictCaseStatusScraper,
    pdf_url: str,
    output_path: Path,
    refresh_pdf_url: Optional[Callable[[], Optional[str]]] = None,
) -> Dict[str, object]:
    started = time.perf_counter()
    current_pdf_url = _normalize_pdf_url(pdf_url)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "identity",
        "Referer": ENTRY_URL,
    }
    last_error: Optional[str] = None
    for attempt in range(1, 5):
        try:
            response = scraper.session.get(
                current_pdf_url,
                headers=headers,
                timeout=60,
            )
        except (
            requests.exceptions.ProxyError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
        ) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < 4:
                if attempt >= 2:
                    scraper._rotate_proxy_and_reinit()
                    if refresh_pdf_url:
                        refreshed = refresh_pdf_url()
                        if refreshed:
                            current_pdf_url = _normalize_pdf_url(refreshed)
                time.sleep(0.5 if attempt == 1 else 1.0)
                continue
            raise RuntimeError(last_error) from exc

        if response.status_code == 405:
            last_error = f"405 Security Page for url: {current_pdf_url}"
            if attempt < 4:
                print("[!] 405 on PDF download, rotating proxy and retrying...")
                scraper._rotate_proxy_and_reinit()
                if refresh_pdf_url:
                    refreshed = refresh_pdf_url()
                    if refreshed:
                        current_pdf_url = _normalize_pdf_url(refreshed)
                time.sleep(min(1.0 * attempt, 3.0))
                continue
            response.raise_for_status()

        if response.status_code == 400:
            last_error = f"400 Client Error: Bad Request for url: {current_pdf_url}"
            if attempt < 4:
                # Fast path for session-bound stale links: resolve once and retry only if URL actually changed.
                refreshed = refresh_pdf_url() if refresh_pdf_url else None
                if refreshed:
                    refreshed_url = _normalize_pdf_url(refreshed)
                    if refreshed_url != current_pdf_url:
                        current_pdf_url = refreshed_url
                        continue
            response.raise_for_status()

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            last_error = str(exc)
            if attempt < 4 and response.status_code >= 500:
                scraper._rotate_proxy_and_reinit()
                if refresh_pdf_url:
                    refreshed = refresh_pdf_url()
                    if refreshed:
                        current_pdf_url = _normalize_pdf_url(refreshed)
                time.sleep(min(1.0 * attempt, 3.0))
                continue
            raise

        content_type = str(response.headers.get("Content-Type", "")).lower()
        if "pdf" not in content_type and not response.content.startswith(b"%PDF"):
            preview = response.text[:120].replace("\n", " ").replace("\r", " ").strip()
            last_error = (
                f"Non-PDF response: status={response.status_code}, content_type={content_type or 'unknown'}, "
                f"preview={preview}"
            )
            if attempt < 4:
                scraper._rotate_proxy_and_reinit()
                if refresh_pdf_url:
                    refreshed = refresh_pdf_url()
                    if refreshed:
                        current_pdf_url = _normalize_pdf_url(refreshed)
                time.sleep(min(1.0 * attempt, 3.0))
                continue
            raise RuntimeError(last_error)

        output_path.write_bytes(response.content)
        return {
            "ok": True,
            "elapsed_sec": time.perf_counter() - started,
            "pdf_url": current_pdf_url,
            "bytes": len(response.content),
            "output_path": str(output_path.resolve()),
            "attempts": attempt,
        }

    raise RuntimeError(last_error or f"Failed to download PDF after retries: {current_pdf_url}")


def _fetch_view_history_with_retries(
    scraper: DistrictCaseStatusScraper,
    params: Dict[str, str],
    attempts: int = 4,
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
            time.sleep(min(1.0 * attempt, 3.0))
    return last_view, "", attempts


def fetch_case_detail(work_item: Dict[str, object], pdf_mode: str) -> Dict[str, object]:
    started = time.perf_counter()
    case = work_item["case"]
    params = work_item["params"]
    cnr = str(work_item["cnr"])
    scraper = _get_worker_scraper()
    view, html, view_attempts = _fetch_view_history_with_retries(scraper, params, attempts=4)
    if not html:
        return {
            "ok": False,
            "row_index": work_item["row_index"],
            "cnr": cnr,
            "case_title": work_item["case_title"],
            "party_names": work_item["party_names"],
            "json_path": str(Path(str(work_item["json_path"])).resolve()),
            "html_path": str(Path(str(work_item["html_path"])).resolve()),
            "pdf_case_dir": str(Path(str(work_item["pdf_case_dir"])).resolve()),
            "error": view.get("errormsg") or "Empty data_list from viewHistory",
            "detail_failure_category": "empty_cins",
            "detail_attempts": view_attempts,
            "elapsed_sec": time.perf_counter() - started,
            "search_row": case,
        }
    html_path = Path(str(work_item["html_path"]))
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html, encoding="utf-8")
    structured = parse_case_detail_html(html)
    pdf_discovery_audit = _build_pdf_discovery_audit(html, structured)
    _resolve_case_pdf_urls(scraper, structured, params)
    structured["cnr"] = cnr
    structured["case_no_internal"] = params["case_no"]
    structured["search_row"] = case
    structured["raw_html_path"] = str(html_path.resolve())
    structured["pdf_discovery_audit"] = pdf_discovery_audit
    structured["view_history_attempts"] = view_attempts
    pdf_successes: List[Dict[str, object]] = []
    pdf_failures: List[Dict[str, object]] = []
    local_pdf_paths: List[str] = []
    pdf_elapsed = 0.0
    pdf_case_dir = Path(str(work_item["pdf_case_dir"]))
    pdf_tasks = _pdf_tasks_from_case(structured, pdf_mode)
    pdf_discovery_audit["download_task_count_for_mode"] = len(pdf_tasks)
    for task in pdf_tasks:
        item = task["item"]
        pdf_url = item.get("pdf_url") or item.get("pdf_href")
        if not pdf_url:
            pdf_url = _resolve_pdf_url_for_item(
                scraper,
                item,
                view_params=params,
                retries=2,
                force=True,
            )
        if not pdf_url:
            item["local_pdf_path"] = None
            item["pdf_download_ok"] = False
            item["pdf_download_error"] = "Missing pdf_url after resolution"
            pdf_failures.append({"ok": False, "error": "Missing pdf_url after resolution", "cnr": cnr})
            continue
        group_name = _sanitize_filename(str(task["group_type"]))
        order_name = _sanitize_filename(str(item.get("order_number") or f"order_{task['order_index']}"))
        order_date = _sanitize_filename(str(item.get("order_date") or item.get("order_on") or ""))
        filename = f"{order_name}_{order_date}.pdf" if order_date else f"{order_name}.pdf"
        output_path = pdf_case_dir / group_name / filename
        try:
            download_result = _download_pdf_with_scraper(
                scraper,
                str(pdf_url),
                output_path,
                refresh_pdf_url=lambda: _resolve_pdf_url_for_item(
                    scraper,
                    item,
                    view_params=params,
                    retries=2,
                    force=True,
                ),
            )
            download_result["cnr"] = cnr
            pdf_elapsed += float(download_result["elapsed_sec"])
            item["local_pdf_path"] = str(output_path.resolve())
            item["pdf_download_ok"] = True
            item["pdf_download_error"] = None
            local_pdf_paths.append(item["local_pdf_path"])
            pdf_successes.append(download_result)
        except Exception as exc:
            item["local_pdf_path"] = None
            item["pdf_download_ok"] = False
            item["pdf_download_error"] = str(exc)
            item["local_pdf_target_path"] = str(output_path.resolve())
            pdf_failures.append(
                {
                    "ok": False,
                    "error": str(exc),
                    "cnr": cnr,
                    "pdf_url": str(item.get("pdf_url") or pdf_url),
                    "output_path": str(output_path.resolve()),
                }
            )
    structured["local_pdf_paths"] = local_pdf_paths
    structured["pdf_download_summary"] = {
        "mode": pdf_mode,
        "success_count": len(pdf_successes),
        "failure_count": len(pdf_failures),
    }
    return {
        "ok": True,
        "row_index": work_item["row_index"],
        "cnr": cnr,
        "case_title": work_item["case_title"],
        "party_names": work_item["party_names"],
        "json_path": str(Path(str(work_item["json_path"])).resolve()),
        "html_path": str(html_path.resolve()),
        "pdf_case_dir": str(pdf_case_dir.resolve()),
        "elapsed_sec": time.perf_counter() - started,
        "structured": structured,
        "pdf_successes": pdf_successes,
        "pdf_failures": pdf_failures,
        "pdf_elapsed_sec": pdf_elapsed,
        "pdf_discovery_audit": pdf_discovery_audit,
    }


def _pdf_tasks_from_case(case_detail: Dict[str, object], mode: str) -> List[Dict[str, object]]:
    tasks: List[Dict[str, object]] = []
    case_title = str(case_detail.get("search_row", {}).get("case_type_case_number_case_year", "")) if isinstance(case_detail.get("search_row"), dict) else ""
    for group in case_detail.get("order_groups", []) or []:
        items = group.get("items", []) if isinstance(group, dict) else []
        selected_items = items[:1] if mode == "first" else items
        for idx, item in enumerate(selected_items, start=1):
            if not item.get("pdf_params") and not item.get("pdf_href"):
                continue
            tasks.append(
                {
                    "cnr": case_detail.get("cnr", ""),
                    "case_title": case_title,
                    "group_type": group.get("type", "orders"),
                    "order_index": idx,
                    "item": item,
                }
            )
    return tasks


def _persist_case_json(
    result: Dict[str, object],
    query_meta: Dict[str, object],
    jurisdiction: Dict[str, object],
    run_id: str,
    pdf_mode: str,
) -> None:
    payload = dict(result["structured"])
    payload["jurisdiction"] = jurisdiction
    payload["query"] = query_meta
    payload["pipeline"] = {
        "run_id": run_id,
        "saved_at_utc": _now_utc_iso(),
        "dedupe_key": result["cnr"],
        "json_path": str(Path(str(result["json_path"])).resolve()),
        "pdf_case_dir": str(Path(str(result["pdf_case_dir"])).resolve()),
        "pdf_mode": pdf_mode,
        "detail_elapsed_sec": round(float(result["elapsed_sec"]), 6),
        "pdf_success_count": len(result["pdf_successes"]),
        "pdf_failure_count": len(result["pdf_failures"]),
    }
    _write_json(Path(str(result["json_path"])), payload)


def _build_case_event(
    *,
    status: str,
    row_index: int,
    cnr: Optional[str],
    case_title: str,
    party_names: str,
    json_path: Optional[str],
    pdf_case_dir: Optional[str],
    elapsed_sec: float,
    retry_recommended: bool,
    error: Optional[str],
    pdf_success_count: int = 0,
    pdf_failure_count: int = 0,
    pdf_raw_ref_count: int = 0,
    pdf_parsed_ref_count: int = 0,
    pdf_parser_miss_count: int = 0,
    pdf_parser_miss_detected: bool = False,
    failure_category: Optional[str] = None,
    search_row: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    event = {
        "timestamp_utc": _now_utc_iso(),
        "row_index": row_index,
        "cnr": cnr,
        "case_title": case_title,
        "party_names": party_names,
        "status": status,
        "elapsed_sec": round(float(elapsed_sec), 6),
        "retry_recommended": retry_recommended,
        "error": error,
        "json_path": json_path,
        "pdf_case_dir": pdf_case_dir,
        "pdf_success_count": pdf_success_count,
        "pdf_failure_count": pdf_failure_count,
        "pdf_raw_ref_count": pdf_raw_ref_count,
        "pdf_parsed_ref_count": pdf_parsed_ref_count,
        "pdf_parser_miss_count": pdf_parser_miss_count,
        "pdf_parser_miss_detected": pdf_parser_miss_detected,
    }
    if failure_category:
        event["failure_category"] = failure_category
    if search_row is not None:
        event["search_row"] = search_row
    return event


def _run_case_phase(
    work_items: List[Dict[str, object]],
    workers: int,
    pdf_mode: str,
) -> Tuple[List[Dict[str, object]], float]:
    started = time.perf_counter()
    results: List[Dict[str, object]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch_case_detail, item, pdf_mode) for item in work_items]
        for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                result = future.result()
            except Exception as exc:
                result = {"ok": False, "error": str(exc), "elapsed_sec": 0.0}
            results.append(result)
            if idx % 100 == 0:
                print(f"[*] Case-detail progress: {idx}/{len(futures)}")
    return results, time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Persist district case-status results into CNR-keyed JSON/PDF pipeline outputs"
    )
    parser.add_argument("--name", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--district", required=True)
    parser.add_argument("--court-complex", required=True)
    parser.add_argument("--year", required=True)
    parser.add_argument("--status", choices=["Pending", "Disposed", "Both"], default="Both")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--detail-workers", type=int, default=6)
    parser.add_argument("--pdf-mode", choices=["first", "all"], default="all")
    parser.add_argument("--refresh-existing", action="store_true")
    parser.add_argument("--captcha-api-key", type=str)
    parser.add_argument("--proxy-file", type=str, help="Proxy list (ip:port:user:pass) for rotating on 405")
    parser.add_argument(
        "--rank-proxies",
        action="store_true",
        help="Probe proxies against entry page and keep only fastest healthy subset",
    )
    parser.add_argument(
        "--proxy-rank-top-k",
        type=int,
        default=64,
        help="How many fastest proxies to keep when --rank-proxies is enabled (0 keeps all healthy)",
    )
    parser.add_argument(
        "--proxy-rank-workers",
        type=int,
        default=120,
        help="Concurrent workers used during proxy probe ranking",
    )
    parser.add_argument(
        "--proxy-rank-timeout",
        type=float,
        default=12.0,
        help="Seconds timeout for each proxy probe request",
    )
    parser.add_argument(
        "--search-retries",
        type=int,
        default=3,
        help="Max attempts for initial party-name search",
    )
    parser.add_argument(
        "--search-retry-sleep",
        type=float,
        default=0.8,
        help="Base sleep seconds between search retries (multiplied by attempt index)",
    )
    parser.add_argument(
        "--retry-empty-search",
        dest="retry_empty_search",
        action="store_true",
        help="Retry initial search when response has zero rows",
    )
    parser.add_argument(
        "--no-retry-empty-search",
        dest="retry_empty_search",
        action="store_false",
        help="Do not retry initial search on zero-row responses",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Base directory under which ecourt_output_json, ecourt_output_pdf, and ecourt_output_logs are created",
    )
    parser.set_defaults(retry_empty_search=True)
    args = parser.parse_args()

    if args.proxy_rank_top_k < 0:
        raise SystemExit("--proxy-rank-top-k must be >= 0")
    if args.proxy_rank_workers <= 0:
        raise SystemExit("--proxy-rank-workers must be > 0")
    if args.proxy_rank_timeout <= 0:
        raise SystemExit("--proxy-rank-timeout must be > 0")

    proxy_pool_info: Dict[str, object] = {
        "loaded_count": 0,
        "active_count": 0,
        "ranked": False,
    }
    if args.proxy_file:
        try:
            loaded_count = _load_shared_proxies(args.proxy_file)
            proxy_pool_info["loaded_count"] = loaded_count
            proxy_pool_info["active_count"] = loaded_count
            print(f"[+] Loaded {loaded_count} proxies for worker rotation")
            if args.rank_proxies and loaded_count > 0:
                print(
                    "[*] Ranking proxies "
                    f"(workers={args.proxy_rank_workers}, timeout={args.proxy_rank_timeout}s, "
                    f"top_k={args.proxy_rank_top_k}) ..."
                )
                ranking_info = _rank_shared_proxies(
                    top_k=int(args.proxy_rank_top_k),
                    workers=int(args.proxy_rank_workers),
                    timeout_sec=float(args.proxy_rank_timeout),
                )
                proxy_pool_info.update(ranking_info)
                proxy_pool_info["active_count"] = len(_shared_proxy_pool)
                print(
                    "[+] Proxy ranking complete: "
                    f"healthy={ranking_info.get('healthy_count', 0)} "
                    f"selected={ranking_info.get('selected_count', 0)} "
                    f"probe_elapsed={ranking_info.get('probe_elapsed_sec', 0.0)}s"
                )
        except Exception as exc:
            print(f"[!] Failed to load proxy file: {exc}")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    query_meta = {
        "name": args.name,
        "state": args.state,
        "district": args.district,
        "court_complex": args.court_complex,
        "year": args.year,
        "status": args.status,
    }

    benchmark_started = time.perf_counter()
    search_scraper = DistrictCaseStatusScraper(
        captcha_api_key=args.captcha_api_key,
        use_local_model=False,
        proxy_getter=_get_next_shared_proxy,
    )

    search_retries = max(1, int(args.search_retries))
    search_result: Dict[str, object] = {}
    best_search_result: Optional[Dict[str, object]] = None
    best_case_count = -1
    search_elapsed = 0.0
    search_attempts: List[Dict[str, object]] = []

    for attempt in range(1, search_retries + 1):
        attempt_started = time.perf_counter()
        try:
            current_result = search_scraper.search_by_party_name(
                args.name,
                args.state,
                args.district,
                args.court_complex,
                case_status=args.status,
                year=args.year,
            )
            attempt_elapsed = time.perf_counter() - attempt_started
            search_elapsed += attempt_elapsed
        except Exception as exc:
            attempt_elapsed = time.perf_counter() - attempt_started
            search_elapsed += attempt_elapsed
            search_attempts.append(
                {
                    "attempt": attempt,
                    "elapsed_sec": round(attempt_elapsed, 3),
                    "rows_found": 0,
                    "status": None,
                    "captcha_rejected": False,
                    "errormsg": str(exc),
                }
            )
            if attempt < search_retries:
                try:
                    search_scraper._rotate_proxy_and_reinit()
                except Exception:
                    pass
                time.sleep(max(0.0, args.search_retry_sleep) * attempt)
                continue
            raise

        current_cases = current_result.get("cases", []) or []
        captcha_rejected = search_scraper._is_captcha_error(current_result)
        current_err = str(current_result.get("errormsg") or "")
        current_status = current_result.get("status")

        search_attempts.append(
            {
                "attempt": attempt,
                "elapsed_sec": round(attempt_elapsed, 3),
                "rows_found": len(current_cases),
                "status": current_status,
                "captcha_rejected": captcha_rejected,
                "errormsg": current_err or None,
            }
        )

        if len(current_cases) > best_case_count:
            best_search_result = current_result
            best_case_count = len(current_cases)

        retry_this_attempt = False
        if attempt < search_retries:
            if captcha_rejected or bool(current_err):
                retry_this_attempt = True
            elif args.retry_empty_search and len(current_cases) == 0:
                retry_this_attempt = True

        if retry_this_attempt:
            try:
                search_scraper._rotate_proxy_and_reinit()
            except Exception:
                pass
            time.sleep(max(0.0, args.search_retry_sleep) * attempt)
            continue

        search_result = current_result
        break

    if not search_result:
        search_result = best_search_result or {}

    all_cases = search_result.get("cases", []) or []
    selected_cases = all_cases[: args.limit]

    jurisdiction = _resolve_jurisdiction_metadata(
        search_scraper,
        args.state,
        args.district,
        args.court_complex,
    )
    layout = _build_output_layout(Path(args.output_dir), jurisdiction, run_id)

    run_context = {
        "run_id": run_id,
        "started_at_utc": _now_utc_iso(),
        "query": query_meta,
        "jurisdiction": jurisdiction,
        "detail_workers": args.detail_workers,
        "pdf_mode": args.pdf_mode,
        "refresh_existing": args.refresh_existing,
        "search_retries": search_retries,
        "search_retry_sleep": args.search_retry_sleep,
        "retry_empty_search": args.retry_empty_search,
        "proxy_pool": proxy_pool_info,
        "storage": {
            "base_output_dir": str(layout["base_output_dir"]),
            "json_case_dir": str(layout["json_case_dir"]),
            "pdf_case_dir": str(layout["pdf_case_dir"]),
            "log_dir": str(layout["log_dir"]),
        },
    }
    _write_json(layout["run_context_path"], run_context)

    work_items, case_events = _prepare_case_work_items(
        selected_cases,
        layout,
        args.refresh_existing,
    )
    case_results, detail_elapsed = _run_case_phase(
        work_items,
        args.detail_workers,
        args.pdf_mode,
    )

    retry_cases: List[Dict[str, object]] = []
    error_events: List[Dict[str, object]] = []
    empty_cins_cases: List[Dict[str, object]] = []
    pdf_discovery_audit_cases: List[Dict[str, object]] = []
    saved_case_count = 0
    detail_success_count = 0
    detail_failure_count = 0
    detail_failure_category_counts: Dict[str, int] = {}
    pdf_success_count = 0
    pdf_failure_count = 0
    pdf_elapsed = 0.0
    pdf_raw_ref_count = 0
    pdf_parsed_ref_count = 0
    pdf_parser_miss_case_count = 0
    pdf_parser_miss_count = 0

    for result in case_results:
        if result.get("ok"):
            detail_success_count += 1
            pdf_success_count += len(result["pdf_successes"])
            pdf_failure_count += len(result["pdf_failures"])
            pdf_elapsed += float(result["pdf_elapsed_sec"])
            audit = result.get("pdf_discovery_audit") or {}
            raw_ref_count = int(audit.get("raw_pdf_ref_count") or 0)
            parsed_ref_count = int(audit.get("parsed_pdf_ref_count") or 0)
            parser_miss_count = int(audit.get("missing_pdf_ref_count") or 0)
            parser_miss_detected = bool(audit.get("parser_miss_detected"))
            pdf_raw_ref_count += raw_ref_count
            pdf_parsed_ref_count += parsed_ref_count
            pdf_parser_miss_count += parser_miss_count
            if parser_miss_detected:
                pdf_parser_miss_case_count += 1
            _persist_case_json(result, query_meta, jurisdiction, run_id, args.pdf_mode)
            saved_case_count += 1

            has_pdf_failures = bool(result["pdf_failures"])
            if has_pdf_failures and parser_miss_detected:
                status = "saved_with_pdf_failures_and_audit_flags"
                error_text = "One or more PDF downloads failed; raw HTML also contained unmatched PDF references"
            elif has_pdf_failures:
                status = "saved_with_pdf_failures"
                error_text = "One or more PDF downloads failed"
            elif parser_miss_detected:
                status = "saved_with_pdf_audit_flags"
                error_text = "Raw HTML contained PDF references that were not converted into parsed PDF tasks"
            else:
                status = "saved"
                error_text = None
            event = _build_case_event(
                status=status,
                row_index=int(result["row_index"]),
                cnr=str(result["cnr"]),
                case_title=str(result["case_title"]),
                party_names=str(result["party_names"]),
                json_path=str(result["json_path"]),
                pdf_case_dir=str(result["pdf_case_dir"]),
                elapsed_sec=float(result["elapsed_sec"]),
                retry_recommended=has_pdf_failures or parser_miss_detected,
                error=error_text,
                pdf_success_count=len(result["pdf_successes"]),
                pdf_failure_count=len(result["pdf_failures"]),
                pdf_raw_ref_count=raw_ref_count,
                pdf_parsed_ref_count=parsed_ref_count,
                pdf_parser_miss_count=parser_miss_count,
                pdf_parser_miss_detected=parser_miss_detected,
            )
            case_events.append(event)
            if parser_miss_detected:
                pdf_discovery_audit_cases.append(
                    {
                        **event,
                        "pdf_discovery_audit": audit,
                    }
                )
            if has_pdf_failures or parser_miss_detected:
                retry_reasons: List[str] = []
                if has_pdf_failures:
                    retry_reasons.append("pdf_failures")
                if parser_miss_detected:
                    retry_reasons.append("pdf_parser_miss")
                retry_case = {
                    **event,
                    "retry_reason": retry_reasons[0] if len(retry_reasons) == 1 else "multiple",
                    "retry_reasons": retry_reasons,
                    "pdf_failures": result["pdf_failures"],
                    "pdf_discovery_audit": audit,
                }
                retry_cases.append(retry_case)
                error_events.append(retry_case)
        else:
            detail_failure_count += 1
            detail_failure_category = str(result.get("detail_failure_category") or "detail_failure")
            detail_failure_category_counts[detail_failure_category] = (
                detail_failure_category_counts.get(detail_failure_category, 0) + 1
            )
            event = _build_case_event(
                status="detail_failed",
                row_index=int(result.get("row_index") or 0),
                cnr=result.get("cnr"),
                case_title=str(result.get("case_title") or ""),
                party_names=str(result.get("party_names") or ""),
                json_path=result.get("json_path"),
                pdf_case_dir=result.get("pdf_case_dir"),
                elapsed_sec=float(result.get("elapsed_sec") or 0.0),
                retry_recommended=True,
                error=str(result.get("error") or "Unknown detail failure"),
                failure_category=detail_failure_category,
                search_row=result.get("search_row"),
            )
            case_events.append(event)
            retry_case = {
                **event,
                "retry_reason": detail_failure_category,
                "retry_reasons": [detail_failure_category],
            }
            retry_cases.append(retry_case)
            error_events.append(retry_case)
            if detail_failure_category == "empty_cins":
                empty_cins_cases.append(retry_case)

    total_elapsed = time.perf_counter() - benchmark_started

    _write_jsonl(layout["case_events_path"], case_events)
    _write_json(layout["case_errors_path"], error_events)
    _write_json(layout["retry_cases_path"], retry_cases)
    _write_json(layout["empty_cins_path"], empty_cins_cases)
    _write_json(layout["pdf_discovery_audit_path"], pdf_discovery_audit_cases)

    skipped_existing_count = sum(1 for event in case_events if event["status"] == "skipped_existing_cnr")
    skipped_duplicate_count = sum(1 for event in case_events if event["status"] == "skipped_duplicate_cnr")
    invalid_onclick_count = sum(1 for event in case_events if event["status"] == "invalid_onclick")

    summary = {
        "run_id": run_id,
        "query": query_meta,
        "jurisdiction": jurisdiction,
        "storage": {
            "base_output_dir": str(layout["base_output_dir"]),
            "json_case_dir": str(layout["json_case_dir"]),
            "pdf_case_dir": str(layout["pdf_case_dir"]),
            "log_dir": str(layout["log_dir"]),
        },
        "search": {
            "elapsed_sec": round(search_elapsed, 3),
            "attempt_count": len(search_attempts),
            "configured_retries": search_retries,
            "retry_empty_search": args.retry_empty_search,
            "total_rows_found": len(all_cases),
            "rows_selected": len(selected_cases),
            "attempts": search_attempts,
        },
        "proxy_pool": proxy_pool_info,
        "dedupe": {
            "unique_cases_queued": len(work_items),
            "skipped_existing_cnr": skipped_existing_count,
            "skipped_duplicate_cnr": skipped_duplicate_count,
            "invalid_onclick": invalid_onclick_count,
            "refresh_existing": args.refresh_existing,
        },
        "details": {
            "elapsed_sec": round(detail_elapsed, 3),
            "success_count": detail_success_count,
            "failure_count": detail_failure_count,
            "failure_category_counts": detail_failure_category_counts,
            "cases_per_sec": round(detail_success_count / detail_elapsed, 3) if detail_elapsed else 0.0,
        },
        "json_output": {
            "saved_case_count": saved_case_count,
        },
        "pdfs": {
            "mode": args.pdf_mode,
            "tasks_discovered": pdf_success_count + pdf_failure_count,
            "elapsed_sec": round(pdf_elapsed, 3),
            "success_count": pdf_success_count,
            "failure_count": pdf_failure_count,
            "pdfs_per_sec": round(pdf_success_count / pdf_elapsed, 3) if pdf_elapsed else 0.0,
        },
        "pdf_discovery_audit": {
            "raw_pdf_ref_count": pdf_raw_ref_count,
            "parsed_pdf_ref_count": pdf_parsed_ref_count,
            "parser_miss_case_count": pdf_parser_miss_case_count,
            "parser_miss_ref_count": pdf_parser_miss_count,
            "audit_cases_path": str(layout["pdf_discovery_audit_path"]),
        },
        "retry": {
            "retry_case_count": len(retry_cases),
            "retry_cases_path": str(layout["retry_cases_path"]),
            "empty_cins_count": len(empty_cins_cases),
            "empty_cins_path": str(layout["empty_cins_path"]),
        },
        "logs": {
            "case_events_path": str(layout["case_events_path"]),
            "case_errors_path": str(layout["case_errors_path"]),
            "empty_cins_path": str(layout["empty_cins_path"]),
            "pdf_discovery_audit_path": str(layout["pdf_discovery_audit_path"]),
            "run_context_path": str(layout["run_context_path"]),
        },
        "total_elapsed_sec": round(total_elapsed, 3),
    }
    _write_json(layout["summary_path"], summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
