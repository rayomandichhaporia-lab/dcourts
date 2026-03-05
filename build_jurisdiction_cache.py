import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set

from district_casestatus_scraper import DistrictCaseStatusScraper


def _sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^\w\-.]+", "_", (name or "").strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "item"


def _safe_label(name: str, fallback: str) -> str:
    cleaned = _sanitize_filename(name)
    return cleaned or _sanitize_filename(fallback) or "unknown"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_state_filters(raw_filter: str, states: Dict[str, str]) -> Set[str]:
    if not raw_filter.strip():
        return set(states.keys())

    tokens = [token.strip() for token in raw_filter.split(",") if token.strip()]
    selected: Set[str] = set()
    lower_tokens = [token.lower() for token in tokens]

    for state_code, state_name in states.items():
        if state_code in tokens:
            selected.add(state_code)
            continue
        normalized_name = state_name.lower()
        if any(token == normalized_name for token in lower_tokens):
            selected.add(state_code)
            continue
        if any(token in normalized_name for token in lower_tokens):
            selected.add(state_code)

    return selected


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and cache state/district/court-complex mappings for eCourts district flow."
    )
    parser.add_argument(
        "--cache-path",
        type=str,
        default="jurisdiction_cache.json",
        help="Path to write cache JSON",
    )
    parser.add_argument(
        "--states",
        type=str,
        default="",
        help="Comma-separated state codes or names (default: all states)",
    )
    parser.add_argument("--captcha-api-key", type=str)
    parser.add_argument("--proxy-file", type=str)
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()

    cache_path = Path(args.cache_path).resolve()
    if cache_path.exists() and not args.force_refresh:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        count = len(payload.get("entries", []))
        print(f"[=] Using existing cache at {cache_path} ({count} court-path entries).")
        return

    scraper = DistrictCaseStatusScraper(
        captcha_api_key=args.captcha_api_key,
        proxy_file=args.proxy_file,
        use_local_model=False,
    )

    states = scraper.list_states()
    selected_state_codes = _parse_state_filters(args.states, states)
    if args.states and not selected_state_codes:
        raise SystemExit(f"No states matched filter: {args.states}")

    entries: List[Dict[str, str]] = []
    state_rollup: List[Dict[str, object]] = []
    total_district_count = 0

    for state_code in sorted(selected_state_codes, key=lambda code: int(code) if code.isdigit() else code):
        state_name = states.get(state_code, state_code)
        districts = scraper.list_districts(state_code)
        total_district_count += len(districts)
        state_court_count = 0

        for district_code, district_name in sorted(districts.items(), key=lambda item: item[0]):
            courts = scraper.list_court_complexes(state_code, district_code)
            state_court_count += len(courts)
            for raw_court_code, court_name in sorted(courts.items(), key=lambda item: item[0]):
                court_complex_code = scraper._extract_complex_code(raw_court_code)
                entries.append(
                    {
                        "state_code": state_code,
                        "state_name": state_name,
                        "district_code": district_code,
                        "district_name": district_name,
                        "court_complex_code": court_complex_code,
                        "court_complex_raw_code": raw_court_code,
                        "court_complex_name": court_name,
                        "path_parts": {
                            "state": _safe_label(state_name, state_code),
                            "district": _safe_label(district_name, district_code),
                            "court_complex": _safe_label(court_name, court_complex_code),
                        },
                    }
                )

        state_rollup.append(
            {
                "state_code": state_code,
                "state_name": state_name,
                "district_count": len(districts),
                "court_complex_count": state_court_count,
            }
        )

    payload: Dict[str, object] = {
        "generated_at_utc": _now_utc_iso(),
        "source": "district_casestatus_scraper.list_states/list_districts/list_court_complexes",
        "filters": {
            "states": args.states or "all",
        },
        "counts": {
            "states": len(state_rollup),
            "districts": total_district_count,
            "court_complexes": len(entries),
        },
        "states": state_rollup,
        "entries": entries,
    }

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[+] Cache written: {cache_path}")
    print(json.dumps(payload["counts"], indent=2))


if __name__ == "__main__":
    main()
