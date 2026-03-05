import argparse
import concurrent.futures
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _state_filter_matches(raw_filter: str, state_code: str, state_name: str) -> bool:
    if not raw_filter.strip():
        return True
    tokens = [token.strip().lower() for token in raw_filter.split(",") if token.strip()]
    state_name_lower = (state_name or "").lower()
    for token in tokens:
        if token == state_code.lower():
            return True
        if token == state_name_lower:
            return True
        if token in state_name_lower:
            return True
    return False


def _load_entries(cache_path: Path, states_filter: str) -> List[Dict[str, object]]:
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    entries = payload.get("entries", [])
    if not isinstance(entries, list):
        return []
    filtered: List[Dict[str, object]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        state_code = str(entry.get("state_code") or "")
        state_name = str(entry.get("state_name") or "")
        if _state_filter_matches(states_filter, state_code, state_name):
            filtered.append(entry)
    filtered.sort(
        key=lambda item: (
            str(item.get("state_code") or ""),
            str(item.get("district_code") or ""),
            str(item.get("court_complex_code") or ""),
        )
    )
    return filtered


def _run_dir_for_entry(base_output_dir: Path, entry: Dict[str, object]) -> Path:
    path_parts = entry.get("path_parts") or {}
    state = str(path_parts.get("state") or "")
    district = str(path_parts.get("district") or "")
    court_complex = str(path_parts.get("court_complex") or "")
    return base_output_dir / "ecourt_output_logs" / state / district / court_complex


def _list_run_dirs(run_root: Path) -> Set[str]:
    if not run_root.exists():
        return set()
    return {
        child.name
        for child in run_root.iterdir()
        if child.is_dir() and child.name.startswith("run_")
    }


def _find_summary_path(run_root: Path, before: Set[str], after: Set[str]) -> Optional[Path]:
    created = sorted(after - before)
    if not created:
        return None
    candidate = run_root / created[-1] / "summary.json"
    if candidate.exists():
        return candidate
    return None


def _build_job_key(
    entry: Dict[str, object],
    year: int,
    name: str,
    status: str,
    limit: int,
    pdf_mode: str,
) -> str:
    return "|".join(
        [
            str(entry.get("state_code") or ""),
            str(entry.get("district_code") or ""),
            str(entry.get("court_complex_code") or ""),
            str(year),
            (name or "").strip().lower(),
            status,
            str(limit),
            pdf_mode,
        ]
    )


def _load_progress_cache(path: Path) -> Set[str]:
    completed: Set[str] = set()
    if not path.exists():
        return completed
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if int(row.get("return_code") or 0) != 0:
                continue
            job_key = str(row.get("job_key") or "")
            if job_key:
                completed.add(job_key)
    return completed


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _append_jsonl(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run benchmark_case_status across cached court paths for a year range."
    )
    parser.add_argument("--cache-path", type=str, default="jurisdiction_cache.json")
    parser.add_argument("--states", type=str, default="", help="Comma-separated state codes or names")
    parser.add_argument("--name", type=str, default="Patil", help="Party name query")
    parser.add_argument("--year-start", type=int, default=1995)
    parser.add_argument("--year-end", type=int, default=2026)
    parser.add_argument("--year-step", type=int, default=1)
    parser.add_argument(
        "--year-order",
        choices=["asc", "desc"],
        default="asc",
        help="Year traversal order for matrix jobs",
    )
    parser.add_argument("--status", choices=["Pending", "Disposed", "Both"], default="Both")
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--detail-workers", type=int, default=6)
    parser.add_argument("--pdf-mode", choices=["first", "all"], default="all")
    parser.add_argument("--refresh-existing", action="store_true")
    parser.add_argument("--captcha-api-key", type=str)
    parser.add_argument("--proxy-file", type=str)
    parser.add_argument(
        "--rank-proxies",
        action="store_true",
        help="Enable proxy probe ranking in benchmark_case_status.py before each job",
    )
    parser.add_argument("--proxy-rank-top-k", type=int, default=64)
    parser.add_argument("--proxy-rank-workers", type=int, default=120)
    parser.add_argument("--proxy-rank-timeout", type=float, default=12.0)
    parser.add_argument("--output-dir", type=str, default=".")
    parser.add_argument("--max-jobs", type=int, default=0, help="Optional cap for smoke runs")
    parser.add_argument(
        "--parallel-jobs",
        type=int,
        default=24,
        help="How many benchmark jobs to run concurrently",
    )
    parser.add_argument(
        "--progress-cache-path",
        type=str,
        default="_tmp/state_year_matrix/progress_cache.jsonl",
        help="Persistent success cache to skip completed court-year jobs across reruns",
    )
    parser.add_argument(
        "--skip-completed",
        dest="skip_completed",
        action="store_true",
        help="Skip jobs already marked successful in progress cache",
    )
    parser.add_argument(
        "--no-skip-completed",
        dest="skip_completed",
        action="store_false",
        help="Ignore progress cache and run everything",
    )
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument(
        "--matrix-run-dir",
        type=str,
        default="_tmp/state_year_matrix",
        help="Where matrix-run logs should be written",
    )
    parser.add_argument(
        "--save-success-logs",
        action="store_true",
        help="By default only failed job stdout/stderr are saved",
    )
    parser.set_defaults(skip_completed=True)
    args = parser.parse_args()

    if args.year_step <= 0:
        raise SystemExit("--year-step must be > 0")
    if args.year_end < args.year_start:
        raise SystemExit("--year-end must be >= --year-start")
    if args.parallel_jobs <= 0:
        raise SystemExit("--parallel-jobs must be > 0")
    if args.proxy_rank_top_k < 0:
        raise SystemExit("--proxy-rank-top-k must be >= 0")
    if args.proxy_rank_workers <= 0:
        raise SystemExit("--proxy-rank-workers must be > 0")
    if args.proxy_rank_timeout <= 0:
        raise SystemExit("--proxy-rank-timeout must be > 0")

    script_dir = Path(__file__).resolve().parent
    cache_path = Path(args.cache_path).resolve()
    if not cache_path.exists():
        raise SystemExit(f"Cache not found: {cache_path}")

    entries = _load_entries(cache_path, args.states)
    if not entries:
        raise SystemExit("No cache entries matched the provided state filter.")

    progress_cache_path = Path(args.progress_cache_path).resolve()
    completed_job_keys = _load_progress_cache(progress_cache_path) if args.skip_completed else set()

    years = list(range(args.year_start, args.year_end + 1, args.year_step))
    if args.year_order == "desc":
        years = list(reversed(years))
    jobs_all: List[Tuple[Dict[str, object], int, str]] = []
    # Interleave by year so parallel jobs are spread across different court paths.
    for year in years:
        for entry in entries:
            job_key = _build_job_key(
                entry=entry,
                year=year,
                name=args.name,
                status=args.status,
                limit=args.limit,
                pdf_mode=args.pdf_mode,
            )
            jobs_all.append((entry, year, job_key))

    jobs = (
        [item for item in jobs_all if item[2] not in completed_job_keys]
        if args.skip_completed
        else jobs_all
    )

    if args.max_jobs > 0:
        jobs = jobs[: args.max_jobs]

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    matrix_root = Path(args.matrix_run_dir).resolve() / f"run_{run_id}"
    job_logs_dir = matrix_root / "job_logs"
    jobs_jsonl_path = matrix_root / "jobs.jsonl"
    counts_jsonl_path = matrix_root / "court_year_counts.jsonl"
    matrix_summary_path = matrix_root / "summary.json"
    run_context_path = matrix_root / "run_context.json"

    run_context = {
        "run_id": run_id,
        "started_at_utc": _now_utc_iso(),
        "cache_path": str(cache_path),
        "states_filter": args.states or "all",
        "name": args.name,
        "year_start": args.year_start,
        "year_end": args.year_end,
        "year_step": args.year_step,
        "year_order": args.year_order,
        "status": args.status,
        "limit": args.limit,
        "detail_workers": args.detail_workers,
        "pdf_mode": args.pdf_mode,
        "refresh_existing": args.refresh_existing,
        "rank_proxies": args.rank_proxies,
        "proxy_rank_top_k": args.proxy_rank_top_k,
        "proxy_rank_workers": args.proxy_rank_workers,
        "proxy_rank_timeout": args.proxy_rank_timeout,
        "parallel_jobs": args.parallel_jobs,
        "progress_cache_path": str(progress_cache_path),
        "skip_completed": args.skip_completed,
        "output_dir": str(Path(args.output_dir).resolve()),
        "job_count_total": len(jobs_all),
        "job_count_skipped_from_cache": len(jobs_all) - len(jobs),
        "job_count": len(jobs),
        "distinct_paths": len(entries),
    }
    _write_json(run_context_path, run_context)

    print(f"[+] Matrix run started: {run_id}")
    print(f"[+] Distinct court paths: {len(entries)}")
    print(f"[+] Years: {args.year_start}-{args.year_end} step {args.year_step} ({len(years)} years)")
    print(f"[+] Total jobs (after skip cache): {len(jobs)}")
    if args.skip_completed:
        print(f"[+] Skipped via progress cache: {len(jobs_all) - len(jobs)}")
    print(f"[+] Parallel jobs: {args.parallel_jobs}")
    print(f"[+] Matrix logs: {matrix_root}")

    started = time.perf_counter()
    job_success = 0
    job_failure = 0
    benchmark_success_cases_total = 0
    benchmark_pdf_success_total = 0
    benchmark_pdf_failure_total = 0
    total_jobs = len(jobs)
    output_base_dir = Path(args.output_dir).resolve()

    def execute_job(index: int, entry: Dict[str, object], year: int, job_key: str):
        state_code = str(entry.get("state_code") or "")
        district_code = str(entry.get("district_code") or "")
        court_complex_code = str(entry.get("court_complex_code") or "")
        state_name = str(entry.get("state_name") or "")
        district_name = str(entry.get("district_name") or "")
        court_complex_name = str(entry.get("court_complex_name") or "")
        run_root = _run_dir_for_entry(output_base_dir, entry)
        before_runs = _list_run_dirs(run_root)

        cmd = [
            sys.executable,
            "benchmark_case_status.py",
            "--name",
            args.name,
            "--state",
            state_code,
            "--district",
            district_code,
            "--court-complex",
            court_complex_code,
            "--year",
            str(year),
            "--status",
            args.status,
            "--limit",
            str(args.limit),
            "--detail-workers",
            str(args.detail_workers),
            "--pdf-mode",
            args.pdf_mode,
            "--output-dir",
            args.output_dir,
        ]
        if args.refresh_existing:
            cmd.append("--refresh-existing")
        if args.captcha_api_key:
            cmd.extend(["--captcha-api-key", args.captcha_api_key])
        if args.proxy_file:
            cmd.extend(["--proxy-file", args.proxy_file])
        if args.rank_proxies:
            cmd.append("--rank-proxies")
            cmd.extend(["--proxy-rank-top-k", str(args.proxy_rank_top_k)])
            cmd.extend(["--proxy-rank-workers", str(args.proxy_rank_workers)])
            cmd.extend(["--proxy-rank-timeout", str(args.proxy_rank_timeout)])

        job_started = time.perf_counter()
        proc = subprocess.run(
            cmd,
            cwd=str(script_dir),
            capture_output=True,
            text=True,
        )
        job_elapsed = time.perf_counter() - job_started

        after_runs = _list_run_dirs(run_root)
        summary_path = _find_summary_path(run_root, before_runs, after_runs)
        summary_payload: Optional[Dict[str, object]] = None
        if summary_path and summary_path.exists():
            try:
                summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                summary_payload = None

        job_record: Dict[str, object] = {
            "timestamp_utc": _now_utc_iso(),
            "job_index": index,
            "job_total": total_jobs,
            "job_key": job_key,
            "state_code": state_code,
            "state_name": state_name,
            "district_code": district_code,
            "district_name": district_name,
            "court_complex_code": court_complex_code,
            "court_complex_name": court_complex_name,
            "year": year,
            "return_code": proc.returncode,
            "elapsed_sec": round(job_elapsed, 3),
            "summary_path": str(summary_path) if summary_path else None,
        }

        saved_case_count = 0
        pdf_success_count = 0
        pdf_failure_count = 0
        skipped_existing_cnr = 0
        skipped_duplicate_cnr = 0
        invalid_onclick = 0
        detail_failure_count = 0
        retry_case_count = 0
        if summary_payload:
            saved_case_count = int(summary_payload.get("json_output", {}).get("saved_case_count") or 0)
            pdf_success_count = int(summary_payload.get("pdfs", {}).get("success_count") or 0)
            pdf_failure_count = int(summary_payload.get("pdfs", {}).get("failure_count") or 0)
            dedupe = summary_payload.get("dedupe") or {}
            details = summary_payload.get("details") or {}
            retry = summary_payload.get("retry") or {}
            skipped_existing_cnr = int(dedupe.get("skipped_existing_cnr") or 0)
            skipped_duplicate_cnr = int(dedupe.get("skipped_duplicate_cnr") or 0)
            invalid_onclick = int(dedupe.get("invalid_onclick") or 0)
            detail_failure_count = int(details.get("failure_count") or 0)
            retry_case_count = int(retry.get("retry_case_count") or 0)
            job_record["benchmark"] = {
                "run_id": summary_payload.get("run_id"),
                "rows_selected": summary_payload.get("search", {}).get("rows_selected"),
                "saved_case_count": saved_case_count,
                "pdf_success_count": pdf_success_count,
                "pdf_failure_count": pdf_failure_count,
                "skipped_existing_cnr": skipped_existing_cnr,
                "skipped_duplicate_cnr": skipped_duplicate_cnr,
                "invalid_onclick": invalid_onclick,
                "detail_failure_count": detail_failure_count,
                "retry_case_count": retry_case_count,
                "total_elapsed_sec": summary_payload.get("total_elapsed_sec"),
            }

        log_suffix = (
            f"{index:05d}_{state_code}_{district_code}_{court_complex_code}_{year}"
        )
        should_save_logs = args.save_success_logs or proc.returncode != 0
        if should_save_logs:
            job_logs_dir.mkdir(parents=True, exist_ok=True)
            (job_logs_dir / f"{log_suffix}.stdout.log").write_text(proc.stdout or "", encoding="utf-8")
            (job_logs_dir / f"{log_suffix}.stderr.log").write_text(proc.stderr or "", encoding="utf-8")
            job_record["stdout_log"] = str((job_logs_dir / f"{log_suffix}.stdout.log").resolve())
            job_record["stderr_log"] = str((job_logs_dir / f"{log_suffix}.stderr.log").resolve())

        if proc.returncode != 0:
            job_record["error"] = f"benchmark_case_status.py exited with code {proc.returncode}"
        return job_record, saved_case_count, pdf_success_count, pdf_failure_count

    def process_job_result(
        index: int,
        job_record: Dict[str, object],
        saved_count: int,
        pdf_succ_count: int,
        pdf_fail_count: int,
    ) -> bool:
        nonlocal job_success, job_failure
        nonlocal benchmark_success_cases_total, benchmark_pdf_success_total, benchmark_pdf_failure_total

        benchmark_success_cases_total += saved_count
        benchmark_pdf_success_total += pdf_succ_count
        benchmark_pdf_failure_total += pdf_fail_count
        _append_jsonl(jobs_jsonl_path, job_record)

        benchmark = job_record.get("benchmark") or {}
        rows_selected = int(benchmark.get("rows_selected") or 0)
        saved_case_count = int(benchmark.get("saved_case_count") or 0)
        pdf_success_count = int(benchmark.get("pdf_success_count") or 0)
        pdf_failure_count = int(benchmark.get("pdf_failure_count") or 0)
        skipped_existing_cnr = int(benchmark.get("skipped_existing_cnr") or 0)
        skipped_duplicate_cnr = int(benchmark.get("skipped_duplicate_cnr") or 0)
        invalid_onclick = int(benchmark.get("invalid_onclick") or 0)
        detail_failure_count = int(benchmark.get("detail_failure_count") or 0)
        retry_case_count = int(benchmark.get("retry_case_count") or 0)
        dropped_from_rows = rows_selected - saved_case_count
        count_record = {
            "timestamp_utc": job_record.get("timestamp_utc"),
            "job_index": index,
            "job_key": job_record.get("job_key"),
            "state_code": job_record.get("state_code"),
            "state_name": job_record.get("state_name"),
            "district_code": job_record.get("district_code"),
            "district_name": job_record.get("district_name"),
            "court_complex_code": job_record.get("court_complex_code"),
            "court_complex_name": job_record.get("court_complex_name"),
            "year": job_record.get("year"),
            "return_code": job_record.get("return_code"),
            "rows_selected": rows_selected,
            "saved_case_count": saved_case_count,
            "dropped_from_rows": dropped_from_rows,
            "skipped_existing_cnr": skipped_existing_cnr,
            "skipped_duplicate_cnr": skipped_duplicate_cnr,
            "invalid_onclick": invalid_onclick,
            "detail_failure_count": detail_failure_count,
            "retry_case_count": retry_case_count,
            "pdf_success_count": pdf_success_count,
            "pdf_failure_count": pdf_failure_count,
            "elapsed_sec": job_record.get("elapsed_sec"),
            "summary_path": job_record.get("summary_path"),
            "error": job_record.get("error"),
        }
        _append_jsonl(counts_jsonl_path, count_record)

        if int(job_record.get("return_code", 1)) == 0:
            job_success += 1
            _append_jsonl(
                progress_cache_path,
                {
                    "timestamp_utc": _now_utc_iso(),
                    "job_key": job_record.get("job_key"),
                    "return_code": 0,
                    "state_code": job_record.get("state_code"),
                    "district_code": job_record.get("district_code"),
                    "court_complex_code": job_record.get("court_complex_code"),
                    "year": job_record.get("year"),
                    "rows_selected": rows_selected,
                    "saved_case_count": saved_case_count,
                    "pdf_success_count": pdf_success_count,
                    "pdf_failure_count": pdf_failure_count,
                    "summary_path": job_record.get("summary_path"),
                },
            )
            print(f"[+] Job {index} completed in {job_record['elapsed_sec']:.1f}s")
            print(
                "[=] Count | "
                f"{job_record.get('state_name')} | {job_record.get('district_name')} | "
                f"{job_record.get('court_complex_name')} | year={job_record.get('year')} | "
                f"rows={rows_selected} | saved={saved_case_count} | "
                f"drop={dropped_from_rows} | dup={skipped_duplicate_cnr} | "
                f"invalid={invalid_onclick} | detail_fail={detail_failure_count} | "
                f"pdf_ok={pdf_success_count} | pdf_fail={pdf_failure_count}"
            )
            return True

        job_failure += 1
        print(f"[!] Job {index} failed in {job_record['elapsed_sec']:.1f}s (code={job_record.get('return_code')})")
        print(
            "[=] Count | "
            f"{job_record.get('state_name')} | {job_record.get('district_name')} | "
            f"{job_record.get('court_complex_name')} | year={job_record.get('year')} | "
            "rows=0 | saved=0 | pdf_ok=0 | pdf_fail=0"
        )
        return False

    if args.parallel_jobs == 1:
        for index, (entry, year, job_key) in enumerate(jobs, start=1):
            state_name = str(entry.get("state_name") or "")
            district_name = str(entry.get("district_name") or "")
            court_complex_name = str(entry.get("court_complex_name") or "")
            print(
                f"[*] Job {index}/{total_jobs} | {state_name} | {district_name} | {court_complex_name} | year={year}"
            )
            job_record, saved_count, pdf_succ_count, pdf_fail_count = execute_job(
                index,
                entry,
                year,
                job_key,
            )
            ok = process_job_result(index, job_record, saved_count, pdf_succ_count, pdf_fail_count)
            if not ok and args.stop_on_error:
                break
    else:
        stop_submitting = False
        next_job_index = 1
        pending: Dict[concurrent.futures.Future, Tuple[int, Dict[str, object], int, str]] = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel_jobs) as executor:
            while (next_job_index <= total_jobs and not stop_submitting) or pending:
                while (
                    next_job_index <= total_jobs
                    and not stop_submitting
                    and len(pending) < args.parallel_jobs
                ):
                    entry, year, job_key = jobs[next_job_index - 1]
                    state_name = str(entry.get("state_name") or "")
                    district_name = str(entry.get("district_name") or "")
                    court_complex_name = str(entry.get("court_complex_name") or "")
                    print(
                        f"[*] Job {next_job_index}/{total_jobs} | {state_name} | {district_name} | {court_complex_name} | year={year}"
                    )
                    future = executor.submit(execute_job, next_job_index, entry, year, job_key)
                    pending[future] = (next_job_index, entry, year, job_key)
                    next_job_index += 1

                if not pending:
                    break

                done, _ = concurrent.futures.wait(
                    pending.keys(),
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    index, entry, year, job_key = pending.pop(future)
                    try:
                        job_record, saved_count, pdf_succ_count, pdf_fail_count = future.result()
                    except Exception as exc:
                        job_record = {
                            "timestamp_utc": _now_utc_iso(),
                            "job_index": index,
                            "job_total": total_jobs,
                            "job_key": job_key,
                            "state_code": str(entry.get("state_code") or ""),
                            "state_name": str(entry.get("state_name") or ""),
                            "district_code": str(entry.get("district_code") or ""),
                            "district_name": str(entry.get("district_name") or ""),
                            "court_complex_code": str(entry.get("court_complex_code") or ""),
                            "court_complex_name": str(entry.get("court_complex_name") or ""),
                            "year": year,
                            "return_code": 1,
                            "elapsed_sec": 0.0,
                            "summary_path": None,
                            "error": f"matrix runner exception: {exc}",
                        }
                        saved_count = 0
                        pdf_succ_count = 0
                        pdf_fail_count = 0

                    ok = process_job_result(index, job_record, saved_count, pdf_succ_count, pdf_fail_count)
                    if not ok and args.stop_on_error:
                        stop_submitting = True

    total_elapsed = time.perf_counter() - started
    summary = {
        "run_id": run_id,
        "finished_at_utc": _now_utc_iso(),
        "job_count_total": len(jobs_all),
        "job_count_skipped_from_cache": len(jobs_all) - len(jobs),
        "job_count": len(jobs),
        "job_success_count": job_success,
        "job_failure_count": job_failure,
        "benchmark_saved_case_total": benchmark_success_cases_total,
        "benchmark_pdf_success_total": benchmark_pdf_success_total,
        "benchmark_pdf_failure_total": benchmark_pdf_failure_total,
        "total_elapsed_sec": round(total_elapsed, 3),
        "avg_job_sec": round(total_elapsed / len(jobs), 3) if jobs else 0.0,
        "run_context_path": str(run_context_path),
        "jobs_jsonl_path": str(jobs_jsonl_path),
        "court_year_counts_path": str(counts_jsonl_path),
        "progress_cache_path": str(progress_cache_path),
    }
    _write_json(matrix_summary_path, summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
