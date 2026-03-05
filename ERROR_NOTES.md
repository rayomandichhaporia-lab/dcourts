# Error Notes

This note records the error messages observed during live runs of the district case-status benchmark in this folder.

Context used for the benchmark:

- State: Maharashtra (`1`)
- District: Pune (`25`)
- Court complex: Pune District and Sessions Court (`1010303`)
- Search: party name `Patil`
- Year: `2023`

## Search / Session Errors

### `405 Client Error: Security Page`

Seen on:

- `https://services.ecourts.gov.in/ecourtindia_v6/?p=casestatus/index...`
- some PDF report URLs under `https://services.ecourts.gov.in/ecourtindia_v6/reports/...`

Meaning:

- The site security layer or anti-abuse layer blocked the request.

Impact:

- fresh session initialization may fail
- PDF downloads may fail even when the PDF URL was resolved successfully

## Case Detail Errors

### `Empty data_list from viewHistory`

Seen from:

- `home/viewHistory`

Meaning:

- the row click action succeeded at the transport level, but the server returned no usable case-detail HTML in `data_list`

Impact:

- no structured case details can be extracted for that result

Observed in the 1000-case benchmark:

- `451` detail failures
- all of them were this exact error

## PDF Resolution / Download Errors

### `Missing pdf_url after resolution`

Meaning:

- the case detail page showed an order section, but the script could not turn the order link into a concrete downloadable PDF URL

Impact:

- no local PDF file for that order

Observed in the 1000-case benchmark:

- `42` failures

### `405 Client Error: Security Page for url: https://services.ecourts.gov.in/ecourtindia_v6/reports/...`

Meaning:

- the PDF report URL was real, but the site blocked the actual PDF fetch

Impact:

- PDF could not be downloaded locally

Observed in the 1000-case benchmark:

- multiple failures of this type across different report URLs

### `('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))`

Meaning:

- the remote server closed the connection during the PDF request

Impact:

- that PDF download attempt failed

Observed in the 1000-case benchmark:

- a small number of failures of this type

## Earlier High-Concurrency Test Error

### `400 Client Error: Bad Request for url: https://services.ecourts.gov.in/ecourtindia_v6/reports/...pdf`

Meaning:

- this was seen in an earlier benchmark version when PDF downloads were attempted from a different session than the one that resolved the case detail page

Impact:

- showed that PDF access is session-sensitive

Fix applied:

- PDF resolution and download were moved into the same per-case session flow

## Practical Takeaway

The most important live failure modes were:

1. `405 Security Page`
2. `Empty data_list from viewHistory`
3. `Missing pdf_url after resolution`
4. remote disconnects during PDF download

These errors increase sharply under higher request concurrency.

---

## Fix Applied: Proxy Rotation on 405 (2026-03-04)

### What we did

Added 1000-proxy rotation to both `district_casestatus_scraper.py` and `benchmark_case_status.py`.

Previously, proxies were only used for 2captcha API calls. The actual eCourts session (`self.session`) always used the bare machine IP, which was what the security layer was blocking.

**Changes to `district_casestatus_scraper.py`:**
- Added `proxy_getter` param to `__init__` — accepts a callable that returns the next proxy dict, allowing the benchmark to inject a shared pool
- Added `set_proxy()`, `_apply_next_proxy()`, `_rotate_proxy_and_reinit()` methods
- `init_session()` now retries up to 4 times on 405, rotating to a new proxy each attempt
- `_ajax_post()` detects 405, rotates proxy + reinitializes session + re-applies state/district context, then retries
- When a `proxy_file` is passed, the first proxy is immediately applied to the eCourts session

**Changes to `benchmark_case_status.py`:**
- Added module-level shared proxy pool (`_shared_proxy_pool`) with thread-safe round-robin via `_get_next_shared_proxy()`
- Each of the 16 worker threads gets its own proxy assigned at startup
- `_download_pdf_with_scraper()` detects 405, rotates proxy + reinitializes session, retries the PDF fetch
- Added `--proxy-file` CLI argument

**Proxy file format:** `ip:port:user:pass` (one per line)

### Benchmark results (2026-03-04)

Same query as the original benchmark:
- State: Maharashtra (`1`), District: Pune (`25`), Court complex: `1010303`
- Search: party name `Patil`, Year: `2023`
- Workers: 16, PDF mode: first, Limit: 1000

| Metric | Before (no proxies) | After (1000 proxies) |
|---|---|---|
| Detail successes | 549 / 1000 | **1000 / 1000** |
| Detail failures | 451 (all `Empty data_list`) | **0** |
| PDFs discovered | — | 241 |
| PDF successes | — | 237 / 241 |
| PDF failures | — | 4 (dead proxies, not 405) |
| Cases/sec | — | 6.8 |
| Total time | — | 166s |

Output saved to: `benchmark_output_1000_proxy/`

### Remaining PDF failures (4/241)

All 4 were proxy-level connection failures, not eCourts blocks:
- 3x `ProxyError: Remote end closed connection` — the proxy itself died mid-download
- 1x `IncompleteRead` — connection dropped partway through a large PDF

These are dead proxies in the pool, not a 405 issue. At 1.7% failure rate this is acceptable. Could be fixed by adding a retry-on-proxy-error, but low priority.

### PDF availability note

Only 241 of 1000 cases had any downloadable PDF order. The other 759 had no order section at all in their `viewHistory` response. This is a data reality — many district court cases (especially recent/pending ones) have no uploaded order documents.
