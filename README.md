# eCourt India District Courts Appraisal

This folder isolates the public district-court case-status flow from the larger repo.

Flow covered:

1. `casestatus/index`
2. `fillDistrict`
3. `fillcomplex`
4. CAPTCHA solve
5. `submitPartyName` or `submitCaseNo` or the related district search endpoints
6. Parse `historytable` into structured JSON rows

The main script is [district_casestatus_scraper.py](./district_casestatus_scraper.py).

## Included

- district-only session bootstrap against `https://services.ecourts.gov.in/ecourtindia_v6/?p=casestatus/index`
- state, district, and court-complex enumeration
- district search modes:
  - party name
  - case number
  - filing number
  - advocate
  - FIR
- parsed JSON output from the returned results table

## Install

```powershell
pip install -r requirements.txt
```

Optional local CAPTCHA model support also needs:

```powershell
pip install numpy Pillow PyYAML onnxruntime
```

If you are not using a local ONNX CAPTCHA model, set `TWOCAPTCHA_API_KEY` or pass `--captcha-api-key`.

## Examples

List states:

```powershell
python district_casestatus_scraper.py --list-states
```

List districts for a state:

```powershell
python district_casestatus_scraper.py --list-districts --state 1
```

List court complexes for a district:

```powershell
python district_casestatus_scraper.py --list-courts --state 1 --district 25
```

Party-name search:

```powershell
python district_casestatus_scraper.py --type party --name "John Doe" --state 1 --district 25 --court-complex 1010303 --year 2023
```

Case-number search:

```powershell
python district_casestatus_scraper.py --type case --case-type 1 --case-no 123 --year 2023 --state 1 --district 25 --court-complex 1010303
```

Write the full response to disk:

```powershell
python district_casestatus_scraper.py --type party --name "John Doe" --state 1 --district 25 --court-complex 1010303 --year 2023 -o output.json
```

## Notes

- This folder is standalone. It does not depend on `api/main.py`.
- The existing `scripts/ecourts_scraper.py` is left in place so the rest of the repo keeps working.
- No full all-state crawler was added here; this folder contains the exact district `casestatus` search flow only.
- Successful 2captcha solves are saved to `training_set/images/`, with labels appended to `training_set/captcha_labels.csv`.
