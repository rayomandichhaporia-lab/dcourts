"""
Standalone district-court scraper for the public eCourts case-status flow.

This script keeps the district-court path isolated:
casestatus/index -> fillDistrict -> fillcomplex -> captcha -> submit* -> parse results
"""

import argparse
import base64
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://services.ecourts.gov.in/ecourtindia_v6/"
ENTRY_URL = (
    BASE_URL
    + "?p=casestatus/index&app_token=8e3fded720c52b0d6257a5cdb8391934f44635bbf8f0b1e5c8ceb52735f66607"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
}

AJAX_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://services.ecourts.gov.in",
    "Referer": ENTRY_URL,
}

ENUM_RE = re.compile(r"(?<!\()(?<!\d)(\d{1,3})\)\s*")


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (text or "").strip().lower())
    return slug.strip("_")


class DistrictCaseStatusScraper:
    def __init__(
        self,
        captcha_api_key: Optional[str] = None,
        proxy_file: Optional[str] = None,
        use_local_model: bool = False,
        onnx_model_path: Optional[str] = None,
        min_local_conf: float = 0.55,
        proxy_getter=None,
    ) -> None:
        self.captcha_api_key = (captcha_api_key or os.getenv("TWOCAPTCHA_API_KEY", "")).strip()
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(HEADERS)
        self._no_proxy_session = requests.Session()
        self._no_proxy_session.trust_env = False
        self._no_proxy_session.headers.update(HEADERS)

        self.app_token: Optional[str] = None
        self.initialized = False
        self._session_state: Optional[str] = None
        self._session_district: Optional[str] = None

        self._proxies: List[Dict[str, str]] = []
        self._proxy_index = 0
        self._current_proxy: Optional[Dict[str, str]] = None
        self._proxy_getter = proxy_getter

        self.use_local_model = bool(use_local_model)
        self.min_local_conf = float(min_local_conf)
        self._ort_session = None
        self._ort_input_name = None
        self._ort_output_name = None
        self._captcha_meta = None
        self.training_set_dir = Path(__file__).resolve().parent / "training_set"
        self.training_images_dir = self.training_set_dir / "images"
        self.training_csv_path = self.training_set_dir / "captcha_labels.csv"
        self._ensure_training_set()

        if proxy_file:
            self._load_proxies(proxy_file)
            if self._proxies:
                self._current_proxy = self._get_next_proxy()
                self.session.proxies.update(self._current_proxy)

        if self.use_local_model:
            self._load_captcha_model(onnx_model_path)

    def _ensure_training_set(self) -> None:
        self.training_images_dir.mkdir(parents=True, exist_ok=True)
        if not self.training_csv_path.exists():
            with open(self.training_csv_path, "w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "timestamp_utc",
                        "image_filename",
                        "captcha_text",
                        "source",
                        "captcha_id",
                    ]
                )

    def _record_2captcha_training_sample(
        self,
        image_data: bytes,
        solution: str,
        captcha_id: str,
    ) -> Path:
        timestamp = datetime.now(timezone.utc)
        stem = timestamp.strftime("%Y%m%dT%H%M%S_%f")
        image_name = f"{stem}.png"
        image_path = self.training_images_dir / image_name
        image_path.write_bytes(image_data)

        with open(self.training_csv_path, "a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    timestamp.isoformat(),
                    image_name,
                    solution,
                    "2captcha",
                    captcha_id,
                ]
            )

        return image_path

    @staticmethod
    def _softmax(x, axis=-1):
        import numpy as np

        x = x - np.max(x, axis=axis, keepdims=True)
        ex = np.exp(x)
        return ex / np.sum(ex, axis=axis, keepdims=True)

    def _load_proxies(self, proxy_file: str) -> None:
        try:
            with open(proxy_file, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(":")
                    if len(parts) != 4:
                        continue
                    ip, port, user, password = parts
                    proxy_url = f"http://{user}:{password}@{ip}:{port}"
                    self._proxies.append({"http": proxy_url, "https": proxy_url})
            print(f"[+] Loaded {len(self._proxies)} proxies for 2captcha")
        except Exception as exc:
            print(f"[!] Failed to load proxies: {exc}")

    def _get_next_proxy(self) -> Optional[Dict[str, str]]:
        if not self._proxies:
            return None
        proxy = self._proxies[self._proxy_index]
        self._proxy_index = (self._proxy_index + 1) % len(self._proxies)
        return proxy

    def _apply_next_proxy(self) -> None:
        if self._proxy_getter:
            proxy = self._proxy_getter()
        elif self._proxies:
            proxy = self._get_next_proxy()
        else:
            proxy = None
        self._current_proxy = proxy
        self.session.cookies.clear()
        self.session.proxies.clear()
        if proxy:
            self.session.proxies.update(proxy)
        self.initialized = False
        self._session_state = None
        self._session_district = None

    def set_proxy(self, proxy: Optional[Dict[str, str]]) -> None:
        self._current_proxy = proxy
        self.session.proxies.clear()
        if proxy:
            self.session.proxies.update(proxy)

    def _rotate_proxy_and_reinit(self) -> None:
        self._apply_next_proxy()
        self.init_session()

    def _load_captcha_model(self, model_path: Optional[str]) -> None:
        try:
            import onnxruntime as ort
            import yaml
        except Exception:
            print("[!] Local CAPTCHA dependencies missing; using 2captcha if configured.")
            self.use_local_model = False
            return

        root = Path(__file__).resolve().parent
        candidates = []
        if model_path:
            candidates.append(Path(model_path))
        candidates.extend(
            [
                root / "models" / "captcha_seq6.onnx",
                root / "captcha_fixed.onnx",
                root / "captcha_ocr_fixed6" / "outputs" / "captcha_fixed.onnx",
            ]
        )

        selected = next((path for path in candidates if path.exists()), None)
        if selected is None:
            print("[!] Local CAPTCHA model not found; using 2captcha if configured.")
            self.use_local_model = False
            return

        meta_path = selected.with_suffix(".yaml")
        if not meta_path.exists():
            print(f"[!] CAPTCHA model metadata not found: {meta_path}")
            self.use_local_model = False
            return

        try:
            with open(meta_path, "r", encoding="utf-8") as handle:
                meta = yaml.safe_load(handle) or {}

            if {"charset", "img_width", "img_height"} <= set(meta.keys()):
                parsed_meta = {
                    "schema": meta.get("schema", "sequence"),
                    "charset": meta["charset"],
                    "img_width": int(meta["img_width"]),
                    "img_height": int(meta["img_height"]),
                }
            else:
                data_meta = meta.get("data") or {}
                model_meta = meta.get("model") or {}
                parsed_meta = {
                    "schema": model_meta.get("type", "sequence"),
                    "charset": data_meta["charset"],
                    "img_width": int(data_meta["img_width"]),
                    "img_height": int(data_meta["img_height"]),
                }

            parsed_meta["idx_to_char"] = {
                idx: char for idx, char in enumerate(parsed_meta["charset"])
            }
            self._captcha_meta = parsed_meta
            self._ort_session = ort.InferenceSession(
                str(selected),
                providers=["CPUExecutionProvider"],
            )
            self._ort_input_name = self._ort_session.get_inputs()[0].name
            self._ort_output_name = self._ort_session.get_outputs()[0].name
            print(f"[+] Loaded local CAPTCHA model: {selected}")
        except Exception as exc:
            print(f"[!] Failed to load local CAPTCHA model: {exc}")
            self.use_local_model = False
            self._ort_session = None
            self._captcha_meta = None

    def _solve_captcha_local(self, image_data: bytes) -> Optional[Tuple[str, float]]:
        if not self.use_local_model or self._ort_session is None or self._captcha_meta is None:
            return None

        try:
            import numpy as np
            from PIL import Image

            img = Image.open(BytesIO(image_data)).convert("L")
            img = img.resize(
                (self._captcha_meta["img_width"], self._captcha_meta["img_height"])
            )
            arr = np.array(img, dtype=np.float32)
            arr = (arr / 255.0 - 0.5) / 0.5
            batch = arr[np.newaxis, np.newaxis, :, :]

            outputs = self._ort_session.run(
                [self._ort_output_name],
                {self._ort_input_name: batch},
            )[0]

            pred = np.argmax(outputs, axis=2)[0]
            chars = [self._captcha_meta["idx_to_char"][int(token)] for token in pred]
            solution = "".join(chars)
            solution = "".join(char for char in solution.lower() if char.isalnum())

            probs = self._softmax(outputs[0], axis=1)
            confidence = float(np.mean(np.max(probs, axis=1)))

            if confidence < self.min_local_conf:
                print(
                    "[!] Local model prediction rejected: "
                    f"{solution} ({confidence:.3f} < {self.min_local_conf})"
                )
                return None

            return solution, confidence
        except Exception as exc:
            print(f"[!] Local model inference failed: {exc}")
            return None

    def init_session(self) -> None:
        for attempt in range(4):
            print("[*] Initializing case-status session...")
            response = self.session.get(ENTRY_URL, timeout=30)
            if response.status_code == 405:
                print(f"[!] 405 Security Page on init (attempt {attempt + 1}/4), rotating proxy...")
                self._apply_next_proxy()
                continue
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            token_input = soup.find("input", {"id": "app_token"})
            if token_input:
                self.app_token = token_input.get("value", "")
                print(f"[+] Got app_token: {self.app_token[:20]}...")

            print(f"[+] Session ID: {self.session.cookies.get('SERVICES_SESSID', 'N/A')}")
            self.initialized = True
            return
        raise RuntimeError("Session init failed: 405 Security Page after 4 proxy rotations")

    def _ajax_post(self, endpoint: str, post_data: str, referer: Optional[str] = None) -> dict:
        if not self.initialized:
            self.init_session()

        url = BASE_URL + "?p=" + endpoint
        data = post_data + f"&ajax_req=true&app_token={self.app_token}"
        headers = dict(AJAX_HEADERS)
        if referer:
            headers["Referer"] = referer

        last_exc = None
        for attempt in range(3):
            try:
                response = self.session.post(url, data=data, headers=headers, timeout=30)
                if response.status_code == 405:
                    print(f"[!] 405 Security Page on {endpoint} (attempt {attempt + 1}/3), rotating proxy...")
                    prev_state = self._session_state
                    prev_dist = self._session_district
                    self._rotate_proxy_and_reinit()
                    if prev_state:
                        self._ensure_session_state(prev_state, prev_dist or "")
                    continue
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as exc:
                last_exc = exc
                if attempt < 2:
                    # Keep retries fast; rotate only after repeated transport failures.
                    wait_sec = 0.75 if attempt == 0 else 1.5
                    print(f"[!] Transport error on {endpoint}, retrying in {wait_sec:.2f}s...")
                    if attempt >= 1:
                        prev_state = self._session_state
                        prev_dist = self._session_district
                        try:
                            self._rotate_proxy_and_reinit()
                            if prev_state:
                                self._ensure_session_state(prev_state, prev_dist or "")
                        except Exception as rotate_exc:
                            print(f"[!] Transport retry proxy-rotate failed: {rotate_exc}")
                    time.sleep(wait_sec)
                else:
                    raise
        else:
            raise last_exc or RuntimeError("AJAX request failed")

        text = response.text
        parts = text.split("#####")
        if len(parts) > 1 and parts[1]:
            self.app_token = parts[1].strip()

        body = parts[0].strip()
        try:
            result = json.loads(body)
            if "app_token" in result:
                self.app_token = result["app_token"]
            return result
        except (json.JSONDecodeError, ValueError):
            pass

        if body:
            return {"html": body, "status": True}
        return {"status": False, "error": "Empty response"}

    def _ensure_session_state(self, state_code: str, dist_code: str) -> None:
        if self._session_state != state_code:
            print(f"[*] Setting session state={state_code}...")
            self._ajax_post("casestatus/fillDistrict", f"state_code={state_code}")
            self._session_state = state_code
            self._session_district = None

        if self._session_district != dist_code:
            print(f"[*] Setting session district={dist_code}...")
            self._ajax_post(
                "casestatus/fillcomplex",
                f"state_code={state_code}&dist_code={dist_code}",
            )
            self._session_district = dist_code

    def get_captcha_image(self, max_retries: int = 4) -> bytes:
        if not self.initialized:
            self.init_session()

        captcha_url = BASE_URL + "vendor/securimage/securimage_show.php"
        last_error: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.get(captcha_url, headers={"Referer": ENTRY_URL}, timeout=30)
            except (
                requests.exceptions.ProxyError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ContentDecodingError,
            ) as exc:
                last_error = exc
                if attempt < max_retries:
                    print(
                        f"[!] CAPTCHA image transport error (attempt {attempt}/{max_retries}), "
                        "rotating proxy and retrying..."
                    )
                    try:
                        self._rotate_proxy_and_reinit()
                    except Exception as rotate_exc:
                        print(f"[!] CAPTCHA image rotate/reinit failed: {rotate_exc}")
                    time.sleep(min(0.5 * attempt, 2.0))
                    continue
                raise

            if response.status_code == 405:
                last_error = RuntimeError("405 Security Page while fetching CAPTCHA image")
                if attempt < max_retries:
                    print(
                        f"[!] 405 Security Page on CAPTCHA image (attempt {attempt}/{max_retries}), "
                        "rotating proxy and retrying..."
                    )
                    try:
                        self._rotate_proxy_and_reinit()
                    except Exception as rotate_exc:
                        print(f"[!] CAPTCHA image rotate/reinit failed: {rotate_exc}")
                    time.sleep(min(0.5 * attempt, 2.0))
                    continue
                response.raise_for_status()

            try:
                response.raise_for_status()
            except requests.RequestException as exc:
                last_error = exc
                if attempt < max_retries and response.status_code >= 500:
                    print(
                        f"[!] CAPTCHA image HTTP {response.status_code} "
                        f"(attempt {attempt}/{max_retries}), rotating proxy and retrying..."
                    )
                    try:
                        self._rotate_proxy_and_reinit()
                    except Exception as rotate_exc:
                        print(f"[!] CAPTCHA image rotate/reinit failed: {rotate_exc}")
                    time.sleep(min(0.5 * attempt, 2.0))
                    continue
                raise

            if not response.content:
                last_error = RuntimeError("Empty CAPTCHA image response")
                if attempt < max_retries:
                    print(
                        f"[!] Empty CAPTCHA image response (attempt {attempt}/{max_retries}), "
                        "rotating proxy and retrying..."
                    )
                    try:
                        self._rotate_proxy_and_reinit()
                    except Exception as rotate_exc:
                        print(f"[!] CAPTCHA image rotate/reinit failed: {rotate_exc}")
                    time.sleep(min(0.5 * attempt, 2.0))
                    continue
                break

            return response.content

        raise RuntimeError(f"Failed to fetch CAPTCHA image after {max_retries} attempts: {last_error}")

    def solve_captcha(self, max_retries: int = 3, allow_2captcha: bool = True) -> str:
        for attempt in range(max_retries):
            try:
                image_data = self.get_captcha_image()
            except Exception as exc:
                print(f"[-] CAPTCHA image fetch failed on attempt {attempt + 1}: {exc}")
                if attempt + 1 < max_retries:
                    continue
                raise

            if self.use_local_model:
                print(f"[*] Solving CAPTCHA locally ({attempt + 1}/{max_retries})...")
                result = self._solve_captcha_local(image_data)
                if result:
                    solution, confidence = result
                    print(f"[+] Local CAPTCHA solution: {solution} ({confidence:.3f})")
                    return solution
                if not allow_2captcha:
                    continue
                print("[!] Local CAPTCHA solve failed, falling back to 2captcha.")

            if not allow_2captcha:
                continue

            if not self.captcha_api_key:
                raise RuntimeError(
                    "CAPTCHA solve requires either a local ONNX model or TWOCAPTCHA_API_KEY."
                )

            proxy = self._get_next_proxy()
            proxy_info = " direct" if not proxy else f" via proxy {self._proxy_index}/{len(self._proxies)}"
            print(f"[*] Solving CAPTCHA with 2captcha ({attempt + 1}/{max_retries}){proxy_info}...")

            image_b64 = base64.b64encode(image_data).decode("utf-8")
            session = requests.Session()
            session.trust_env = False

            try:
                submit_response = session.post(
                    "https://2captcha.com/in.php",
                    data={
                        "key": self.captcha_api_key,
                        "method": "base64",
                        "body": image_b64,
                        "json": 1,
                        "min_len": 4,
                        "max_len": 6,
                        "regsense": 0,
                    },
                    proxies=proxy,
                    timeout=30,
                )
                submit_result = submit_response.json()
            except Exception as exc:
                print(f"[-] 2captcha submit failed{proxy_info}: {exc}")
                if proxy:
                    print("[*] Retrying 2captcha submit without proxy...")
                    try:
                        submit_response = self._no_proxy_session.post(
                            "https://2captcha.com/in.php",
                            data={
                                "key": self.captcha_api_key,
                                "method": "base64",
                                "body": image_b64,
                                "json": 1,
                                "min_len": 4,
                                "max_len": 6,
                                "regsense": 0,
                            },
                            timeout=30,
                        )
                        submit_result = submit_response.json()
                    except Exception as retry_exc:
                        print(f"[-] Direct 2captcha submit failed: {retry_exc}")
                        continue
                else:
                    continue

            if submit_result.get("status") != 1:
                print(f"[-] 2captcha submit error: {submit_result}")
                continue

            captcha_id = submit_result["request"]
            print(f"[*] Waiting for 2captcha solution (id={captcha_id})...")

            for poll_attempt in range(30):
                time.sleep(5)
                try:
                    result_response = session.get(
                        "https://2captcha.com/res.php",
                        params={
                            "key": self.captcha_api_key,
                            "action": "get",
                            "id": captcha_id,
                            "json": 1,
                        },
                        proxies=proxy,
                        timeout=30,
                    )
                    result = result_response.json()
                except Exception as exc:
                    print(f"[-] 2captcha poll failed{proxy_info}: {exc}")
                    if proxy and poll_attempt < 29:
                        continue
                    break

                if result.get("status") == 1:
                    solution = result["request"]
                    print(f"[+] CAPTCHA solved: {solution}")
                    image_path = self._record_2captcha_training_sample(
                        image_data=image_data,
                        solution=solution,
                        captcha_id=captcha_id,
                    )
                    print(f"[+] Training sample saved: {image_path.name}")
                    return solution

                if result.get("request") != "CAPCHA_NOT_READY":
                    print(f"[-] 2captcha error: {result}")
                    break

            print(f"[-] CAPTCHA solve failed on attempt {attempt + 1}")

        raise RuntimeError("Failed to solve CAPTCHA after all retries.")

    def _is_captcha_error(self, result: dict) -> bool:
        err = (result.get("errormsg") or "").lower()
        if "captcha" in err or "security code" in err:
            return True
        history = (result.get("historytable") or "").lower()
        if "captcha" in history or "security code" in history:
            return True
        return False

    def _submit_search_with_captcha(
        self,
        endpoint: str,
        post_data_without_captcha: str,
        captcha_field: str,
        retries: int = 3,
    ) -> dict:
        result = {}
        for attempt in range(retries):
            try:
                captcha_code = self.solve_captcha()
            except Exception as exc:
                print(f"[!] CAPTCHA solve failed on attempt {attempt + 1}/{retries}: {exc}")
                if attempt + 1 < retries:
                    try:
                        self._rotate_proxy_and_reinit()
                    except Exception as rotate_exc:
                        print(f"[!] CAPTCHA solve rotate/reinit failed: {rotate_exc}")
                    continue
                raise

            try:
                result = self._ajax_post(
                    endpoint,
                    f"{post_data_without_captcha}&{captcha_field}={captcha_code}",
                )
            except Exception as exc:
                print(f"[!] Search submit failed on attempt {attempt + 1}/{retries}: {exc}")
                if attempt + 1 < retries:
                    try:
                        self._rotate_proxy_and_reinit()
                    except Exception as rotate_exc:
                        print(f"[!] Search submit rotate/reinit failed: {rotate_exc}")
                    continue
                raise

            if not self._is_captcha_error(result):
                return result
            print(f"[!] CAPTCHA rejected on attempt {attempt + 1}, retrying...")
            if attempt + 1 < retries:
                try:
                    self._rotate_proxy_and_reinit()
                except Exception as rotate_exc:
                    print(f"[!] CAPTCHA reject rotate/reinit failed: {rotate_exc}")
        return result

    def list_states(self) -> Dict[str, str]:
        if not self.initialized:
            self.init_session()
        response = self.session.get(ENTRY_URL, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        select = soup.find("select", {"id": "sess_state_code"})
        states: Dict[str, str] = {}
        if select:
            for option in select.find_all("option"):
                value = option.get("value", "")
                if value and value != "0":
                    states[value] = option.get_text(strip=True)
        return states

    def list_districts(self, state_code: str) -> Dict[str, str]:
        result = self._ajax_post("casestatus/fillDistrict", f"state_code={state_code}")
        districts: Dict[str, str] = {}
        if result.get("status") == 1 and result.get("dist_list"):
            soup = BeautifulSoup(result["dist_list"], "html.parser")
            for option in soup.find_all("option"):
                value = option.get("value", "")
                if value:
                    districts[value] = option.get_text(strip=True)
        return districts

    def list_court_complexes(self, state_code: str, dist_code: str) -> Dict[str, str]:
        result = self._ajax_post(
            "casestatus/fillcomplex",
            f"state_code={state_code}&dist_code={dist_code}",
        )
        courts: Dict[str, str] = {}
        if result.get("status") == 1 and result.get("complex_list"):
            soup = BeautifulSoup(result["complex_list"], "html.parser")
            for option in soup.find_all("option"):
                value = option.get("value", "")
                if value:
                    courts[value] = option.get_text(strip=True)
        return courts

    @staticmethod
    def _extract_complex_code(court_complex_code: str) -> str:
        return court_complex_code.split("@", 1)[0] if "@" in court_complex_code else court_complex_code

    def search_by_party_name(
        self,
        name: str,
        state_code: str,
        dist_code: str,
        court_complex_code: str,
        case_status: str = "Both",
        year: str = "",
    ) -> dict:
        complex_code = self._extract_complex_code(court_complex_code)
        self._ensure_session_state(state_code, dist_code)
        post_data = (
            f"petres_name={requests.utils.quote(name, safe='')}"
            f"&rgyearP={year}"
            f"&case_status={case_status}"
            f"&state_code={state_code}"
            f"&dist_code={dist_code}"
            f"&court_complex_code={complex_code}"
            f"&est_code="
        )
        print(f"[*] Searching party name: {name}")
        result = self._submit_search_with_captcha(
            "casestatus/submitPartyName",
            post_data,
            "fcaptcha_code",
        )
        return self._process_search_result(result)

    def search_by_case_number(
        self,
        case_type: str,
        case_no: str,
        year: str,
        state_code: str,
        dist_code: str,
        court_complex_code: str,
    ) -> dict:
        complex_code = self._extract_complex_code(court_complex_code)
        self._ensure_session_state(state_code, dist_code)
        post_data = (
            f"case_type={case_type}"
            f"&search_case_no={case_no}"
            f"&rgyear={year}"
            f"&state_code={state_code}"
            f"&dist_code={dist_code}"
            f"&court_complex_code={complex_code}"
            f"&est_code="
        )
        print(f"[*] Searching case number: {case_type}/{case_no}/{year}")
        result = self._submit_search_with_captcha(
            "casestatus/submitCaseNo",
            post_data,
            "case_captcha_code",
        )
        return self._process_search_result(result)

    def search_by_filing_number(
        self,
        filing_no: str,
        year: str,
        state_code: str,
        dist_code: str,
        court_complex_code: str,
    ) -> dict:
        complex_code = self._extract_complex_code(court_complex_code)
        self._ensure_session_state(state_code, dist_code)
        post_data = (
            f"filing_no={filing_no}"
            f"&filyear={year}"
            f"&state_code={state_code}"
            f"&dist_code={dist_code}"
            f"&court_complex_code={complex_code}"
            f"&est_code="
        )
        print(f"[*] Searching filing number: {filing_no}/{year}")
        result = self._submit_search_with_captcha(
            "casestatus/submitFillingNo",
            post_data,
            "file_captcha_code",
        )
        return self._process_search_result(result)

    def search_by_advocate(
        self,
        advocate_name: str,
        state_code: str,
        dist_code: str,
        court_complex_code: str,
        case_status: str = "Both",
    ) -> dict:
        complex_code = self._extract_complex_code(court_complex_code)
        self._ensure_session_state(state_code, dist_code)
        post_data = (
            f"advocate_name={requests.utils.quote(advocate_name, safe='')}"
            f"&case_status={case_status}"
            f"&state_code={state_code}"
            f"&dist_code={dist_code}"
            f"&court_complex_code={complex_code}"
            f"&est_code="
        )
        print(f"[*] Searching advocate: {advocate_name}")
        result = self._submit_search_with_captcha(
            "casestatus/submitAdvName",
            post_data,
            "adv_captcha_code",
        )
        return self._process_search_result(result)

    def search_by_fir(
        self,
        police_station: str,
        fir_no: str,
        year: str,
        state_code: str,
        dist_code: str,
        court_complex_code: str,
        case_status: str = "Both",
    ) -> dict:
        complex_code = self._extract_complex_code(court_complex_code)
        self._ensure_session_state(state_code, dist_code)
        post_data = (
            f"police_st_code={police_station}"
            f"&fir_no={fir_no}"
            f"&firyear={year}"
            f"&case_status={case_status}"
            f"&state_code={state_code}"
            f"&dist_code={dist_code}"
            f"&court_complex_code={complex_code}"
        )
        print(f"[*] Searching FIR: {fir_no}/{year}")
        result = self._submit_search_with_captcha(
            "casestatus/submitFirNo",
            post_data,
            "fir_captcha_code",
        )
        return self._process_search_result(result)

    def _process_search_result(self, result: dict) -> dict:
        if result.get("errormsg"):
            print(f"[-] Server error: {result['errormsg']}")
            return result

        html_key, html = self._extract_results_html(result)
        if result.get("status") == 1 and html:
            headers, cases = self._parse_case_table(html)
            result["case_headers"] = headers
            result["cases"] = cases
            result["results_html_key"] = html_key
            print(f"[+] Found {len(cases)} case(s)")

        return result

    def _extract_results_html(self, result: dict) -> Tuple[Optional[str], str]:
        preferred_keys = [
            "historytable",
            "party_data",
            "case_data",
            "filing_data",
            "adv_data",
            "fir_data",
            "html",
        ]
        for key in preferred_keys:
            value = result.get(key)
            if isinstance(value, str) and "<table" in value.lower():
                return key, value

        for key, value in result.items():
            if isinstance(value, str) and "<table" in value.lower():
                return key, value

        return None, ""

    def _parse_case_table(self, html: str) -> Tuple[List[str], List[dict]]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.find_all("tr")

        headers: List[str] = []
        cases: List[dict] = []

        for row in rows:
            th_cells = row.find_all("th")
            if th_cells and not headers:
                headers = [cell.get_text(" ", strip=True) for cell in th_cells]
                continue

            td_cells = row.find_all("td")
            if not td_cells:
                continue

            record: Dict[str, object] = {"cells": []}
            for idx, cell in enumerate(td_cells):
                text = cell.get_text(" ", strip=True)
                link = cell.find("a")
                cast_cells = record["cells"]
                if isinstance(cast_cells, list):
                    cast_cells.append(text)
                record[f"col_{idx}"] = text

                if headers and idx < len(headers):
                    header_key = _slugify(headers[idx]) or f"field_{idx}"
                    if header_key in record:
                        header_key = f"{header_key}_{idx}"
                    record[header_key] = text

                if link:
                    href = link.get("href")
                    onclick = link.get("onclick")
                    if href:
                        record[f"col_{idx}_href"] = href
                    if onclick:
                        record[f"col_{idx}_onclick"] = onclick

            if record.get("cells"):
                cases.append(record)

        return headers, cases


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Standalone eCourts district case-status scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python district_casestatus_scraper.py --list-states\n"
            "  python district_casestatus_scraper.py --list-districts --state 1\n"
            "  python district_casestatus_scraper.py --list-courts --state 1 --district 25\n"
            "  python district_casestatus_scraper.py --type party --name \"John Doe\" "
            "--state 1 --district 25 --court-complex 1010303 --year 2023\n"
            "  python district_casestatus_scraper.py --type case --case-type 1 --case-no 123 "
            "--year 2023 --state 1 --district 25 --court-complex 1010303\n"
        ),
    )

    parser.add_argument("--list-states", action="store_true", help="List available states")
    parser.add_argument("--list-districts", action="store_true", help="List districts for a state")
    parser.add_argument("--list-courts", action="store_true", help="List court complexes")

    parser.add_argument(
        "--type",
        choices=["party", "case", "filing", "advocate", "fir"],
        help="Search type",
    )
    parser.add_argument("--state", type=str, help="State code")
    parser.add_argument("--district", type=str, help="District code")
    parser.add_argument(
        "--court-complex",
        type=str,
        help="Court complex code or the raw dropdown value",
    )

    parser.add_argument("--name", type=str, help="Party name to search")
    parser.add_argument("--status", choices=["Pending", "Disposed", "Both"], default="Both")
    parser.add_argument("--year", type=str, default="", help="Registration year")
    parser.add_argument("--case-type", type=str, help="Case type code")
    parser.add_argument("--case-no", type=str, help="Case number")
    parser.add_argument("--filing-no", type=str, help="Filing number")
    parser.add_argument("--advocate", type=str, help="Advocate name")
    parser.add_argument("--police-station", type=str, help="Police station code")
    parser.add_argument("--fir-no", type=str, help="FIR number")

    parser.add_argument("--captcha-api-key", type=str, help="Override TWOCAPTCHA_API_KEY")
    parser.add_argument("--proxy-file", type=str, help="Proxy list for 2captcha")
    parser.add_argument(
        "--use-local-model",
        action="store_true",
        help="Enable local ONNX CAPTCHA solving (disabled by default)",
    )
    parser.add_argument(
        "--no-local-model",
        action="store_true",
        help="Disable local ONNX CAPTCHA solving (deprecated; already disabled by default)",
    )
    parser.add_argument("--onnx-model-path", type=str, help="Path to a local CAPTCHA ONNX model")
    parser.add_argument("--min-local-conf", type=float, default=0.55)
    parser.add_argument("--output", "-o", type=str, help="Write full JSON result to a file")
    return parser


def _print_mapping(title: str, items: Dict[str, str]) -> None:
    print(f"\n=== {title} ===")
    for code, name in sorted(items.items(), key=lambda entry: entry[1]):
        print(f"  {code}: {name}")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    use_local_model = bool(args.use_local_model)
    if args.no_local_model:
        use_local_model = False

    scraper = DistrictCaseStatusScraper(
        captcha_api_key=args.captcha_api_key,
        proxy_file=args.proxy_file,
        use_local_model=use_local_model,
        onnx_model_path=args.onnx_model_path,
        min_local_conf=args.min_local_conf,
    )
    scraper.init_session()

    if args.list_states:
        _print_mapping("States", scraper.list_states())
        return

    if args.list_districts:
        if not args.state:
            parser.error("--state is required for --list-districts")
        _print_mapping(f"Districts (State {args.state})", scraper.list_districts(args.state))
        return

    if args.list_courts:
        if not args.state or not args.district:
            parser.error("--state and --district are required for --list-courts")
        title = f"Court Complexes (State {args.state}, District {args.district})"
        _print_mapping(title, scraper.list_court_complexes(args.state, args.district))
        return

    if not args.type:
        parser.error("--type is required unless using a list mode")

    if args.type == "party":
        if not all([args.name, args.state, args.district, args.court_complex, args.year]):
            parser.error(
                "--name, --state, --district, --court-complex, and --year are required for party search"
            )
        result = scraper.search_by_party_name(
            args.name,
            args.state,
            args.district,
            args.court_complex,
            case_status=args.status,
            year=args.year,
        )
    elif args.type == "case":
        if not all([args.case_type, args.case_no, args.year, args.state, args.district, args.court_complex]):
            parser.error(
                "--case-type, --case-no, --year, --state, --district, and --court-complex are required for case search"
            )
        result = scraper.search_by_case_number(
            args.case_type,
            args.case_no,
            args.year,
            args.state,
            args.district,
            args.court_complex,
        )
    elif args.type == "filing":
        if not all([args.filing_no, args.year, args.state, args.district, args.court_complex]):
            parser.error(
                "--filing-no, --year, --state, --district, and --court-complex are required for filing search"
            )
        result = scraper.search_by_filing_number(
            args.filing_no,
            args.year,
            args.state,
            args.district,
            args.court_complex,
        )
    elif args.type == "advocate":
        if not all([args.advocate, args.state, args.district, args.court_complex]):
            parser.error(
                "--advocate, --state, --district, and --court-complex are required for advocate search"
            )
        result = scraper.search_by_advocate(
            args.advocate,
            args.state,
            args.district,
            args.court_complex,
            case_status=args.status,
        )
    else:
        if not all(
            [
                args.police_station,
                args.fir_no,
                args.year,
                args.state,
                args.district,
                args.court_complex,
            ]
        ):
            parser.error(
                "--police-station, --fir-no, --year, --state, --district, and --court-complex are required for FIR search"
            )
        result = scraper.search_by_fir(
            args.police_station,
            args.fir_no,
            args.year,
            args.state,
            args.district,
            args.court_complex,
            case_status=args.status,
        )

    output = json.dumps(result, indent=2, ensure_ascii=False)
    print("\n=== Result ===")
    safe_output = output.encode("cp1252", "replace").decode("cp1252")
    print(safe_output[:5000])
    if len(output) > 5000:
        print(f"\n... (truncated, total {len(output)} chars)")

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output, encoding="utf-8")
        safe_path = str(output_path).encode("cp1252", "replace").decode("cp1252")
        print(f"\n[+] Full result saved to {safe_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
