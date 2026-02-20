import hashlib
import re
from typing import Iterable

import pandas as pd
import requests


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
)


def fetch_html(url: str, timeout: int = 20) -> str:
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding
    return resp.text


def read_html_tables(html: str) -> list[pd.DataFrame]:
    return pd.read_html(html)


def norm_col(value: object) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"<[^>]+>", "", text).strip().lower()
    text = re.sub(r"[\s_/.-]+", "", text)
    return text


def pick_table_by_keywords(tables: Iterable[pd.DataFrame], required_keywords: list[str]) -> pd.DataFrame | None:
    required = [norm_col(x) for x in required_keywords]
    for df in tables:
        cols = [norm_col(c) for c in df.columns]
        joined = "|".join(cols)
        if all(any(key in c for c in cols) or key in joined for key in required):
            return df
    return None


def stable_player_id(name: str, birth_date: str) -> str:
    base = f"{name.strip()}|{birth_date.strip()}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"statiz_{digest}"
