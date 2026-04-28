import os
from datetime import datetime
from typing import Iterable, List

import pandas as pd

from pipeline.models import VideoRecord


def _to_rows(records: Iterable[VideoRecord]) -> List[dict]:
    return [
        {
            "platform": r.platform,
            "hashtag": r.hashtag,
            "video_link": r.video_link,
            "posted_date": r.posted_date,
            "profile_handle": r.profile_handle,
        }
        for r in records
    ]


def _to_date_only(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Handles values like "2026-04-24T09:27:19.000Z" and "2026-04-24 09:27:19"
    if "T" in text:
        return text.split("T", 1)[0]
    if " " in text:
        return text.split(" ", 1)[0]
    return text


def write_results(records: Iterable[VideoRecord], filename_input: str = "") -> str:
    os.makedirs("Results", exist_ok=True)

    if filename_input.strip():
        filename = filename_input.strip().replace(" ", "_")
        if not filename.endswith(".csv"):
            filename += ".csv"
    else:
        filename = "keywordscraper.csv"

    path = os.path.join("Results", filename)
    rows = _to_rows(records)
    df = pd.DataFrame(rows, columns=["platform", "hashtag", "video_link", "posted_date", "profile_handle"])
    df["posted_date"] = df["posted_date"].map(_to_date_only)

    platform_order = {"Instagram": 0, "TikTok": 1}
    df["__order"] = df["platform"].map(platform_order).fillna(99)
    df = df.sort_values(by=["__order", "hashtag", "posted_date"], ascending=[True, True, False]).drop(columns=["__order"])
    df.to_csv(path, index=False)
    return path


def make_run_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
