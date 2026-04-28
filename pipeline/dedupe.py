from typing import Iterable, List

from pipeline.models import VideoRecord


def dedupe_videos(records: Iterable[VideoRecord]) -> List[VideoRecord]:
    seen = set()
    out = []
    for row in records:
        key = (row.platform, row.video_link)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def reduce_unique_profiles(records: Iterable[VideoRecord]) -> List[VideoRecord]:
    # Prefer latest date when available.
    sorted_rows = sorted(
        records,
        key=lambda r: (r.platform, r.profile_handle or "~", r.posted_date or "", r.video_link),
        reverse=True,
    )

    kept = []
    seen_profiles = set()
    for row in sorted_rows:
        profile_key = (row.platform, row.profile_handle or row.video_link)
        if profile_key in seen_profiles:
            continue
        seen_profiles.add(profile_key)
        kept.append(row)
    return kept
