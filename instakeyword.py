import asyncio
import re
import sys
from collections import Counter

from collectors.instagram import collect_instagram
from collectors.tiktok import collect_tiktok
from pipeline.dedupe import dedupe_videos
from pipeline.export import write_results
from scraper_config import ScraperConfig


def load_hashtags(file_path: str = "hashtags.txt"):
    with open(file_path, "r", encoding="utf-8") as f:
        cleaned = []
        seen = set()
        for line in f:
            hashtag = re.sub(r"\s+", "", line.strip().lstrip("#"))
            key = hashtag.lower()
            if hashtag and key not in seen:
                cleaned.append(hashtag)
                seen.add(key)
    return cleaned


def print_run_summary(label, rows):
    by_platform = Counter([r.platform for r in rows])
    print(f"{label}: total={len(rows)} | Instagram={by_platform.get('Instagram', 0)} | TikTok={by_platform.get('TikTok', 0)}")


def keep_rows_with_posted_date(rows):
    return [row for row in rows if row.posted_date]


def dedupe_profiles_global(rows):
    """
    Global (per-platform) profile dedupe:
    - key: (platform, profile_handle) when available, else (platform, video_link)
    - keep newest posted_date (string compare works for 'YYYY-MM-DD...' formats we generate)
    """
    def _key(r):
        handle = (r.profile_handle or "").strip().lower() if isinstance(r.profile_handle, str) else r.profile_handle
        if handle:
            return (r.platform, handle)
        return (r.platform, r.video_link)

    best = {}
    for r in rows:
        k = _key(r)
        prev = best.get(k)
        if not prev:
            best[k] = r
            continue
        prev_date = prev.posted_date or ""
        curr_date = r.posted_date or ""
        if curr_date > prev_date:
            best[k] = r
    return list(best.values())


async def main():
    config = ScraperConfig()
    hashtags = load_hashtags("hashtags.txt")
    if not hashtags:
        print("No hashtags found in hashtags.txt")
        return

    print(f"Loaded {len(hashtags)} hashtag queries")
    print("Starting Instagram pass first...")
    ig_rows, ig_diag = await collect_instagram(hashtags, config)
    print(f"Instagram raw videos: {len(ig_rows)}")
    print(f"Instagram per-tag new videos: {ig_diag}")

    print("Starting TikTok pass second...")
    tt_rows, tt_diag = await collect_tiktok(hashtags, config)
    print(f"TikTok raw videos: {len(tt_rows)}")
    print(f"TikTok per-tag new videos: {tt_diag}")

    all_rows = ig_rows + tt_rows
    print_run_summary("Collected", all_rows)

    video_unique = dedupe_videos(all_rows)
    print_run_summary("After video dedupe", video_unique)

    dated_rows = keep_rows_with_posted_date(video_unique)
    print_run_summary("After posted-date filter", dated_rows)

    global_profile_unique = dedupe_profiles_global(dated_rows)
    print_run_summary("After global profile dedupe", global_profile_unique)

    if sys.stdin and sys.stdin.isatty():
        filename_input = input("Enter filename (or press Enter for auto): ").strip()
    else:
        # Non-interactive/background runs: auto filename.
        filename_input = ""
    output_path = write_results(global_profile_unique, filename_input)
    print(f"Saved {len(global_profile_unique)} records to: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())