import random
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from playwright.async_api import Page, async_playwright

from pipeline.models import VideoRecord
from scraper_config import ScraperConfig


def _ts_to_str(ts) -> Optional[str]:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _walk(payload):
    if isinstance(payload, dict):
        yield payload
        for value in payload.values():
            yield from _walk(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _walk(item)


def _extract_video_id(url: str) -> Optional[str]:
    match = re.search(r"/video/(\d+)", url)
    if match:
        return match.group(1)
    return None


def _to_tiktok_meta(node: dict) -> Optional[dict]:
    aweme_id = node.get("aweme_id") or node.get("id")
    video_obj = node.get("video")
    if not aweme_id or not isinstance(video_obj, dict):
        return None

    author = node.get("author") if isinstance(node.get("author"), dict) else {}
    author_info = node.get("authorInfo") if isinstance(node.get("authorInfo"), dict) else {}
    user = node.get("user") if isinstance(node.get("user"), dict) else {}
    handle = (
        author.get("unique_id")
        or author.get("uniqueId")
        or author.get("uid")
        or author_info.get("uniqueId")
        or user.get("uniqueId")
        or user.get("unique_id")
    )
    create_time = node.get("create_time") or node.get("createTime")
    share_url = node.get("share_url") or node.get("shareUrl")
    return {
        "aweme_id": str(aweme_id),
        "handle": str(handle).lower() if handle else None,
        "share_url": share_url,
        "posted_date": _ts_to_str(create_time),
    }


async def _extract_from_dom(page: Page, hashtag: str, records: Dict[str, VideoRecord], meta_by_id: Dict[str, dict]):
    anchors = await page.query_selector_all("a[href*='/video/']")
    for anchor in anchors:
        href = await anchor.get_attribute("href")
        if not href:
            continue
        if href.startswith("/"):
            link = f"https://www.tiktok.com{href.split('?')[0]}"
        else:
            link = href.split("?")[0]
        video_id = _extract_video_id(link)
        if not video_id:
            continue
        meta = meta_by_id.get(video_id, {})
        handle = meta.get("handle")
        share_url = meta.get("share_url")
        posted_date = meta.get("posted_date")

        if share_url and "/video/" in share_url:
            final_link = share_url.split("?")[0]
        elif handle:
            final_link = f"https://www.tiktok.com/@{handle}/video/{video_id}"
        else:
            final_link = f"https://www.tiktok.com/video/{video_id}"

        if final_link in records:
            continue
        records[final_link] = VideoRecord(
            platform="TikTok",
            hashtag=hashtag,
            video_link=final_link,
            posted_date=posted_date,
            profile_handle=handle,
            profile_link=(f"https://www.tiktok.com/@{handle}" if handle else None),
            source_confidence="dom+network" if meta else "dom",
        )


async def _fill_missing_dates(page: Page, records: Dict[str, VideoRecord], config: ScraperConfig):
    missing = [r for r in records.values() if not r.posted_date]
    for row in missing:
        for _ in range(config.post_date_retry_limit):
            try:
                await page.goto(row.video_link, wait_until="domcontentloaded")
                await page.wait_for_timeout(config.post_date_wait_ms)
                time_element = await page.query_selector("time")
                if time_element:
                    dt = await time_element.get_attribute("datetime")
                    if dt:
                        row.posted_date = dt
                author_link = await page.query_selector("a[href*='@']")
                if author_link:
                    href = await author_link.get_attribute("href")
                    if href and "@" in href:
                        handle = href.split("@", 1)[1].split("/", 1)[0].strip().lower()
                        if handle:
                            row.profile_handle = handle
                            row.profile_link = f"https://www.tiktok.com/@{handle}"
                if row.posted_date:
                    break
            except Exception:
                continue


async def collect_tiktok(hashtags: List[str], config: ScraperConfig) -> Tuple[List[VideoRecord], Dict[str, int]]:
    records: Dict[str, VideoRecord] = {}
    diagnostics: Dict[str, int] = {}
    meta_by_id: Dict[str, dict] = {}

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch_persistent_context(
                user_data_dir=config.tiktok_user_data_dir,
                headless=config.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            browser = await p.chromium.launch_persistent_context(
                user_data_dir=config.tiktok_user_data_dir,
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
        page = await browser.new_page()
        current_tag = {"value": ""}

        async def on_response(response):
            try:
                if "tiktok.com" not in response.url:
                    return
                if "json" not in response.headers.get("content-type", ""):
                    return
                payload = await response.json()
                for node in _walk(payload):
                    if not isinstance(node, dict):
                        continue
                    meta = _to_tiktok_meta(node)
                    if meta:
                        meta_by_id[meta["aweme_id"]] = meta
            except Exception:
                return

        page.on("response", on_response)
        await page.goto("https://www.tiktok.com", wait_until="domcontentloaded")
        await page.wait_for_timeout(config.page_load_wait_ms)

        for tag in hashtags:
            current_tag["value"] = tag
            start_count = len(records)
            print(f"[TikTok] searching #{tag}")
            await page.goto(f"https://www.tiktok.com/tag/{tag}", wait_until="domcontentloaded")
            await page.wait_for_timeout(config.per_tag_open_wait_ms)

            for scroll_idx in range(config.tt_scroll_count):
                await page.mouse.wheel(0, 5000)
                await page.wait_for_timeout(random.randint(config.min_wait_ms, config.max_wait_ms))
                if scroll_idx % 5 == 0:
                    print(f"[TikTok] {tag}: scroll {scroll_idx + 1}/{config.tt_scroll_count} | videos: {len(records)}")

            await _extract_from_dom(page, tag, records, meta_by_id)
            added = len(records) - start_count

            # Robustness: some tags render a different grid on first load.
            # If we got nothing, retry once with a longer wait and a second scroll pass.
            if added == 0:
                try:
                    await page.goto(f"https://www.tiktok.com/tag/{tag}", wait_until="domcontentloaded")
                    await page.wait_for_timeout(int(config.per_tag_open_wait_ms * 1.8))
                    for _ in range(max(6, config.tt_scroll_count // 3)):
                        await page.mouse.wheel(0, 5000)
                        await page.wait_for_timeout(random.randint(config.min_wait_ms, config.max_wait_ms))
                    await _extract_from_dom(page, tag, records, meta_by_id)
                except Exception:
                    pass

            diagnostics[tag] = len(records) - start_count

        await _fill_missing_dates(page, records, config)
        await browser.close()

    return list(records.values()), diagnostics
