import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from urllib.parse import parse_qs, urlparse, unquote

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


def _to_ig_reel(node: dict) -> Optional[dict]:
    # Your network response contains nodes with:
    # media_type: 2 (reel/video), 1 (photo)
    media_type = node.get("media_type")
    if media_type != 2:
        return None

    code = node.get("code") or node.get("shortcode")
    user = node.get("user") if isinstance(node.get("user"), dict) else {}
    username = user.get("username")
    taken_at = node.get("taken_at") or node.get("taken_at_timestamp") or node.get("taken_at_ts")

    if not code:
        return None

    return {
        "code": str(code),
        "posted_date": _ts_to_str(taken_at),
        "handle": str(username).lower() if username else None,
    }


def _is_search_query_response(url: str, hashtag: str) -> bool:
    """
    Only accept responses from the search keyword page network calls.
    This prevents unrelated Instagram JSON (home feed, other endpoints) from being mislabeled as this hashtag.
    """
    try:
        u = urlparse(url)
        if "instagram.com" not in u.netloc:
            return False

        # We only want responses that clearly include the hashtag in query params.
        # (Path checks are brittle across IG experiments.)
        path = (u.path or "").lower()
        if "/api/" not in path and "/graphql/" not in path:
            return False

        qs = parse_qs(u.query or "")
        # IG commonly uses one of these for keyword search.
        candidates = []
        for key in ("query", "q", "keyword"):
            candidates.extend(qs.get(key, []))

        # If there is no obvious query key, scan all query values (some experiments rename params).
        if not candidates:
            for vals in qs.values():
                candidates.extend(vals)

        normalized_tag = hashtag.strip().lstrip("#").lower()
        for raw in candidates:
            if not raw:
                continue
            text = unquote(str(raw)).strip().lower()
            text = text.lstrip("#")
            if normalized_tag and normalized_tag in text:
                return True
        return False
    except Exception:
        return False


def _referer_matches_search_page(referer: str, hashtag: str) -> bool:
    try:
        if not referer:
            return False
        u = urlparse(referer)
        if "instagram.com" not in u.netloc:
            return False
        if "/explore/search/keyword/" not in (u.path or ""):
            return False
        qs = parse_qs(u.query or "")
        q = (qs.get("q", [None])[0] or "").strip()
        if not q:
            return False
        text = unquote(q).strip().lower().lstrip("#")
        tag = hashtag.strip().lower().lstrip("#")
        return text == tag
    except Exception:
        return False


def _upsert_reel(records: Dict[str, VideoRecord], hashtag: str, meta: dict, source: str):
    link = f"https://www.instagram.com/reel/{meta['code']}/"
    if link in records:
        # Backfill missing fields if we later see them.
        row = records[link]
        row.posted_date = row.posted_date or meta.get("posted_date")
        row.profile_handle = row.profile_handle or meta.get("handle")
        if row.profile_handle and not row.profile_link:
            row.profile_link = f"https://www.instagram.com/{row.profile_handle}/"
        return

    handle = meta.get("handle")
    records[link] = VideoRecord(
        platform="Instagram",
        hashtag=hashtag,
        video_link=link,
        posted_date=meta.get("posted_date"),
        profile_handle=handle,
        profile_link=(f"https://www.instagram.com/{handle}/" if handle else None),
        source_confidence=source,
    )


async def _extract_from_dom(page: Page, hashtag: str, records: Dict[str, VideoRecord]):
    # Fallback: sometimes reels show up as anchors (if not, network collector will handle it).
    anchors = await page.query_selector_all("a[href*='/reel/']")
    for anchor in anchors:
        href = await anchor.get_attribute("href")
        if not href:
            continue
        link = f"https://www.instagram.com{href.split('?')[0]}" if href.startswith("/") else href.split("?")[0]
        if "/reel/" not in link:
            continue
        if link in records:
            continue
        records[link] = VideoRecord(
            platform="Instagram",
            hashtag=hashtag,
            video_link=link,
            posted_date=None,
            profile_handle=None,
            profile_link=None,
            source_confidence="dom",
        )


async def collect_instagram(hashtags: List[str], config: ScraperConfig) -> Tuple[List[VideoRecord], Dict[str, int]]:
    records: Dict[str, VideoRecord] = {}
    diagnostics: Dict[str, int] = {}

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch_persistent_context(
                user_data_dir=config.instagram_user_data_dir,
                headless=config.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            # Fallback keeps runs alive on machines where headed browser cannot be attached.
            browser = await p.chromium.launch_persistent_context(
                user_data_dir=config.instagram_user_data_dir,
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
        page = await browser.new_page()
        current_tag = {"value": ""}

        seen_debug_urls = set()

        async def on_response(response):
            try:
                if "instagram.com" not in response.url:
                    return
                # Only consider requests happening while we're on a tag.
                hashtag = current_tag.get("value") or ""
                if not hashtag:
                    return

                # Primary filter: match by the request Referer (stable across IG experiments).
                req = response.request
                referer = (req.headers or {}).get("referer") if req else None
                if referer:
                    if not _referer_matches_search_page(referer, hashtag):
                        return
                else:
                    # Fallback: match by response URL query params when referer is missing.
                    if not _is_search_query_response(response.url, hashtag):
                        return

                if getattr(config, "ig_response_url_debug", False) and response.url not in seen_debug_urls and len(seen_debug_urls) < 2:
                    seen_debug_urls.add(response.url)
                    print(f"[Instagram][debug] matched JSON url: {response.url}")

                # IG sometimes serves JSON with non-json content-type; just attempt parsing.
                payload = await response.json()
                for node in _walk(payload):
                    if not isinstance(node, dict):
                        continue
                    meta = _to_ig_reel(node)
                    if meta:
                        _upsert_reel(records, hashtag, meta, source="network(media_type=2)")
            except Exception:
                return

        page.on("response", on_response)

        # Warm homepage once so cookies/session are loaded (user should be logged in in insta_session).
        await page.goto("https://www.instagram.com", wait_until="domcontentloaded")
        await page.wait_for_timeout(config.page_load_wait_ms)

        for tag in hashtags:
            current_tag["value"] = tag
            start_count = len(records)
            print(f"[Instagram] searching #{tag}")

            # Open search keyword page (matches your DevTools "query" JSON responses).
            q = quote(f"#{tag}")
            await page.goto(f"https://www.instagram.com/explore/search/keyword/?q={q}", wait_until="domcontentloaded")
            await page.wait_for_timeout(config.per_tag_open_wait_ms)

            for scroll_idx in range(config.ig_scroll_count):
                await page.mouse.wheel(0, 5000)
                await page.wait_for_timeout(random.randint(config.min_wait_ms, config.max_wait_ms))
                if scroll_idx % 5 == 0:
                    print(f"[Instagram] {tag}: scroll {scroll_idx + 1}/{config.ig_scroll_count} | reels: {len(records)}")

            # DOM fallback (in case network was blocked but anchors exist).
            await _extract_from_dom(page, tag, records)
            diagnostics[tag] = len(records) - start_count

        await browser.close()

    return list(records.values()), diagnostics
