from dataclasses import dataclass


@dataclass(frozen=True)
class ScraperConfig:
    instagram_user_data_dir: str = "insta_session"
    tiktok_user_data_dir: str = "tiktok_session"
    headless: bool = False
    ig_scroll_count: int = 28
    tt_scroll_count: int = 28
    # Instagram network/API pagination pages (50 items per page, best-effort)
    ig_api_pages: int = 3
    # When True, prints a couple of matched Instagram JSON URLs per tag (debugging).
    ig_response_url_debug: bool = False
    min_wait_ms: int = 2200
    max_wait_ms: int = 3600
    page_load_wait_ms: int = 7000
    per_tag_open_wait_ms: int = 6500
    post_date_wait_ms: int = 2200
    post_date_retry_limit: int = 2
