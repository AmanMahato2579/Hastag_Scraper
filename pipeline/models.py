from dataclasses import dataclass
from typing import Optional


@dataclass
class VideoRecord:
    platform: str
    hashtag: str
    video_link: str
    posted_date: Optional[str]
    profile_handle: Optional[str]
    profile_link: Optional[str]
    source_confidence: str
