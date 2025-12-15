import os
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional

import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv

def build_youtube_client(api_key: str):
    """Create a YouTube Data API client."""
    return build("youtube", "v3", developerKey=api_key)


def search_videos(
    youtube,
    keywords: List[str],
    start_date: str,
    end_date: str,
    max_videos_per_keyword: int = 50,
) -> List[Dict]:
    """
    Search for videos matching the given keywords within a date range.
    Returns a list of dicts with video_id, title, channel, published_at, keyword.
    """
    all_videos = []
    
    for keyword in keywords:
        print(f"Keyword: {keyword}")
        collected = 0
        next_page_token: Optional[str] = None

        while collected < max_videos_per_keyword:
            try:
                request = youtube.search().list(
                    part="snippet",
                    q=keyword,
                    type="video",
                    publishedAfter=start_date,
                    publishedBefore=end_date,
                    maxResults=50,
                    pageToken=next_page_token,
                    order="relevance",
                    relevanceLanguage="en",
                )
                response = request.execute()
            except Exception as e:
                print(f"Error: Search failed for keyword '{keyword}': {e}")
                break

            for item in response.get("items", []):
                video_id = item["id"]["videoId"]
                snippet = item["snippet"]
                all_videos.append(
                    {
                        "video_id": video_id,
                        "video_title": snippet.get("title", ""),
                        "channel": snippet.get("channelTitle", ""),
                        "video_published_at": snippet.get("publishedAt", ""),
                        "keyword": keyword,
                    }
                )
                collected += 1
                if collected >= max_videos_per_keyword:
                    break

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        print(f"Collected {collected} videos for keyword '{keyword}'")

    # Remove duplicates by video_id
    unique_videos = {v["video_id"]: v for v in all_videos}
    print(f"Total unique videos: {len(unique_videos)}")

    return list(unique_videos.values())


def fetch_comments_for_video(
    youtube,
    video,
    start_date: datetime,
    end_date: datetime,
    max_comments: int = 500,
) -> List[Dict]:
    """
    Fetch top-level comments for a single video within the date range.
    Returns a list of dicts with comment info.
    """
    comments = []
    video_id = video["video_id"]
    video_title = video["video_title"]
    channel = video["channel"]
    video_published_at = video["video_published_at"]
    keyword = video["keyword"]

    print(f"Fetching comments for video {video_id} ({video_title[:40]}...)")

    next_page_token: Optional[str] = None
    fetched = 0

    while fetched < max_comments:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token,
                textFormat="plainText",
                order="time",
            )
            response = request.execute()
        except Exception as e:
            print(f"Error: Fetching comments failed for video {video_id}: {e}")
            break

        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]

            comment_text = snippet.get("textDisplay", "")
            comment_published_at_str = snippet.get("publishedAt", "")
            comment_likes = snippet.get("likeCount", 0)

            # Convert comment date and filter by range
            try:
                comment_dt = datetime.fromisoformat(
                    comment_published_at_str.replace("Z", "+00:00")
                )
            except Exception:
                # If something is weird with the format, skip
                continue

            if comment_dt < start_date or comment_dt > end_date:
                continue

            comments.append(
                {
                    "video_id": video_id,
                    "video_title": video_title,
                    "channel": channel,
                    "video_published_at": video_published_at,
                    "comment_id": item["id"],
                    "comment_text": comment_text,
                    "comment_likes": int(comment_likes),
                    "comment_published_at": comment_published_at_str,
                    "keyword": keyword,
                }
            )
            fetched += 1
            if fetched >= max_comments:
                break

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

        time.sleep(0.2)

    print(f"Collected {fetched} comments for video {video_id}")
    return comments

def scrape_youtube_comments(
    api_key: str,
    keywords: List[str],
    start_date_str: str,
    end_date_str: str,
    max_videos_per_keyword: int,
    max_comments_per_video: int,
    output_path: str,
):
    """
    Main orchestrator:
    - Builds client
    - Searches videos
    - Fetches comments per video
    - Saves to CSV
    """
    youtube = build_youtube_client(api_key)

    # For the API: RFC3339 timestamps
    start_rfc3339 = f"{start_date_str}T00:00:00Z"
    end_rfc3339 = f"{end_date_str}T23:59:59Z"

    # For local comparison: timezone-aware UTC datetimes
    start_dt = datetime.fromisoformat(start_date_str).replace(
        tzinfo=timezone.utc
    )
    end_dt = datetime.fromisoformat(end_date_str).replace(
        tzinfo=timezone.utc,
        hour=23,
        minute=59,
        second=59
    )

    videos = search_videos(
        youtube=youtube,
        keywords=keywords,
        start_date=start_rfc3339,
        end_date=end_rfc3339,
        max_videos_per_keyword=max_videos_per_keyword,
    )

    all_comments: List[Dict] = []

    for video in videos:
        video_comments = fetch_comments_for_video(
            youtube=youtube,
            video=video,
            start_date=start_dt,
            end_date=end_dt,
            max_comments=max_comments_per_video,
        )
        all_comments.extend(video_comments)

    if not all_comments:
        print("No comments collected. Check your filters/keywords.")
        return

    df = pd.DataFrame(all_comments)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved {len(df)} comments to {output_path}")

if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("YOUTUBE_API_KEY")
    
    # Keywords to fetch comments related to AI
    KEYWORDS = [
        "generative ai",
        "chatgpt",
        "gpt-4",
        "large language model",
        "text-to-image ai",
        "midjourney ai",
        "stable diffusion ai",
        "ai replacing jobs",
        "ai jobs",
        "ai regulation",
        "ai ethics",
        "ai automation",
    ]

    scrape_youtube_comments(
        api_key=API_KEY,
        keywords=KEYWORDS,
        start_date_str="2022-01-01",
        end_date_str="2025-12-08",
        max_videos_per_keyword=50,
        max_comments_per_video=500,
        output_path="data/youtube_raw.csv",
    )