import os
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pandas as pd


GDELT_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


def to_gdelt_datetime(dt: datetime) -> str:
    """
    Convert datetime to GDELT API format: YYYYMMDDHHMMSS
    """
    return dt.strftime("%Y%m%d%H%M%S")


def generate_monthly_ranges(start_date: str, end_date: str) -> List[Dict[str, datetime]]:
    """
    Generate a list of {start, end} monthly ranges between start_date and end_date (inclusive).
    start_date / end_date: 'YYYY-MM-DD'
    """
    ranges = []
    current = datetime.fromisoformat(start_date).replace(day=1)
    end = datetime.fromisoformat(end_date)

    while current <= end:
        # Next month
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)

        # End of current month or global end
        month_end = min(next_month - timedelta(seconds=1), end.replace(hour=23, minute=59, second=59))
        month_start = current.replace(hour=0, minute=0, second=0)

        ranges.append({"start": month_start, "end": month_end})
        current = next_month

    return ranges


def fetch_gdelt_chunk(
    keyword: str,
    start_dt: datetime,
    end_dt: datetime,
    maxrecords: int = 250,
) -> List[Dict]:
    """
    Fetch one chunk from GDELT for a given keyword and time window.
    Returns a list of article dicts.
    """

    # Wrap the keyword in double quotes for exact phrase matching 
    # and to satisfy minimum length requirements for multi-word phrases.
    gdelt_query = f'"{keyword}"'

    params = {
        "query": gdelt_query,
        "mode": "ArtList",
        "maxrecords": str(maxrecords),
        "format": "json",
        "startdatetime": to_gdelt_datetime(start_dt),
        "enddatetime": to_gdelt_datetime(end_dt),
    }

    try:
        resp = requests.get(GDELT_API_URL, params=params, timeout=30)
        resp.raise_for_status()

        # Check for empty response body (important for this error)
        if not resp.text.strip():
            print(f"GDELT returned empty response for keyword '{keyword}' "
                  f"{start_dt.date()}–{end_dt.date()}. Skipping.")
            return []

        # Use specific exception handling for JSON decoding errors
        try:
            data = resp.json()
        except requests.exceptions.JSONDecodeError as jde:
            print(f"Error: JSON decode failed for keyword '{keyword}' "
                  f"{start_dt.date()}–{end_dt.date()}. Response text starts with: {resp.text[:100]}")
            # If GDELT gives a plaintext error, you can inspect it here.
            # You might want to introduce a delay and retry, but for now, we skip.
            return []
            
    except Exception as e:
        print(f"[Error: GDELT request failed for keyword '{keyword}' "
              f"{start_dt.date()}–{end_dt.date()}: {e}")
        return []

    # The rest of the logic remains the same
    if "articles" not in data:
        # Sometimes GDELT returns no 'articles' key if empty
        return []

    articles = data["articles"]

    # ... (rest of the record creation)
    records = []
    for art in articles:
        # GDELT fields can vary; we guard with .get
        records.append(
            {
                "url": art.get("url", ""),
                "title": art.get("title", ""),
                "source_domain": art.get("sourceDomain", ""),
                "language": art.get("language", ""),
                "country": art.get("domainCountryCode", ""),  # country of source domain
                "published_at": art.get("seendate", ""),      # format: YYYYMMDDHHMMSS
                "tone": art.get("tone", None),                # may be None
                "keyword": keyword,
                "source": "gdelt",
            }
        )

    return records


def normalize_published_time(seendate_str: str) -> Optional[str]:
    """
    Convert GDELT 'seendate' (YYYYMMDDHHMMSS) to ISO 'YYYY-MM-DD HH:MM:SS'.
    Returns None if parsing fails.
    """
    if not seendate_str:
        return None

    try:
        dt = datetime.strptime(seendate_str, "%Y%m%d%H%M%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def scrape_gdelt(
    keywords: List[str],
    start_date_str: str,
    end_date_str: str,
    output_path: str,
    maxrecords_per_query: int = 250,
    sleep_between_requests: float = 1.0,
):
    """
    Main GDELT scraper:
    - Iterates monthly between start_date and end_date
    - For each month and keyword, queries GDELT
    - Aggregates, deduplicates by URL
    - Saves to CSV
    """
    date_ranges = generate_monthly_ranges(start_date_str, end_date_str)
    print(f"Time windows: {len(date_ranges)} months from {start_date_str} to {end_date_str}")

    all_records: List[Dict] = []

    for keyword in keywords:
        print(f"Keyword: '{keyword}'")
        for dr in date_ranges:
            start_dt = dr["start"]
            end_dt = dr["end"]
            print(f" Time window: {start_dt.date()} – {end_dt.date()}")

            chunk_records = fetch_gdelt_chunk(
                keyword=keyword,
                start_dt=start_dt,
                end_dt=end_dt,
                maxrecords=maxrecords_per_query,
            )

            if chunk_records:
                all_records.extend(chunk_records)
                print(f"Retrieved {len(chunk_records)} articles")
            else:
                print("No articles")

            time.sleep(sleep_between_requests)

    if not all_records:
        print("[No GDELT articles collected. Check keywords/date range.")
        return

    df = pd.DataFrame(all_records)

    # Normalize published_at
    df["published_at_iso"] = df["published_at"].apply(normalize_published_time)

    # Deduplicate by URL + keyword to avoid exact duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["url", "keyword"])
    after = len(df)
    print(f"[RESULT] Deduplicated: {before} → {after} rows")

    # Ensure output directory exists
    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"[RESULT] Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    # Same keyword universe as YouTube for comparability
    KEYWORDS = [
        "generative ai",
        "chatgpt",
        "gpt-4",
        "large language model",
        "text-to-image ai",
        "midjourney",
        "stable diffusion",
        "ai replacing jobs",
        "ai jobs",
        "ai regulation",
        "ai ethics",
        "ai automation",
    ]

    scrape_gdelt(
        keywords=KEYWORDS,
        start_date_str="2022-01-01",
        end_date_str="2025-12-08",
        output_path="data/gdelt_raw.csv",
        maxrecords_per_query=250,
        sleep_between_requests=1.0,
    )