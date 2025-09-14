#!/usr/bin/env python3
"""
Safer YouTube batch query runner with hard caps.

Caps added
  per-query window cap (default 100)
  global max total videos (default 2000)
  max number of search.list calls per run (default 80)  ~8k quota units
  periodic checkpointing to CSV so you keep partial progress

Install
  pip install google-api-python-client python-dateutil pandas
"""

import os, sys, time, argparse, json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

ISO_FMT = "%Y-%m-%d"

def iso8601(dt): return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
def parse_date(s): return datetime.strptime(s, ISO_FMT)

def month_ranges(start_dt, end_dt):
    cur = start_dt.replace(day=1)
    endm = end_dt.replace(day=1)
    while cur <= endm:
        nxt = cur + relativedelta(months=1)
        yield iso8601(cur), iso8601(nxt)
        cur = nxt

def safe_execute(request, max_retries=5):
    for attempt in range(max_retries):
        try:
            return request.execute()
        except HttpError as e:
            status = getattr(e, "status_code", None) or getattr(e, "resp", {}).get("status")
            if status in ("403", "429", 403, 429):
                time.sleep(min(2 ** attempt, 16))
                continue
            raise
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return request.execute()

def collect_search_ids(youtube, query, order, published_after, published_before,
                       per_query_cap, search_call_budget):
    """
    Returns a tuple (ids, search_calls_used).
    Respects per_query_cap and stops if search_call_budget is depleted.
    """
    results, page_token = [], None
    calls_used = 0
    while True:
        if search_call_budget is not None and calls_used >= search_call_budget:
            break
        req = youtube.search().list(
            part="id",
            q=query,
            type="video",
            order=order,
            maxResults=50,
            pageToken=page_token,
            **({"publishedAfter": published_after} if published_after else {}),
            **({"publishedBefore": published_before} if published_before else {})
        )
        res = safe_execute(req)
        calls_used += 1
        items = res.get("items", [])
        for it in items:
            if it.get("id", {}).get("kind") == "youtube#video":
                results.append(it["id"]["videoId"])
        page_token = res.get("nextPageToken")
        if page_token is None or len(results) >= per_query_cap:
            break
    return results[:per_query_cap], calls_used

def enrich_video_meta(youtube, video_ids):
    out = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        req = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(chunk)
        )
        res = safe_execute(req)
        for it in res.get("items", []):
            sn = it.get("snippet", {})
            cd = it.get("contentDetails", {})
            st = it.get("statistics", {})
            vid = it.get("id")
            out.append({
                "watch_url": f"https://www.youtube.com/watch?v={vid}",
                "short_url": f"https://youtu.be/{vid}",
                "embed_url": f"https://www.youtube.com/embed/{vid}",
                "videoId": it.get("id"),
                "title": sn.get("title"),
                "description": sn.get("description"),
                "channelId": sn.get("channelId"),
                "channelTitle": sn.get("channelTitle"),
                "publishedAt": sn.get("publishedAt"),
                "duration": cd.get("duration"),
                "dimension": cd.get("dimension"),
                "definition": cd.get("definition"),
                "licensedContent": cd.get("licensedContent"),
                "projection": cd.get("projection"),
                "viewCount": st.get("viewCount"),
                "likeCount": st.get("likeCount"),
                "commentCount": st.get("commentCount"),
                "tags": "|".join(sn.get("tags", [])) if sn.get("tags") else None,
                "defaultAudioLanguage": sn.get("defaultAudioLanguage"),
                "defaultLanguage": sn.get("defaultLanguage"),
                "categoryId": sn.get("categoryId"),
                "thumbnail_default_url": sn.get("thumbnails", {}).get("default", {}).get("url"),
                "thumbnail_medium_url": sn.get("thumbnails", {}).get("medium", {}).get("url"),
                "thumbnail_high_url": sn.get("thumbnails", {}).get("high", {}).get("url"),
            })
    return out

def read_queries(path):
    qs = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            qs.append(s)
    return qs

def write_checkpoint(rows, counts_rows, output_csv, counts_csv):
    df = pd.DataFrame(rows)
    if not df.empty:
        preferred = [
            "query", "window_start", "window_end",
            "videoId", "title", "channelTitle", "channelId",
            "publishedAt", "duration", "viewCount", "likeCount", "commentCount",
            "tags", "description",
            "thumbnail_default_url", "thumbnail_medium_url", "thumbnail_high_url",
            "definition", "projection", "licensedContent", "dimension",
            "defaultAudioLanguage", "defaultLanguage", "categoryId",
        ]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]
    df.to_csv(output_csv, index=False)
    pd.DataFrame(counts_rows).to_csv(counts_csv, index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--counts-csv", default="counts.csv")
    ap.add_argument("--api-key", default=os.getenv("YOUTUBE_API_KEY"))
    ap.add_argument("--order", default="relevance",
                    choices=["relevance", "date", "rating", "viewCount", "title", "videoCount"])
    ap.add_argument("--published-after", default=None, help="YYYY-MM-DD")
    ap.add_argument("--published-before", default=None, help="YYYY-MM-DD")
    ap.add_argument("--split-monthly", action="store_true")
    ap.add_argument("--per-query-cap", type=int, default=100, help="max videos per query per window")
    ap.add_argument("--max-total", type=int, default=2000, help="global max videos across the run")
    ap.add_argument("--max-search-calls", type=int, default=80,
                    help="hard limit on number of search.list calls per run")
    ap.add_argument("--checkpoint-every", type=int, default=500,
                    help="write partial CSV every N enriched videos")
    args = ap.parse_args()

    if not args.api_key:
        print("Missing API key. Set YOUTUBE_API_KEY or use --api-key.", file=sys.stderr)
        sys.exit(1)
    yt = build("youtube", "v3", developerKey=args.api_key)

    queries = read_queries(args.queries)
    print(f"Loaded {len(queries)} queries")

    start_iso = iso8601(parse_date(args.published_after)) if args.published_after else None
    # end exclusive by adding one day
    end_iso = iso8601(parse_date(args.published_before) + timedelta(days=1)) if args.published_before else None

    windows = [(start_iso, end_iso)]
    if args.split_monthly and args.published_after and args.published_before:
        start_dt = parse_date(args.published_after)
        end_dt = parse_date(args.published_before)
        windows = list(month_ranges(start_dt, end_dt))

    # Tracking and caps
    total_enriched = 0
    search_calls_used = 0
    rows = []
    counts_rows = []
    seen_video_ids = set()

    try:
        for q_idx, q in enumerate(queries, start=1):
            if args.max_search_calls is not None and search_calls_used >= args.max_search_calls:
                print("Max search calls reached; stopping.")
                break
            print(f"[{q_idx}/{len(queries)}] Query: {q}")

            query_seen = set()
            query_added = 0

            for w_idx, (ws, we) in enumerate(windows, start=1):
                if args.max_search_calls is not None and search_calls_used >= args.max_search_calls:
                    print("Max search calls reached within windows; stopping.")
                    break

                print(f"  Window {w_idx}/{len(windows)}  {ws or '-inf'} -> {we or '+inf'}")
                budget_left = None
                if args.max_search_calls is not None:
                    budget_left = max(0, args.max_search_calls - search_calls_used)

                ids, used = collect_search_ids(
                    youtube=yt,
                    query=q,
                    order=args.order,
                    published_after=ws,
                    published_before=we,
                    per_query_cap=args.per_query_cap,
                    search_call_budget=budget_left
                )
                search_calls_used += used
                if not ids:
                    counts_rows.append({"query": q, "window_start": ws, "window_end": we, "raw_count": 0})
                    continue

                # Dedup across all queries and this query
                new_ids = [v for v in ids if v not in seen_video_ids]
                seen_video_ids.update(new_ids)
                query_seen.update(ids)

                # Respect global max total
                room = args.max_total - total_enriched
                if room <= 0:
                    print("Global max-total reached; stopping.")
                    break
                if len(new_ids) > room:
                    new_ids = new_ids[:room]

                if not new_ids:
                    counts_rows.append({"query": q, "window_start": ws, "window_end": we, "raw_count": len(ids)})
                    if args.max_search_calls is not None and search_calls_used >= args.max_search_calls:
                        break
                    continue

                # Enrich and record
                enriched = enrich_video_meta(yt, new_ids)
                total_enriched += len(enriched)
                query_added += len(enriched)

                for rec in enriched:
                    rec_row = {"query": q, "window_start": ws, "window_end": we}
                    rec_row.update(rec)
                    rows.append(rec_row)

                counts_rows.append({
                    "query": q, "window_start": ws, "window_end": we,
                    "raw_count": len(ids), "unique_new_in_window": len(new_ids)
                })

                print(f"    collected ids: {len(ids)}  new unique: {len(new_ids)}  total_enriched: {total_enriched}  search_calls_used: {search_calls_used}")

                # Periodic checkpoint
                if total_enriched % max(args.checkpoint_every, 1) == 0:
                    print("    checkpoint: writing partial CSVs...")
                    write_checkpoint(rows, counts_rows, args.output_csv, args.counts_csv)

                if total_enriched >= args.max_total:
                    print("Global max-total reached; stopping.")
                    break
                if args.max_search_calls is not None and search_calls_used >= args.max_search_calls:
                    print("Max search calls reached; stopping.")
                    break

            print(f"  query summary  raw_ids={len(query_seen)}  added_rows={query_added}")

            if total_enriched >= args.max_total:
                break
            if args.max_search_calls is not None and search_calls_used >= args.max_search_calls:
                break

    finally:
        # Final write
        write_checkpoint(rows, counts_rows, args.output_csv, args.counts_csv)
        print(f"Wrote {len(rows)} rows to {args.output_csv}")
        print(f"Wrote counts to {args.counts_csv}")
        print(f"search.list calls used: {search_calls_used}  approx quota: {search_calls_used * 100} units")

if __name__ == "__main__":
    main()
