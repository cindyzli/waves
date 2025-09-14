import time
import random
import re
import json
from dataclasses import dataclass
from typing import Iterable, List, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
import urllib.robotparser as robotparser

@dataclass
class CrawlConfig:
    start_urls: List[str]
    allowed_domains: List[str]
    user_agent: str = "EduCrawler/1.0 (+contact@example.edu)"
    max_pages: int = 300
    delay_range: Tuple[float, float] = (0.8, 2.0)  # seconds
    timeout: int = 15

def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent, "Accept-Language": "en"})
    retries = Retry(
        total=6,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

def can_fetch(url: str, user_agent: str) -> bool:
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch(user_agent, url)
    except Exception:
        # If robots fails to load, err on the safe side and disallow
        return False

def same_domain(url: str, allowed_domains: List[str]) -> bool:
    host = urlparse(url).netloc.lower()
    return any(host.endswith(d) for d in allowed_domains)

def extract_links(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("a[href]"):
        href = a["href"].strip()
        if href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)
        links.append(abs_url)
    return links

def parse_wikipedia_page(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.select_one("#firstHeading")
    title_text = title.get_text(strip=True) if title else ""
    # Grab first paragraph text as a simple summary
    para = soup.select_one("div.mw-parser-output > p:not(.mw-empty-elt)")
    summary = para.get_text(" ", strip=True) if para else ""
    # Example: collect external links that look like performance resources
    ext_links = []
    for a in soup.select("#bodyContent a[href]"):
        href = a["href"]
        if href.startswith("http"):
            if any(k in href.lower() for k in ["piano", "score", "imslp", "musescore", "performance", "recital"]):
                ext_links.append(href)
    return {
        "url": url,
        "title": title_text,
        "summary": summary,
        "external_links": sorted(set(ext_links))[:50],
    }

def respectful_sleep(cfg: CrawlConfig):
    time.sleep(random.uniform(*cfg.delay_range))

def crawl(cfg: CrawlConfig) -> List[dict]:
    session = make_session(cfg.user_agent)
    to_visit: List[str] = list(cfg.start_urls)
    seen: Set[str] = set()
    results: List[dict] = []

    while to_visit and len(seen) < cfg.max_pages:
        url = to_visit.pop(0)
        if url in seen:
            continue
        if not same_domain(url, cfg.allowed_domains):
            continue
        if not can_fetch(url, cfg.user_agent):
            continue

        try:
            resp = session.get(url, timeout=cfg.timeout)
            resp.raise_for_status()
        except requests.RequestException:
            continue

        html = resp.text
        seen.add(url)

        # Example parser for Wikipedia pages
        if "wikipedia.org" in url:
            data = parse_wikipedia_page(url, html)
            results.append(data)

        # Frontier expansion from category or content pages
        links = extract_links(url, html)
        for link in links:
            if same_domain(link, cfg.allowed_domains) and link not in seen:
                # Keep exploration bounded to category and article space
                if re.search(r"/wiki/(?:Category:|.+)$", link) and not re.search(r":Talk|:Help|:File|:Template|:Portal", link):
                    to_visit.append(link)

        respectful_sleep(cfg)

    return results

if __name__ == "__main__":
    cfg = CrawlConfig(
        start_urls=[
            # Wikipedia category seeds related to piano topics
            "https://en.wikipedia.org/wiki/Category:Piano_music",
            "https://en.wikipedia.org/wiki/Category:Compositions_for_piano",
            "https://en.wikipedia.org/wiki/Category:Piano_composers"
        ],
        allowed_domains=["wikipedia.org", "en.wikipedia.org"]
    )
    data = crawl(cfg)
    with open("piano_wiki_crawl.jsonl", "w", encoding="utf-8") as f:
        for row in data:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"wrote {len(data)} rows to piano_wiki_crawl.jsonl")
