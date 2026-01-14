import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import httpx
from bs4 import BeautifulSoup

BASE = "https://www.car.gr"
LISTING_RE = re.compile(r"^/classifieds/cars/view/(\d+)(?:-|$)")

PRICE_RE = re.compile(r"(\d[\d\.]*)\s*€")
KM_RE    = re.compile(r"(\d[\d\.]*)\s*Km", re.IGNORECASE)
CC_RE    = re.compile(r"(\d[\d\.]*)\s*cc", re.IGNORECASE)
HP_RE    = re.compile(r"(\d+)\s*hp", re.IGNORECASE)

def with_pg(url: str, pg: int) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    q["pg"] = [str(pg)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def fetch(client: httpx.Client, url: str) -> str:
    r = client.get(url)
    r.raise_for_status()
    return r.text

def parse_last_page(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    pages = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "pg=" in href:
            m = re.search(r"[?&]pg=(\d+)", href)
            if m:
                pages.add(int(m.group(1)))
    return max(pages) if pages else 1

def parse_results_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    out = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = LISTING_RE.match(href)
        if not m:
            continue

        ad_id = int(m.group(1))
        if ad_id in seen:
            continue
        seen.add(ad_id)

        url = urljoin(BASE, href)
        text = " ".join(a.stripped_strings)

        # Basic summary extraction from the card text
        price = (PRICE_RE.search(text).group(1) if PRICE_RE.search(text) else None)
        km    = (KM_RE.search(text).group(1) if KM_RE.search(text) else None)
        cc    = (CC_RE.search(text).group(1) if CC_RE.search(text) else None)
        hp    = (HP_RE.search(text).group(1) if HP_RE.search(text) else None)

        out.append({
            "ad_id": ad_id,
            "url": url,
            "card_text": text,
            "price_eur": int(price.replace(".", "")) if price else None,
            "km": int(km.replace(".", "")) if km else None,
            "cc": int(cc.replace(".", "")) if cc else None,
            "hp": int(hp) if hp else None,
        })

    return out

# --- Listing page parsing (label -> value scanning) ---

DETAIL_LABELS = {
    "Μάρκα - μοντέλο",
    "Αριθμός αγγελίας",
    "Τιμή",
    "Κατάσταση",
    "Κατηγορία",
    "Χρονολογία",
    "Χιλιόμετρα",
    "Καύσιμο",
    "Κυβικά",
    "Ιπποδύναμη",
    "Σασμάν",
    "Χρώμα",
    "Κίνηση τροχών",
    "Πόρτες",
    "Θέσεις επιβατών",
    "Τελευταία αλλαγή",
}

def parse_listing_page(html: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")
    strings = list(soup.stripped_strings)

    data: Dict[str, str] = {}

    # Focus around the "Στοιχεία αγγελίας" section if present
    try:
        start = strings.index("Στοιχεία αγγελίας")
    except ValueError:
        start = 0

    window = strings[start:start + 200]

    i = 0
    while i < len(window) - 1:
        k = window[i]
        v = window[i + 1]
        if k in DETAIL_LABELS:
            data[k] = v
            i += 2
        else:
            i += 1

    # Description
    try:
        di = strings.index("Περιγραφή")
        data["Περιγραφή"] = strings[di + 1]
    except ValueError:
        pass

    # Title + headline price (usually near the top)
    # (Works because the page has an H1-like title and a standalone "### 8.300 €" block.)
    if "Τίτλος" not in data:
        for s in strings[:80]:
            if "Suzuki" in s or "Jimny" in s:
                data["Τίτλος"] = s
                break

    return data

def crawl_search(search_url: str, max_pages: Optional[int] = None, fetch_details: bool = False) -> List[Dict]:
    headers = {
        "User-Agent": "GeorgeMetaSearch/0.1 (+contact: you@example.com)",
        "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
    }
    results: List[Dict] = []

    with httpx.Client(headers=headers, timeout=20.0, follow_redirects=True) as client:
        first_html = fetch(client, search_url)
        last_page = parse_last_page(first_html)
        if max_pages is not None:
            last_page = min(last_page, max_pages)

        for pg in range(1, last_page + 1):
            html = first_html if pg == 1 else fetch(client, with_pg(search_url, pg))
            page_items = parse_results_page(html)

            if fetch_details:
                for item in page_items:
                    time.sleep(1.0)  # be polite
                    detail_html = fetch(client, item["url"])
                    item["details"] = parse_listing_page(detail_html)

            results.extend(page_items)
            time.sleep(1.0)  # be polite between result pages

    return results

if __name__ == "__main__":
    url = "https://www.car.gr/used-cars/suzuki/jimny.html?category=15001&crashed=f&make=12858&media_types=photo&mileage-to=125000&model=14897&offer_type=sale&pg=1&price-from=1000&withprice=1"
    data = crawl_search(url, fetch_details=True)
    print(f"Got {len(data)} listings")
    print(data[0]["url"])
    print(data[0].get("details", {}).keys())
