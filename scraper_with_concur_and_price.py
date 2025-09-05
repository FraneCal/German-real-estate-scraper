import os
import time
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

BASE_URL = "https://www.bestfewo.de/expose/"
SAVE_DIR = "htmls_3"
LISTING_URL = "https://www.bestfewo.de/laender/deutschland?page={}"

# Requests session with retries
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
session.mount("https://", HTTPAdapter(max_retries=retries))

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
}

DELAY_RANGE = (0.3, 0.6)
LOG_FILE = "download_log.txt"
EXCEL_FILE = "results.xlsx"


def scrape_property_links(pages=5980):
    listings = []
    for page in range(1, pages + 1):
        url = LISTING_URL.format(page)
        print(f"Fetching page {page} -> {url}")
        try:
            response = session.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract both object ID and price from listing container
            for div in soup.select("div.prices.objectInfos"):
                object_id = div.get("data-objectid")
                price_tag = div.select_one("p.pricetag")

                price = "N/A"
                if price_tag:
                    price = price_tag.get_text(strip=True)
                    # Clean up: remove "ab" and non-breaking spaces
                    price = price.replace("ab", "").replace("\xa0", "").strip()

                if object_id:
                    listings.append({
                        "object_id": object_id,
                        "link": BASE_URL + object_id,
                        "price": price
                    })

            time.sleep(random.uniform(*DELAY_RANGE))

        except Exception as e:
            print(f"❌ Failed to fetch page {page}: {e}")
    return listings


def save_html(object_id):
    link = BASE_URL + object_id
    file_path = os.path.join(SAVE_DIR, f"{object_id}.html")

    if os.path.exists(file_path):
        return f"Already saved: {object_id}"

    try:
        # Retry loop
        for attempt in range(5):
            try:
                response = session.get(link, headers=HEADERS, timeout=15)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                print(f"⚠️ Retry {attempt + 1} for {object_id}: {e}")
                time.sleep(2 ** attempt + random.uniform(0, 1))
        else:
            log_result(object_id, success=False, message="Failed after retries")
            return f"❌ Failed after retries: {object_id}"

        os.makedirs(SAVE_DIR, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        time.sleep(random.uniform(*DELAY_RANGE))
        log_result(object_id, success=True)
        return f"Saved: {object_id}"

    except Exception as e:
        log_result(object_id, success=False, message=str(e))
        return f"❌ Crash at ID {object_id}: {e}"


def log_result(object_id, success=True, message=""):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        status = "SUCCESS" if success else "ERROR"
        f.write(f"{status}: {object_id} {message}\n")


def download_with_eta(listings, max_workers=8, eta_interval=1000, total_pages=150000):
    total = len(listings)
    completed = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(save_html, item["object_id"]): item for item in listings}
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            print(result)

            if completed % eta_interval == 0 or completed == total:
                elapsed = time.time() - start_time
                remaining = total - completed
                eta_seconds = elapsed / completed * remaining
                eta_full_seconds = elapsed / completed * total_pages
                print(f"\n⏱ Completed {completed}/{total}. Estimated remaining time: {eta_seconds/3600:.2f} hours")
                print(f"⏱ Estimated total time for {total_pages} pages: {eta_full_seconds/3600:.2f} hours\n")


if __name__ == "__main__":
    listings = scrape_property_links(pages=5980)
    print(f"Found {len(listings)} listings. Downloading...")

    # Save to Excel (initially with Link + Price)
    df = pd.DataFrame(listings)
    df.to_excel(EXCEL_FILE, index=False)
    print(f"💾 Saved initial Excel file: {EXCEL_FILE}")

    # Download HTMLs
    download_with_eta(listings, max_workers=8, eta_interval=1000, total_pages=150000)
