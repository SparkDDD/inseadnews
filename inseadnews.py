import os
import cloudscraper
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime
import time

# --- Airtable Configuration ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tblLnvZF5bb6oj9ef"

FIELD_TITLE = "fldEhhyuhrKxmpjl0"
FIELD_PUBLICATION_DATE = "fldJZNPnajc0SHyh9"
FIELD_IMAGE_URL = "fldy48rpwvX54YoaU"
FIELD_ARTICLE_URL = "fldUo3r63Cnh6exMR"

AIRTABLE_ARTICLE_URL_COLUMN_NAME = "articleURL"

# --- URLs and Headers ---
BASE_URL = "https://www.insead.edu"
AJAX_URL = "https://www.insead.edu/views/ajax"

HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept":
    "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With":
    "XMLHttpRequest",
    "Referer":
    "https://www.insead.edu/newsroom/news?sort_by=field_publishing_date&sort_order=DESC&search_api_fulltext=",
}

# --- Logging Setup ---
logging.basicConfig(filename='insead_ajax_scraper.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(asctime)s [%(levelname)s] %(message)s')

# --- Cloudscraper ---
scraper = cloudscraper.create_scraper()


# --- Utility ---
def normalize_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")


def extract_date_from_tag(tag):
    if tag and tag.has_attr("datetime"):
        try:
            parsed_date = datetime.fromisoformat(tag["datetime"].replace(
                "Z", "+00:00"))
            return parsed_date.strftime("%Y-%m-%d")
        except Exception as e:
            logging.error(f"[DATE PARSE ERROR] {e}")
    return None


# --- AJAX Pagination Function ---
def fetch_ajax_page(page_num: int) -> list:
    logging.debug(f"🌐 Fetching AJAX page {page_num}")

    params = {
        "_wrapper_format":
        "drupal_ajax",
        "view_name":
        "insead_stories",
        "view_display_id":
        "insead_stories",
        "view_args":
        "",
        "view_path":
        "/node/116796",
        "view_base_path":
        "",
        "view_dom_id":
        "564c33262573b6ee5ad8d0673ea91d3d6b9012a98daa9de483dd48f5bcd240c0",
        "pager_element":
        0,
        "page":
        page_num,
        "_drupal_ajax":
        1,
        "ajax_page_state[theme]":
        "insead_core",
        "ajax_page_state[theme_token]":
        "",
        "ajax_page_state[libraries]":
        "eJx9Vu2WgygMfSGp77F_9wE4AVJlisSF2I7z9BvFflhpz5kzxXuvIMlNwIC9aCb5G1vzHOuf3JgPFOMvNwaZMWn8HSmj02cf5DG3MIkoT2bwHyUdRkwQGhPgb26NpxP8wG9jKVAy9Ns6PMMUuME_S5ExssbBoGvfnrXN-UXDPQ7YnqcQ1M077pXp1PLRXaIpuuYMFjm3Lk0jhFN5Otke7UWWXN7okOsil2h0dItfRfc9Vcmrx1tW6yYLoh0w6gSxw3YZqnXYnCkN7Mfc3geNj9lpiBBm9ja366OBKGtpS8NIUXZeFdkLOs-UdPDxkjVeRVifTnKJSSBRZvax0_eAfl-gzMtJIiwvVSUZIdn-u2Yl787YFLRxMhC5H4A9xY1L8mrANnMij69xbLavMgnS3L4-nGwQ-2kzMVPUveyS0vxFvrerznO0X9RdIAPhn_xFwjdEBhNQrYXzWVgMlseEcMG0Fya0kop_15AWZoQEXYKxz89UbZGrUqcoLtSZEm9bK9Pc0Cx20xjovwna8qNek4YgfqKELVj5cVsynqgTpwkIQa2m2JHFrHuIJBGDqjAWklODGBc-wKpL3h05Mj9o-YhL0PdgcfY21yLJVT4z8J5ZfHYAlI_jtF_23rpesVLye4iI3_ZeIPm0ONXwKyQP8mmSyh1d7LeDehjMlLraZL2MvkwmpdZVkNW2j5e4T_gmipJ3PK52wbkSysUj0kqlQUNKdHvjMteyWRLm7ZvzBuzguOwClAWO-xnIvUVrkHR59SyIBzHKu29IAB9rvh0TSbUNA8o80X0kI1x992hmD0HCjCwngBTWDi_ts5hMLSX6xtplJsWepbFY3pdMHkDOwcXhe5islzI9hGxpidJOa2FYu6X6VGZy0K-zqbCvNKm8fdbZD7i4pAoeP_TB9PJhf1KWb1l78FdcTgTZ07pigJkmllbvg6ShXa0q1wq9BevA36jCOp8tybxzK_UhfEPmPGW7nDQ4gA-PIn922ft5_0ROk6TNiJ97dM12EMLo9XI9WlpyQL5n-IA3JU16dX05aKUNSAxumGnA0_NgrgiP0Gm9GTV5zoyDNOSMzdU7pO0WdfYYXHu_fFUo8acsl_1Vor2QzXqdadf_JymoKWCBtI_ify9xyjZRCEWi7qgq6P833RZ2"
    }

    try:
        response = scraper.get(AJAX_URL,
                               headers=HEADERS,
                               params=params,
                               timeout=20)
        logging.debug(f"📡 Status Code: {response.status_code}")
        response.raise_for_status()

        data = response.json()
        logging.debug(f"✅ AJAX JSON length: {len(data)}")

        for item in data:
            if item.get("command") == "insert" and "data" in item:
                soup = BeautifulSoup(item["data"], "html.parser")
                return soup.select("div.story-card-object")

        logging.warning("⚠️ No 'insert' content block found in AJAX JSON.")
    except Exception as e:
        logging.error(f"❌ Failed to fetch AJAX page {page_num}: {e}",
                      exc_info=True)

    return []


# --- Process Articles and Upload ---
def process_and_add_articles(soup_cards, existing_urls, table):
    added = 0
    skipped = 0

    for card in soup_cards:
        current_article_url = "N/A"
        try:
            link_tag = card.select_one("h3.list-object__heading a.h3__link")
            if not link_tag:
                continue

            current_article_url = normalize_url(
                urljoin(BASE_URL, link_tag['href']))
            if current_article_url in existing_urls:
                skipped += 1
                continue

            title = link_tag.get_text(strip=True)
            image_url = ""
            image_tag = card.select_one("a.link--image-overlay img")
            if image_tag:
                image_src = image_tag.get("src") or image_tag.get("data-src")
                if image_src:
                    image_url = urljoin(BASE_URL, image_src)

            pub_date = extract_date_from_tag(card.select_one("time[datetime]"))

            fields = {
                FIELD_TITLE: title,
                FIELD_IMAGE_URL: image_url,
                FIELD_ARTICLE_URL: current_article_url,
            }
            if pub_date:
                fields[FIELD_PUBLICATION_DATE] = pub_date

            table.create(fields)
            existing_urls.add(current_article_url)
            added += 1
            logging.info(f"✅ ADDED: {title} | {current_article_url}")
        except Exception as e:
            logging.error(
                f"❌ Error processing article ({current_article_url}): {e}",
                exc_info=True)

    return added, skipped


# --- Main Logic ---
def main():
    logging.info("🚀 Starting INSEAD AJAX Scraper")
    MAX_PAGES = 10

    if not AIRTABLE_API_KEY:
        logging.error("❌ AIRTABLE_API_KEY missing.")
        print("❌ AIRTABLE_API_KEY missing.")
        return

    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_ID)

    existing_urls = set()
    try:
        for record in table.all():
            url = record.get("fields",
                             {}).get(AIRTABLE_ARTICLE_URL_COLUMN_NAME)
            if url:
                existing_urls.add(normalize_url(url))
        logging.info(
            f"📦 Loaded {len(existing_urls)} existing URLs from Airtable")
    except Exception as e:
        logging.error(f"❌ Failed to load Airtable records: {e}", exc_info=True)
        return

    total_added = 0
    total_skipped = 0

    for page in range(MAX_PAGES):
        logging.info(f"🔁 Requesting page {page}")
        articles = fetch_ajax_page(page)
        if not articles:
            logging.info("✅ No more articles found. Ending loop.")
            break
        logging.info(f"📄 Found {len(articles)} article cards on page {page}")

        added, skipped = process_and_add_articles(articles, existing_urls,
                                                  table)
        total_added += added
        total_skipped += skipped

        time.sleep(2)

    logging.info(f"🏁 Done. Added: {total_added} | Skipped: {total_skipped}")
    print(f"✅ Done. Added: {total_added}, Skipped: {total_skipped}")


if __name__ == "__main__":
    main()
