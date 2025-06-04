import os
import cloudscraper
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime
import time
import json
import re

# --- Configuration ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tblLnvZF5bb6oj9ef" # Using the correct table ID from the initial prompt's DEBUG logs

# Airtable Field Names (using provided Field IDs for desired fields)
FIELD_TITLE = "fldEhhyuhrKxmpjl0"
FIELD_PUBLICATION_DATE = "fldJZNPnajc0SHyh9"
FIELD_IMAGE_URL = "fldy48rpwvX54YoaU"
FIELD_ARTICLE_URL = "fldUo3r63Cnh6exMR" 

# --- BASE_URL AND AJAX_ENDPOINT FOR NEWSROOM ---
BASE_URL = "https://www.insead.edu"
AJAX_ENDPOINT = "https://www.insead.edu/views/ajax"

# Logging setup
logging.basicConfig(
    filename='insead_newsroom_scrape.log', 
    filemode='w',
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Initialize cloudscraper once globally
scraper = cloudscraper.create_scraper()

# --- Helper Functions ---

def normalize_url(url):
    """Normalizes a URL by parsing and reconstructing it without query parameters or fragments."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def extract_publication_date(article_url):
    """
    Visits an individual article page to extract the publication date, prioritizing meta tags.
    Formats the date to ISO 8601 (YYYY-MM-DD).
    """
    try:
        logging.debug(f"Attempting to fetch article page for date: {article_url}")
        res = scraper.get(article_url, timeout=15)
        
        if res.status_code != 200:
            logging.error(f"Failed to load article page {article_url} for date (Status: {res.status_code}).")
            return None
        
        logging.debug(f"Successfully fetched article page {article_url} (Status: {res.status_code}). Parsing HTML for date.")
        soup = BeautifulSoup(res.content, "html.parser")
        
        # --- PRIORITY 1: Extract from <meta property="article:published_time"> tag ---
        meta_pub_date_tag = soup.find("meta", property="article:published_time")
        if meta_pub_date_tag and meta_pub_date_tag.has_attr("content"):
            date_iso_str = meta_pub_date_tag["content"]
            try:
                date_object = datetime.fromisoformat(date_iso_str.replace('Z', '+00:00')) 
                iso_date = date_object.strftime("%Y-%m-%d")
                logging.debug(f"Found and formatted date from meta tag: '{date_iso_str}' -> '{iso_date}'")
                return iso_date
            except ValueError as ve:
                logging.error(f"Failed to parse ISO date string '{date_iso_str}' from meta tag for {article_url}: {ve}", exc_info=True)
        else:
            logging.debug(f"Meta tag 'article:published_time' not found or content attribute missing for {article_url}. Falling back to link tag.")

        # --- PRIORITY 2: Fallback to existing 'a.link.link--date' selector ---
        date_tag = soup.select_one("a.link.link--date") 
        if date_tag:
            date_str = date_tag.get_text(strip=True)
            logging.debug(f"Found potential date string from link tag: '{date_str}' for {article_url}")
            try:
                date_object = datetime.strptime(date_str, "%d %b %Y")
                iso_date = date_object.strftime("%Y-%m-%d")
                logging.debug(f"Successfully parsed and formatted date from link tag: '{date_str}' -> '{iso_date}')")
                return iso_date
            except ValueError as ve:
                logging.error(f"Failed to parse date string '{date_str}' from link tag for {article_url} into '%d %b %Y' format: {ve}", exc_info=True)
                return None
        else:
            logging.warning(f"Publication date tag 'a.link.link--date' NOT found for {article_url}. HTML structure might have changed or element is missing.")
            return None
    except Exception as e:
        logging.error(f"General error during date extraction for {article_url}: {e}", exc_info=True)
        return None

def process_and_add_articles(article_cards, existing_urls, table, added_count_ref, skipped_duplicates_count_ref):
    """
    Processes a list of BeautifulSoup article card elements and adds them to Airtable.
    Includes title, date, image URL, and article URL as per the user's latest request.
    Updates the counts of added and skipped articles via mutable references.
    """
    for card in article_cards:
        current_article_url = "N/A" 
        try:
            # Refined link selection: Try to find the primary link within common heading/body elements
            link_tag = None
            # Option 1: Link directly within a heading (most common for titles)
            link_tag = card.select_one("h2 a[href], h3 a[href], h4 a[href]")
            if not link_tag:
                # Option 2: Link within the body or heading-like container if not directly in hX
                link_tag = card.select_one(".card-object__body a[href], .card-object__heading a[href]")
            if not link_tag:
                # Option 3: General link search within the card (fallback)
                link_tag = card.find("a", href=True)

            if not link_tag:
                logging.debug("Skipping card: No valid link tag with href found.")
                continue

            current_article_url = normalize_url(urljoin(BASE_URL, link_tag['href']))
            
            if current_article_url in existing_urls:
                logging.debug(f"Skipping duplicate: {current_article_url}")
                skipped_duplicates_count_ref[0] += 1
                continue

            title = link_tag.get_text(strip=True)
            
            image_figure = card.select_one(".card-object__figure")
            image_tag = None
            if image_figure:
                image_tag = image_figure.select_one("picture img")
                if not image_tag:
                    image_tag = image_figure.select_one("img")

            image_url = ""
            if image_tag:
                image_src = image_tag.get("src") or image_tag.get("data-src")
                if image_src:
                    image_url = urljoin(BASE_URL, image_src)

            pub_date = extract_publication_date(current_article_url)

            # --- Construct record_fields WITH requested fields including Article URL ---
            record_fields = {
                FIELD_TITLE: title,
                FIELD_IMAGE_URL: image_url,
                FIELD_ARTICLE_URL: current_article_url, # Now explicitly included
            }
            if pub_date: # Only add date if successfully extracted
                record_fields[FIELD_PUBLICATION_DATE] = pub_date

            table.create(record_fields)
            existing_urls.add(current_article_url)
            logging.info(f"✅ ADDED: Title: '{title}', Date: {pub_date if pub_date else 'N/A'}, Image URL: {image_url}, Article URL: {current_article_url}")
            added_count_ref[0] += 1

        except Exception as e:
            logging.error(f"❌ Failed to process article card (URL: {current_article_url}): {e}", exc_info=True)

# --- Main Scraper Logic ---

def main():
    logging.info("INSEAD Newsroom Scraper Started.")

    if not AIRTABLE_API_KEY:
        logging.error("AIRTABLE_API_KEY environment variable not set. Exiting.")
        print("❌ Error: AIRTABLE_API_KEY not set. Please configure your environment variables.")
        return

    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_ID)

    existing_urls = set()
    try:
        logging.info("Fetching existing article URLs from Airtable...")
        for record in table.all(): 
            url = record.get("fields", {}).get(FIELD_ARTICLE_URL) 
            if url: 
                existing_urls.add(normalize_url(url))
            elif record.get("fields", {}).get(FIELD_TITLE): 
                existing_urls.add(normalize_url(record.get("fields", {}).get(FIELD_TITLE))) # Fallback
        logging.info(f"Found {len(existing_urls)} existing articles in Airtable.")
    except Exception as e:
        logging.error(f"Error loading existing records from Airtable: {e}", exc_info=True)
        print(f"❌ Error loading existing records: {e}. Check Airtable config/permissions.")
        return

    added_count = [0]
    skipped_duplicates_count = [0]

    ajax_libraries_param = "" # This will still be extracted but not used in the AJAX request for now.
    view_dom_id = "2bcf87ffae10d903e48004546039697aebd0e6dc08d71cbbb4b8e009c1559405" # Initial Default/Hardcoded Value
    view_name = "newsroom_archive" # Initial Default/Hardcoded Value
    view_display_id = "news_room_archive_page" # Initial Default/Hardcoded Value


    # --- Fetch initial homepage and extract ajax_page_state[libraries] & view_dom_id ---
    initial_page_url = f"{BASE_URL}/newsroom/news"
    logging.info(f"Fetching initial homepage: {initial_page_url}") 
    try:
        response = scraper.get(initial_page_url, timeout=30) 
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, "html.parser")
        initial_cards = soup.select("div.card-object")
        logging.info(f"Found {len(initial_cards)} articles on initial load.")
        
        process_and_add_articles(initial_cards, existing_urls, table, added_count, skipped_duplicates_count)
        
        # --- Extract Drupal.settings from inline script tags ---
        drupal_settings_json = None
        # Look for script tags that contain the "Drupal.settings =" pattern
        script_tags = soup.find_all("script", string=re.compile(r"Drupal\.settings\s*="))
        for script_tag in script_tags:
            script_content = script_tag.string
            if script_content:
                # Extract the JSON part after "Drupal.settings ="
                match = re.search(r"Drupal\.settings\s*=\s*(\{.*?\});", script_content, re.DOTALL)
                if match:
                    try:
                        drupal_settings_json = json.loads(match.group(1))
                        logging.info("Successfully extracted and parsed Drupal.settings from inline script.")
                        break
                    except json.JSONDecodeError as jde:
                        logging.debug(f"Failed to decode Drupal.settings JSON from script content: {jde}")
                        continue
        
        if drupal_settings_json:
            ajax_page_state = drupal_settings_json.get('ajaxPageState', {})
            # We are extracting libraries, but will not use it in the AJAX call to debug 404
            ajax_libraries_param = ajax_page_state.get('libraries', '')
            if ajax_libraries_param:
                logging.info(f"Extracted ajax_page_state[libraries] from JSON: {ajax_libraries_param[:50]}...")
            else:
                logging.warning("ajax_page_state[libraries] not found in Drupal.settings JSON.")

            view_name_dynamically_extracted = False
            view_display_id_dynamically_extracted = False
            dom_id_dynamically_extracted = False

            views_settings = drupal_settings_json.get('views', {})
            for view_id_key, view_data in views_settings.items():
                if isinstance(view_data, dict) and 'ajax' in view_data:
                    ajax_view_data = view_data['ajax']
                    if ajax_view_data.get('view_path') == "/newsroom/news": # Match the correct view
                        if 'view_name' in ajax_view_data and 'view_display_id' in ajax_view_data:
                            view_name = ajax_view_data['view_name']
                            view_display_id = ajax_view_data['view_display_id']
                            logging.info(f"Dynamically extracted view_name: '{view_name}' and view_display_id: '{view_display_id}' by matching view_path.")
                            view_name_dynamically_extracted = True
                            view_display_id_dynamically_extracted = True
                            if 'dom_id' in ajax_view_data:
                                view_dom_id = ajax_view_data['dom_id']
                                logging.info(f"Dynamically extracted view_dom_id: '{view_dom_id}' associated with the matched view_path.")
                                dom_id_dynamically_extracted = True
                            break 

            if not dom_id_dynamically_extracted:
                logging.warning("Dynamic view_dom_id not found in Drupal.settings JSON associated with /newsroom/news. Using initial hardcoded value or existing one.")
            if not view_name_dynamically_extracted or not view_display_id_dynamically_extracted:
                logging.warning(f"Failed to dynamically extract view_name/view_display_id by view_path. Falling back to hardcoded values. Current view_name: '{view_name}', view_display_id: '{view_display_id}'")
                view_name = "newsroom_archive"
                view_display_id = "news_room_archive_page"
                logging.info(f"Using hardcoded view_name: '{view_name}' and view_display_id: '{view_display_id}'.")

        else:
            logging.warning("Drupal.settings (inline JS) not found. Falling back to regex for parameters.")
            html_content_str = response.content.decode('utf-8')
            
            # Fallback regex for ajax_page_state[libraries]
            match_libs = re.search(r'"ajaxPageState":{"libraries":"(.*?)"', html_content_str)
            if match_libs:
                ajax_libraries_param = match_libs.group(1)
                logging.info(f"Extracted ajax_page_state[libraries] via regex: {ajax_libraries_param[:50]}...")
            else:
                logging.warning("Could not extract ajax_page_state[libraries] via regex.")

            # Fallback regex for view_dom_id
            dom_id_match = re.search(r"'view_dom_id':\s*'([a-f0-9]+)'", html_content_str)
            if dom_id_match:
                view_dom_id = dom_id_match.group(1)
                logging.info(f"Extracted view_dom_id via regex: {view_dom_id}")
            else:
                logging.warning("Could not extract view_dom_id via regex. Using initial hardcoded value.")
            
            # Use hardcoded view_name/display_id as a complete fallback
            view_name = "newsroom_archive" 
            view_display_id = "news_room_archive_page"
            logging.info(f"Using hardcoded view_name: '{view_name}' and view_display_id: '{view_display_id}' due to complete fallback.")


        time.sleep(5) 

    except Exception as e:
        logging.error(f"Error fetching initial homepage or extracting AJAX state: {e}", exc_info=True)
        print(f"❌ Error fetching initial homepage or extracting AJAX state: {e}. Exiting.")
        return 

    page_num = 2 
    total_articles_fetched_from_ajax = 0 

    while True:
        request_params = {
            "_wrapper_format": "drupal_ajax",
            "view_name": view_name, 
            "view_display_id": view_display_id, 
            "view_args": "",
            "view_path": "/newsroom/news", 
            "view_base_path": "",
            "view_dom_id": view_dom_id, 
            "pager_element": "0",
            "page": page_num - 1, 
            "_drupal_ajax": "1",
            "ajax_page_state[theme]": "knowledge_theme", 
            "ajax_page_state[theme_token]": "",
            # Explicitly NOT including ajax_page_state[libraries] to debug 404
        }
        
        logging.info(f"Fetching page {page_num} via AJAX from {AJAX_ENDPOINT} with params (libraries param excluded): {request_params}")
        
        try:
            response = scraper.get(AJAX_ENDPOINT, params=request_params, timeout=30)
            response.raise_for_status() 

            response_content = response.text 

            response_json = None 
            
            try:
                response_json = json.loads(response_content) 
                logging.debug(f"Successfully parsed direct JSON for page {page_num}.")
            except json.JSONDecodeError:
                logging.debug(f"Direct JSON parsing failed for page {page_num}. Checking for textarea wrapper.")
                soup_ajax = BeautifulSoup(response_content, 'html.parser')
                textarea_tag = soup_ajax.find('textarea')
                if textarea_tag:
                    json_string = textarea_tag.text 
                    try:
                        response_json = json.loads(json_string) 
                        logging.debug(f"Successfully parsed JSON from textarea for page {page_num}.")
                    except json.JSONDecodeError as jde:
                        logging.error(f"Failed to decode JSON from textarea content for page {page_num}: {jde}", exc_info=True)
                        logging.error(f"Textarea content (first 500 chars): {json_string[:500]}...")
                        print(f"❌ Error: Textarea content for page {page_num} was not valid JSON. Halting pagination.")
                        break 
                else:
                    logging.error(f"AJAX response for page {page_num} could not be parsed as JSON (neither direct nor from textarea). Halting.")
                    break
            
            if response_json is None: 
                logging.error(f"AJAX response for page {page_num} could not be parsed as JSON. Halting.")
                break 

            new_cards_html = ""

            for command in response_json:
                logging.debug(f"Processing command: {command.get('command')}, selector: {command.get('selector')}")
                if command.get("command") == "insert" and "data" in command:
                    # Priority given to exact view-dom-id, then more general selectors
                    if command.get("selector") == f".js-view-dom-id-{view_dom_id}":
                        new_cards_html = command["data"]
                        logging.debug(f"Found 'insert' command matching dynamic view_dom_id for page {page_num}.")
                        break
                    elif command.get("selector") == f".block-views-block{view_name.replace('_', '-')}-{view_display_id.replace('_', '-')} .view-content":
                         new_cards_html = command["data"]
                         logging.debug(f"Found 'insert' command with dynamic view_name/display_id selector for page {page_num}.")
                         break
                    elif command.get("selector") == ".view-content": 
                        new_cards_html = command["data"]
                        logging.debug(f"Found 'insert' command with general .view-content selector for page {page_num}.")
                        break
                    elif command.get("selector") == "#block-insead-content": 
                        new_cards_html = command["data"]
                        logging.debug(f"Found 'insert' command with #block-insead-content selector for page {page_num}.")
                        break

            if not new_cards_html:
                logging.info(f"No HTML content found in AJAX response commands for page {page_num}. Ending pagination.")
                break 

            soup = BeautifulSoup(new_cards_html, "html.parser")
            new_cards = soup.select("div.card-object")

            if not new_cards:
                logging.info(f"No more articles found in HTML snippet for page {page_num}. Ending pagination.")
                break 
            
            total_articles_fetched_from_ajax += len(new_cards)
            logging.info(f"Fetched {len(new_cards)} articles for page {page_num}. Total articles found from AJAX so far: {total_articles_fetched_from_ajax}")
            
            process_and_add_articles(new_cards, existing_urls, table, added_count, skipped_duplicates_count)

        except Exception as e: 
            logging.error(f"Error during AJAX request or parsing for page {page_num}: {e}", exc_info=True)
            print(f"❌ Error fetching page {page_num}: {e}. Halting pagination.")
            break 

        page_num += 1
        time.sleep(5) 
    
    logging.info(f"Scraper Finished. {added_count[0]} new article(s) added. {skipped_duplicates_count[0]} duplicates skipped.")
    print(f"✅ Done. {added_count[0]} new articles added. {skipped_duplicates_count[0]} duplicates skipped. See insead_newsroom_scrape.log for details.")

if __name__ == "__main__":
    main()
