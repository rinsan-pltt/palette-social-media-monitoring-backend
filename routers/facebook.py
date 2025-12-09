import time
import json
import random
import os
import urllib.parse
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import getpass
from helpers.mongo_helper import get_sessions_collection, get_session, upsert_session
from helpers.mongo_helper import upsert_facebook_profile
import logging

logger = logging.getLogger("social_media_monitoring.facebook")

router = APIRouter(prefix="/facebook", tags=["facebook"]) 


class ScrapeRequest(BaseModel):
    profile: str
    max_posts: int = 0


def random_wait(a=1.0, b=3.0):
    time.sleep(random.uniform(a, b))


COOKIE_FILE = "fb_cookies.json"


def _normalize_fb_href(href: str):
    """Normalize Facebook hrefs.
    - Convert relative URLs to absolute
    - For /photo.php ensure 'fbid' is present and preserve query string
    - For other URLs strip query/fragment
    Returns normalized URL or None if it should be ignored (e.g., plain photo.php without fbid)
    """
    if not href:
        return None
    href = href.strip()
    if href.startswith('/'):
        href = urllib.parse.urljoin('https://www.facebook.com', href)
    # remove surrounding whitespace
    low = href.lower()
    # If it's a photo.php URL, ensure it has fbid parameter and keep the query
    if '/photo.php' in low:
        if 'fbid=' in low:
            # remove fragment but keep full query
            return href.split('#', 1)[0]
        else:
            # plain/broken photo.php without fbid is not useful
            return None
    # For other urls, strip query and fragment
    return href.split('?')[0].split('#')[0]



def scroll_page(driver, max_scrolls=50):
    """Scroll page until no new content is loaded"""
    try:
        last_height = driver.execute_script("return document.body.scrollHeight")
    except Exception:
        last_height = 0
    for _ in range(max_scrolls):
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        except Exception:
            pass
        random_wait(2, 5)
        try:
            new_height = driver.execute_script("return document.body.scrollHeight")
        except Exception:
            new_height = last_height
        if new_height == last_height:
            break
        last_height = new_height


def load_cookies_if_available(driver, cookie_file):
    """Try to load cookies from MongoDB sessions collection first, then fallback to a local
    cookie file. Returns a tuple (authenticated: bool, source: str) where source is one of
    'mongo', 'local', or 'none'."""
    source = 'none'
    cookies = None

    # First try to load cookies from MongoDB sessions collection (platform='facebook')
    try:
        logger.info("Attempting to load Facebook cookies from MongoDB sessions collection")
        # Try a few common session document shapes
        doc = get_session({"platform": "facebook"})
        if not doc:
            doc = get_session({"type": "facebook_cookies"})
        if not doc:
            doc = get_session({"type": "facebook"})

        if doc is None:
            logger.info("No Facebook session document found in MongoDB sessions collection")
            cookies = None
        else:
            logger.debug("Found session document in MongoDB: %s", {k: v for k, v in doc.items() if k != 'cookies'})
            cands = doc.get("cookies") or doc.get("cookie") or doc.get("session")
            if isinstance(cands, list) and cands:
                cookies = cands
                source = 'mongo'
                logger.info("Loaded %d cookies from MongoDB session document", len(cookies))
            else:
                logger.info("Session document present but no cookie list found")
                cookies = None
    except Exception as e:
        logger.debug("Error while reading session document from MongoDB: %s", e)
        cookies = None

    # If no cookies found in DB, fall back to local cookie file
    if not cookies:
        if not os.path.exists(cookie_file):
            return False, source
        try:
            with open(cookie_file, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            source = 'local'
            logger.info("Loaded %d cookies from local cookie file: %s", len(cookies) if isinstance(cookies, list) else 0, cookie_file)
        except Exception as e:
            logger.debug("Failed to read local cookie file %s: %s", cookie_file, e)
            cookies = None

    if not cookies:
        return False, source

    try:
        driver.get('https://www.facebook.com/')
        added = 0
        for cookie in cookies:
            # accept both httpOnly and httponly naming
            c = cookie.copy()
            if 'httponly' in c and 'httpOnly' not in c:
                c['httpOnly'] = c.pop('httponly')
            allowed = {k: v for k, v in c.items() if k in ('name', 'value', 'domain', 'path', 'expiry', 'httpOnly', 'secure')}
            try:
                driver.add_cookie(allowed)
                added += 1
            except Exception as e:
                logger.debug("Skipping cookie %s due to add_cookie error: %s", cookie.get('name'), e)
                continue
        logger.info("Attempted to inject cookies, successfully added %d/%d (source=%s)", added, len(cookies), source)
        driver.refresh()
        random_wait(3, 5)
        try:
            driver.find_element(By.NAME, 'email')
            # found the login email input: still logged out
            return False, source
        except NoSuchElementException:
            # email input not found: likely logged in
            return True, source
    except Exception as e:
        logger.debug("Exception during cookie injection: %s", e)
        return False, source


def perform_login_and_save_cookies(driver, cookie_file, email=None, pwd=None):
    """
    Perform login using provided credentials (or interactive fallback) and save cookies to local file
    and to the Mongo `sessions` collection (platform='facebook'). Returns True on successful login.
    """
    driver.get('https://www.facebook.com/login')
    random_wait(2, 4)
    try:
        # If credentials not provided, fall back to interactive prompt
        if not email or not pwd:
            try:
                email = os.getenv('FB_EMAIL') or input('Facebook email: ').strip()
            except Exception:
                email = None
            try:
                pwd = os.getenv('FB_PASSWORD') or getpass.getpass('Facebook password (input hidden): ')
            except Exception:
                pwd = None

        if not email or not pwd:
            print('No Facebook credentials available; cannot perform automated login')
            return False

        email_input = WebDriverWait(driver, 20).until(lambda d: d.find_element(By.NAME, 'email'))
        pass_input = driver.find_element(By.NAME, 'pass')
        for ch in str(email):
            email_input.send_keys(ch)
            time.sleep(random.uniform(0.03, 0.12))
        time.sleep(0.3)
        for ch in str(pwd):
            pass_input.send_keys(ch)
            time.sleep(random.uniform(0.03, 0.12))
        login_btn = driver.find_element(By.NAME, 'login')
        login_btn.click()
        random_wait(5, 8)

        # Check if still on login page
        try:
            driver.find_element(By.NAME, 'email')
            print('Login appears to have failed or additional challenge required')
            return False
        except NoSuchElementException:
            pass

        # Save cookies to local file
        try:
            cookies = driver.get_cookies()
            with open(cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f)
            # Also upsert into MongoDB sessions collection
            try:
                upsert_session({"platform": "facebook"}, {"platform": "facebook", "cookies": cookies, "updated_at": int(time.time())})
            except Exception as e:
                print(f'Failed to upsert cookies to MongoDB sessions collection: {e}')

            print(f'Cookies saved to {cookie_file} and sessions collection')
        except Exception as e:
            print(f'Failed to save cookies locally or to DB: {e}')
        return True
    except Exception as e:
        print(f'Login error: {e}')
        return False


def manual_capture_and_save_cookies(cookie_file, timeout=300):
    """Open a visible Chrome window and let the user manually log in (handles 2FA). Polls
    until the login appears successful or `timeout` seconds elapse. On success saves cookies
    to both `cookie_file` and MongoDB sessions collection.
    Returns True if cookies were captured and saved, False otherwise."""
    from selenium.webdriver.chrome.options import Options

    logger.info("Opening visible Chrome for manual Facebook login (you may need to complete 2FA/challenge)...")
    chrome_options = Options()
    # Visible browser (no headless)
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--window-size=1200,900')
    chrome_options.add_argument('--lang=en-US')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-infobars')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    try:
        try:
            driver.get('https://www.facebook.com/login')
        except Exception:
            try:
                driver.get('https://www.facebook.com')
            except Exception:
                pass

        start = time.time()
        while time.time() - start < timeout:
            time.sleep(4)
            try:
                # If email input is not present, assume logged in (best-effort)
                els = driver.find_elements(By.NAME, 'email')
                if not els:
                    # success — capture cookies
                    try:
                        cookies = driver.get_cookies()
                        # write local file
                        try:
                            with open(cookie_file, 'w', encoding='utf-8') as f:
                                json.dump(cookies, f)
                        except Exception as e:
                            logger.debug("Failed to write local cookie file: %s", e)
                        # upsert into Mongo
                        try:
                            upsert_session({"platform": "facebook"}, {"platform": "facebook", "cookies": cookies, "updated_at": int(time.time())})
                        except Exception as e:
                            logger.exception("Failed to upsert cookies to MongoDB during manual capture: %s", e)
                        logger.info("Manual login detected and cookies saved (count=%d)", len(cookies))
                        return True
                    except Exception as e:
                        logger.exception("Error capturing cookies after manual login: %s", e)
                        return False
                else:
                    # still showing email input — continue waiting
                    logger.info("Waiting for manual login to complete... (%ds remaining)", int(timeout - (time.time() - start)))
            except Exception as e:
                logger.debug("Polling error while waiting for manual login: %s", e)
                time.sleep(2)

        logger.info("Manual login timed out after %d seconds", timeout)
        return False
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# Copied and adapted from b.py
def scroll_and_extract(driver, profile, max_attempts=50, no_height_threshold=3, stop_after=None):
    post_links, reels_links = set(), set()
    patterns = ['/reel/', '/posts/', '/photos/', '/photo.php', '/videos/',
                '/permalink.php', '/permalink', '/watch', '/story.php', '/stories/']
    profile_patterns = [f'/{profile}/posts', f'/{profile}/reel', f'/{profile}/videos', f'/{profile}/photos']

    last_count = 0
    attempts = 0
    no_height_increase = 0
    try:
        last_height = driver.execute_script("return document.body.scrollHeight")
    except Exception:
        last_height = 0

    import re
    while attempts < max_attempts:
        # Scroll once
        try:
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
        except Exception:
            pass
        time.sleep(2 + random.random() * 2)

        # Check page height
        try:
            new_height = driver.execute_script("return document.body.scrollHeight")
        except Exception:
            new_height = last_height
        if new_height == last_height:
            no_height_increase += 1
        else:
            no_height_increase = 0
        last_height = new_height

        # Try to expand collapsed posts
        try:
            see_more_buttons = driver.find_elements(By.XPATH, "//div[contains(text(), 'See More')]")
        except Exception:
            see_more_buttons = []
        for btn in see_more_buttons:
            try:
                btn.click()
                time.sleep(0.3)
            except Exception:
                continue

        # Extract anchors
        try:
            anchors = driver.find_elements(By.XPATH, "//a[@href]")
        except Exception:
            anchors = []
        for el in anchors:
            try:
                href = el.get_attribute('href')
                if not href:
                    continue
                norm = _normalize_fb_href(href)
                if not norm:
                    continue
                lu = norm.lower()
                if '/reel/' in lu:
                    reels_links.add(norm)
                elif any(p in lu for p in patterns) or any(pp in lu for pp in profile_patterns):
                    post_links.add(norm)
            except Exception:
                continue

        # Also look inside attributes for embedded URLs (onclick, data-store, data-ft)
        try:
            elems_with_attrs = driver.find_elements(By.XPATH, "//*[@onclick or @data-store or @data-gt or @data-ft]")
        except Exception:
            elems_with_attrs = []
        for el in elems_with_attrs:
            try:
                text = ' '.join(filter(None, [el.get_attribute('onclick') or '', el.get_attribute('data-store') or '', el.get_attribute('data-gt') or '', el.get_attribute('data-ft') or '']))
                for m in re.finditer(r"(https?://[\\w\\-\\.\\/:?=&%]+|/photo\.php\?fbid=[0-9]+[\\w\\-\\./:?=&%]*)", text):
                    href = m.group(1)
                    if href.startswith('/'):
                        href = urllib.parse.urljoin('https://www.facebook.com', href)
                    clean_url = href.split('?')[0].split('#')[0]
                    lu = clean_url.lower()
                    if any(p in lu for p in patterns) or any(pp in lu for pp in profile_patterns) or '/photo.php' in lu:
                        if '/reel/' in lu or '/videos/' in lu:
                            reels_links.add(clean_url)
                        else:
                            post_links.add(clean_url)
            except Exception:
                continue

        total_count = len(post_links) + len(reels_links)
        if total_count == last_count:
            attempts += 1
        else:
            attempts = 0
        last_count = total_count

        # If caller requested to stop after a certain number of links, honor it
        if stop_after and total_count >= stop_after:
            break

        # Stop early when page height hasn't grown for several rounds and we've seen no new links
        if no_height_increase >= no_height_threshold and attempts > 0:
            break

    return list(post_links), list(reels_links)


# Extract photo links from the /photos page (photo album view)
def extract_photos_page(driver, profile, max_attempts=40, no_height_threshold=3, stop_after=None):
    photo_links = []
    photos_url = f"https://www.facebook.com/{profile}/photos"
    try:
        driver.get(photos_url)
    except Exception:
        return photo_links
    random_wait(2, 5)

    import re

    last_count = 0
    attempts = 0
    no_height_increase = 0
    try:
        last_height = driver.execute_script("return document.body.scrollHeight")
    except Exception:
        last_height = 0

    # Repeatedly scroll and collect all image anchor hrefs and embedded photo.php links
    while attempts < max_attempts:
        # Collect anchors that wrap images
        try:
            images = driver.find_elements(By.XPATH, "//img")
        except Exception:
            images = []

        for img in images:
            try:
                # find ancestor anchor
                parent = img
                href = None
                for _ in range(3):
                    parent = parent.find_element(By.XPATH, './..')
                    if parent.tag_name.lower() == 'a':
                        href = parent.get_attribute('href')
                        break
                if href:
                    norm = _normalize_fb_href(href)
                    if not norm:
                        continue
                    if norm not in photo_links and ('/photo.php' in norm.lower() or '/photos/' in norm.lower()):
                        photo_links.append(norm)
                        if stop_after and len(photo_links) >= stop_after:
                            return photo_links
            except Exception:
                continue

        # Additionally scan anchors directly for photo.php or /photos/
        try:
            anchors = driver.find_elements(By.XPATH, "//a[@href]")
        except Exception:
            anchors = []
        for el in anchors:
            try:
                href = el.get_attribute('href')
                if not href:
                    continue
                norm = _normalize_fb_href(href)
                if not norm:
                    continue
                if norm not in photo_links and ('/photo.php' in norm.lower() or '/photos/' in norm.lower()):
                    photo_links.append(norm)
                    if stop_after and len(photo_links) >= stop_after:
                        return photo_links
            except Exception:
                continue

        # Also extract from attributes (onclick, data-store, data-ft)
        try:
            elems = driver.find_elements(By.XPATH, "//*[@onclick or @data-store or @data-gt or @data-ft]")
        except Exception:
            elems = []
        for el in elems:
            try:
                text = ' '.join(filter(None, [el.get_attribute('onclick') or '', el.get_attribute('data-store') or '', el.get_attribute('data-gt') or '', el.get_attribute('data-ft') or '']))
                for m in re.finditer(r"(https?://[\\w\\-\\.\\/:?=&%]+|/photo\\.php\\?fbid=[0-9]+[\\w\\-\\./:?=&%]*)", text):
                    href = m.group(1)
                    norm = _normalize_fb_href(href)
                    if not norm:
                        continue
                    if norm not in photo_links and ('/photo.php' in norm.lower() or '/photos/' in norm.lower()):
                        photo_links.append(norm)
                        if stop_after and len(photo_links) >= stop_after:
                            return photo_links
            except Exception:
                continue

        # Scroll once and re-evaluate page height
        try:
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
        except Exception:
            pass
        time.sleep(1 + random.random() * 2)
        try:
            new_height = driver.execute_script("return document.body.scrollHeight")
        except Exception:
            new_height = last_height
        if new_height == last_height:
            no_height_increase += 1
        else:
            no_height_increase = 0
        last_height = new_height

        total_count = len(photo_links)
        if total_count == last_count:
            attempts += 1
        else:
            attempts = 0
        last_count = total_count

        if stop_after and total_count >= stop_after:
            break
        if no_height_increase >= no_height_threshold and attempts > 0:
            break

    return photo_links

# Comment scraping logic (adapted from a.py)
def scrape_comments_for_url(driver, fb_url):
    try:
        driver.get(fb_url)
    except Exception:
        return {"post_url": fb_url, "comments": []}
    random_wait(6, 12)
    # Randomly scroll the page a few times
    scroll_times = random.randint(2, 5)
    for _ in range(scroll_times):
        try:
            driver.execute_script("window.scrollBy(0, window.innerHeight / 2);")
        except Exception:
            pass
        random_wait(1, 4)

    # Click comment expansion buttons
    try:
        while True:
            buttons = driver.find_elements(By.XPATH, "//span[text()='View more comments' or text()='View previous comments']")
            if not buttons:
                break
            btn = random.choice(buttons)
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                random_wait(0.5, 1.5)
                btn.click()
                random_wait(1.5, 4)
            except Exception:
                break
    except Exception:
        pass

    # Wait for comments to appear (best-effort)
    try:
        WebDriverWait(driver, random.randint(15, 30)).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@aria-label,'Comment by')]") )
        )
    except Exception:
        pass

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    comments = soup.find_all("div", {"aria-label": lambda x: x and x.startswith("Comment by")})
    url_comments = []
    for c in comments:
        try:
            # Try several strategies to find the commenter's name
            name = None

            # 1) aria-label often contains "Comment by {Name}" — extract name
            aria = c.get('aria-label')
            if aria and aria.lower().startswith('comment by'):
                # remove prefix like 'Comment by ' or 'Comment by:'
                name = aria.split(' ', 2)[-1].strip(': ').strip()

            # 2) strong tag often contains the author name
            if not name:
                strong = c.find('strong')
                if strong and strong.get_text(strip=True):
                    name = strong.get_text(strip=True)

            # 3) look for an anchor or span with a profile link or role
            if not name:
                author_link = c.find(['a', 'span'], href=True)
                if author_link and author_link.get_text(strip=True):
                    name = author_link.get_text(strip=True)

            # 4) fallback: find first child with text that looks like a name (heuristic)
            if not name:
                first_text = None
                for child in c.find_all(recursive=False):
                    txt = child.get_text(strip=True)
                    if txt and len(txt) <= 50 and '\n' not in txt:
                        first_text = txt
                        break
                name = first_text

            # Normalize final name value
            if not name:
                name = "Unknown"

            # Clean up name if it contains relative time info like '4 years ago'
            posted_before = None
            try:
                import re as _re
                # match patterns like '4 years ago', '1 week ago', '2 months ago', 'yesterday', 'just now',
                # and variants like 'a week ago' or 'an hour ago'
                time_match = _re.search(r"(\b(?:\d+|a|an)\s+(?:years?|year|months?|month|weeks?|week|days?|day|hours?|hour|hrs?|minutes?|minute|mins?|seconds?|second|secs?)\s+ago\b|\byesterday\b|\btoday\b|\bjust now\b)", name, flags=_re.IGNORECASE)
                if time_match:
                    posted_before = time_match.group(1).strip()
                    # remove the matched token from the name
                    name = _re.sub(_re.escape(posted_before), '', name, flags=_re.IGNORECASE).strip(' ,·-')
            except Exception:
                posted_before = None

            # Extract comment text — look for divs with dir="auto" or aria-label child
            text_div = c.find("div", {"dir": "auto"}) or c.find("span", {"dir": "auto"})
            text = text_div.get_text(strip=True) if text_div else ""
            if not text:
                # As a fallback, try to find any paragraph/span inside the comment container
                txt_el = c.find(['span', 'p'], string=True)
                text = txt_el.get_text(strip=True) if txt_el else ""

            if text:
                # If text is only emoji/symbols (no alphanumeric) or equals the username, treat as blank
                try:
                    import re as _re
                    stripped = _re.sub(r"\s+", "", text)
                    has_alnum = bool(_re.search(r"[\w\p{L}]", stripped)) if hasattr(_re, 'search') else bool(_re.search(r"[A-Za-z0-9]", stripped))
                except Exception:
                    # fallback: basic alnum check
                    has_alnum = any(ch.isalnum() for ch in text)

                final_text = text
                if not has_alnum:
                    final_text = ""
                # If comment text is same as extracted name, assume it's not a real comment
                if final_text.strip() and final_text.strip() == name.strip():
                    final_text = ""

                comment_obj = {"name": name, "comment": final_text}
                # mark emoji-only comments explicitly
                if not final_text and not has_alnum:
                    comment_obj["is_emoji_only"] = True
                if posted_before:
                    comment_obj["posted_before"] = posted_before
                url_comments.append(comment_obj)
        except Exception:
            continue
    return {"post_url": fb_url, "comments": url_comments}


@router.post('/scrape')
def facebook_scrape(req: ScrapeRequest):
    profile = req.profile
    max_posts = req.max_posts or 0
    # results will be saved to MongoDB; no local output filename needed

    from selenium.webdriver.chrome.options import Options
    chrome_options = Options()
    # Run in headless mode for backend scraping
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--lang=en-US')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-infobars')
    chrome_options.add_argument('--blink-settings=imagesEnabled=true')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    try:
        try:
            driver.maximize_window()
        except Exception:
            # In headless mode maximize may fail; ignore
            pass

        # Always attempt to load cookies from Mongo (load_cookies_if_available does that)
        logger.info("Starting Facebook scrape for profile: %s (max_posts=%s)", profile, max_posts)

        logged, cookie_source = load_cookies_if_available(driver, COOKIE_FILE)
        logger.info("Cookie load result: authenticated=%s, source=%s", bool(logged), cookie_source)
        if not logged:
            # No valid cookies; attempt automated login using env credentials
            fb_email = os.getenv('FB_EMAIL')
            fb_password = os.getenv('FB_PASSWORD')
            login_ok = False
            try:
                login_ok = perform_login_and_save_cookies(driver, COOKIE_FILE, email=fb_email, pwd=fb_password)
            except Exception:
                login_ok = False
            logger.info("Login attempt result: %s", bool(login_ok))
            if not login_ok:
                logger.warning("Automated credential login failed. Attempting manual interactive capture as fallback.")
                try:
                    manual_ok = manual_capture_and_save_cookies(COOKIE_FILE, timeout=300)
                except Exception as e:
                    logger.exception("Manual capture raised exception: %s", e)
                    manual_ok = False

                if manual_ok:
                    # after manual capture saved cookies to Mongo/local, try loading them into current driver
                    logger.info("Manual capture succeeded; reloading cookies into headless driver")
                    try:
                        logged_after, src_after = load_cookies_if_available(driver, COOKIE_FILE)
                        logger.info("Post-manual capture cookie load result: authenticated=%s, source=%s", bool(logged_after), src_after)
                        if logged_after:
                            logged = True
                        else:
                            logger.error("Manual capture completed but session still not authenticated after injection")
                    except Exception as e:
                        logger.exception("Failed to reload cookies after manual capture: %s", e)

                if not logged:
                    logger.error("Facebook login failed; no valid cookies available and automated login failed")
                    raise HTTPException(status_code=401, detail='Facebook login failed; provide valid session cookies in MongoDB sessions collection or perform manual login to capture cookies')

        # Step 1: collect photos page links first
        if max_posts and max_posts > 0:
            photo_links = extract_photos_page(driver, profile, stop_after=max_posts)
        else:
            photo_links = extract_photos_page(driver, profile)
        # Step 2: collect remaining post/reel links
        if max_posts and max_posts > 0:
            post_links, reels_links = scroll_and_extract(driver, profile, stop_after=max_posts)
        else:
            post_links, reels_links = scroll_and_extract(driver, profile)
        all_links = photo_links + post_links + reels_links
        # Deduplicate while preserving order
        seen = set()
        ordered = []
        for u in all_links:
            if u not in seen:
                seen.add(u)
                ordered.append(u)
        if max_posts and max_posts > 0:
            ordered = ordered[:max_posts]

        results = []
        posts_for_db = []
        for i, u in enumerate(ordered, start=1):
            logger.info("Scraping post %d/%d: %s", i, len(ordered), u)
            d = scrape_comments_for_url(driver, u)
            logger.info("Scraped %d comments from %s", len(d.get('comments', [])), u)
            results.append(d)
            posts_for_db.append({
                "post_url": d.get("post_url"),
                "comments": d.get("comments", []),
                "scraped_at": int(time.time())
            })

        # Upsert into MongoDB facebook collection using profile-based structure
        try:
            upsert_res = upsert_facebook_profile(profile, posts_for_db)
            logger.info("Upserted %d posts for profile %s: %s", len(posts_for_db), profile, upsert_res)
        except Exception as e:
            logger.exception("Failed to save results to MongoDB: %s", e)
            raise HTTPException(status_code=500, detail=f"Failed to save results to MongoDB: {e}")

        return {"profile": profile, "scraped": len(results), "db": upsert_res}
    finally:
        driver.quit()
