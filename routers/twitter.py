"""
Twitter Comment Scraper FastAPI Router
"""

import json
import time
import os
import re
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from ddgs import DDGS
from helpers.mongo_helper import get_sessions_collection, get_session, upsert_twitter_profile, upsert_session
from bs4 import BeautifulSoup

router = APIRouter(prefix="/twitter", tags=["twitter"])

# Request/Response models
class TwitterScrapeRequest(BaseModel):
    brand_name: str
    max_users: int = 3
    max_tweets_per_user: int = 5

class TwitterScrapeResponse(BaseModel):
    success: bool
    message: str
    brand_name: str
    total_tweets: int
    total_comments: int
    results: List[Dict[str, Any]]

class TwitterCommentScraper:
    def __init__(self):
        self.driver = None
        
    def setup_driver(self):
        """Initialize Chrome driver with session cookies from MongoDB"""
        options = Options()
        # Headless controlled via env var; default to non-headless for reliability
        headless_flag = os.getenv("TWITTER_HEADLESS", "false").lower()
        is_headless = headless_flag in ("1", "true", "yes")
        self.is_headless = is_headless
        if is_headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            # Provide a desktop user-agent to avoid mobile/reduced views
            ua = os.getenv('TW_USER_AGENT') or 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            options.add_argument(f"--user-agent={ua}")
            print("Running Chrome in headless mode; using desktop user-agent and larger window-size")
        else:
            options.add_argument("--start-maximized")
            options.add_argument("--window-size=1920,1080")
            print("Running Chrome with UI (non-headless); set TWITTER_HEADLESS=1 to run headless")
       
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        options.add_experimental_option("prefs", {"profile.default_content_setting_values.notifications": 2})
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), 
            options=options
        )
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Load Twitter/X homepage first
        print("Loading Twitter/X...")
        self.driver.get("https://x.com/")
        # give extra time when headless so client-side rendering finishes
        if getattr(self, 'is_headless', False):
            time.sleep(6)
        else:
            time.sleep(3)
        
        # Load cookies from MongoDB
        # Optional: allow loading cookies from a local file for debugging (TW_COOKIES_FILE)
        local_cookie_file = os.getenv('TW_COOKIES_FILE')
        cookies_loaded = False
        if local_cookie_file and os.path.exists(local_cookie_file):
            try:
                print(f"🔁 Loading cookies from local file: {local_cookie_file}")
                with open(local_cookie_file, 'r', encoding='utf-8') as cf:
                    file_cookies = json.load(cf)
                # Reuse the same acceptance logic as load_cookies_from_mongo
                current_domain = "x.com"
                from selenium.common.exceptions import InvalidCookieDomainException
                loaded = 0
                for cookie in file_cookies:
                    cookie_to_add = cookie.copy()
                    if 'domain' in cookie_to_add and current_domain not in str(cookie_to_add.get('domain', '')):
                        cookie_to_add.pop('domain', None)
                    try:
                        self.driver.add_cookie(cookie_to_add)
                        loaded += 1
                    except InvalidCookieDomainException:
                        print(f"   ⚠️ Skipping cookie due to domain mismatch: {cookie.get('name')}")
                    except Exception as e:
                        print(f"   Skipping invalid cookie: {e}")
                print(f"Local cookies accepted: {loaded}/{len(file_cookies)}")
                self.driver.refresh()
                time.sleep(4)
                cookies_loaded = True
            except Exception as e:
                print(f"Failed to load local cookie file: {e}")

        if not cookies_loaded:
            cookies_loaded = self.load_cookies_from_mongo()
        if not cookies_loaded:
            print("\nCannot proceed without valid authentication cookies.")
            if self.driver:
                self.driver.quit()
            return False
        
        return True

    def safe_scroll_into_view(self, element):
        """Scroll element into view only if it's below the current viewport to avoid jumping up."""
        try:
            rect_top = self.driver.execute_script("return arguments[0].getBoundingClientRect().top;", element)
            viewport_height = self.driver.execute_script("return window.innerHeight || document.documentElement.clientHeight;")
            # If element is already visible or above, avoid scrolling up; only scroll when it's below viewport
            if rect_top > viewport_height - 40:
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                    time.sleep(0.25)
                except Exception:
                    pass
        except Exception:
            # fallback to default scrollIntoView
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                time.sleep(0.25)
            except Exception:
                pass

    def safe_click(self, element):
        """Click an element only if it is unlikely to be a 'Like' button or a navigation anchor.
        Returns True if clicked, False if skipped.
        """
        try:
            # Avoid clicking anchors that would navigate away, unless caller explicitly allows anchors
            try:
                closest_href = self.driver.execute_script(
                    "return (function(el){var a=el.closest('a'); return a? a.getAttribute('href') : null;})(arguments[0]);",
                    element
                )
            except Exception:
                closest_href = None

            # allow_anchor will be passed by caller when clicking expansion 'Read/Show' anchors
            allow_anchor = getattr(element, '_allow_anchor_click', False)
            if closest_href and isinstance(closest_href, str) and closest_href.strip() and not allow_anchor:
                # if href looks like a full tweet link, avoid clicking
                if not closest_href.strip().startswith('javascript') and not closest_href.strip().startswith('#'):
                    return False

            # Avoid clicking elements that are probably Like buttons: look for closest ancestor with data-testid containing 'like' or aria-label containing 'Like'
            try:
                like_ancestor = self.driver.execute_script(
                    "return (function(el){var p=el.closest('[data-testid]'); while(p){ if(p.getAttribute('data-testid') && p.getAttribute('data-testid').toLowerCase().includes('like')) return p; p=p.parentElement;} return null;})(arguments[0]);",
                    element
                )
            except Exception:
                like_ancestor = None

            if like_ancestor:
                return False

            # Also check aria-labels for 'Like' nearby
            try:
                aria_like = self.driver.execute_script(
                    "return (function(el){var a=el.closest('[aria-label]'); return a? a.getAttribute('aria-label') : null;})(arguments[0]);",
                    element
                )
            except Exception:
                aria_like = None
            if aria_like and 'like' in aria_like.lower():
                return False

            # Finally, attempt to click
            try:
                element.click()
                time.sleep(0.45)
                return True
            except Exception:
                try:
                    self.driver.execute_script("arguments[0].click();", element)
                    time.sleep(0.45)
                    return True
                except Exception:
                    return False
        except Exception:
            return False
    
    def load_cookies_from_mongo(self):
        """Load cookies from MongoDB sessions collection"""
        try:
            sessions_collection = get_sessions_collection()
            session_doc = get_session({"type": "twitter_cookies"})
            
            if not session_doc:
                print("No Twitter cookies found in MongoDB sessions collection")
                return False
                
            cookies = session_doc.get("cookies", [])
            if not cookies:
                print("No cookies data found in the session document")
                return False
                
            print(f"Loading {len(cookies)} cookies from MongoDB")
            # Ensure we're on x.com so Selenium accepts cookies; Selenium requires the current
            # domain to match the cookie domain. We'll remove cookie 'domain' entries that
            # don't match the current page domain to increase acceptance.
            current_domain = "x.com"
            from selenium.common.exceptions import InvalidCookieDomainException
            loaded = 0
            for cookie in cookies:
                cookie_to_add = cookie.copy()
                if 'domain' in cookie_to_add and current_domain not in str(cookie_to_add.get('domain', '')):
                    cookie_to_add.pop('domain', None)
                try:
                    self.driver.add_cookie(cookie_to_add)
                    loaded += 1
                except InvalidCookieDomainException:
                    print(f"Skipping cookie due to domain mismatch: {cookie.get('name')}")
                except Exception as e:
                    print(f"   Skipping invalid cookie: {e}")
                    pass
            print(f"Cookies accepted: {loaded}/{len(cookies)}")
            print("Session cookies loaded successfully from MongoDB.")
            self.driver.refresh()
            time.sleep(5)
            return True
            
        except Exception as e:
            print(f"Could not load cookies from MongoDB: {e}")
            return False

    def is_logged_in(self):
        """Heuristic: determine whether the current browser session is logged in to X.
        We consider the session logged in if we can find tweet/article elements
        or profile header elements. If a login prompt appears or no tweet elements
        are present on a profile page, we treat it as logged out.
        """
        try:
            # 1) Logged-in UX usually contains a reply textarea or explicit 'Post your reply' text.
            try:
                reply_box = self.driver.find_elements(By.XPATH, "//div[contains(., 'Post your reply')] | //div[@role='textbox'] | //textarea[contains(@placeholder, 'Reply') or contains(@placeholder, 'Tweet')]")
                if reply_box and len(reply_box) > 0:
                    return True
            except Exception:
                pass

            # 2) Presence of article tweet nodes AND profile/header elements is a good indicator
            try:
                articles = self.driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
                headers = self.driver.find_elements(By.XPATH, "//div[contains(@data-testid,'primaryColumn')]//h2 | //div[contains(@data-testid,'UserName')]")
                if articles and headers and len(articles) > 0 and len(headers) > 0:
                    return True
            except Exception:
                pass

            # 3) Detect explicit login prompts (Log in / Sign in) — if present and no reply box, likely logged out
            try:
                login_buttons = self.driver.find_elements(By.XPATH, "//a[contains(., 'Log in') or contains(., 'Log in to') or contains(., 'Sign in')] | //div[contains(., 'Log in') or contains(., 'Sign in')]")
                if login_buttons and len(login_buttons) > 0:
                    return False
            except Exception:
                pass

            # 4) If we see 'Read <n> replies' prompts but no reply box, that's another sign of logged-out restricted view
            try:
                read_prompts = self.driver.find_elements(By.XPATH, "//div[contains(., 'Read') and contains(., 'replies')] | //span[contains(., 'Read') and contains(., 'replies')]")
                if read_prompts and len(read_prompts) > 0:
                    return False
            except Exception:
                pass

        except Exception:
            return False

        return False

    def ensure_logged_in(self, target_url: Optional[str] = None):
        """Ensure the webdriver is initialized and the session is authenticated.
        If not logged in, reload cookies from MongoDB and refresh the page.
        Returns True if session is authenticated, False otherwise.
        """
        if not self.driver:
            ok = self.setup_driver()
            if not ok:
                return False

        # If a target URL is provided, navigate there first
        try:
            if target_url:
                self.driver.get(target_url)
                time.sleep(3)
        except Exception:
            pass

        if self.is_logged_in():
            return True

        # Try reloading cookies from MongoDB (in case driver lost them)
        print("Session appears logged out — reloading cookies from DB and refreshing...")
        loaded = self.load_cookies_from_mongo()
        if loaded:
            try:
                self.driver.refresh()
                time.sleep(5)
            except Exception:
                pass

            if self.is_logged_in():
                print("Session restored from DB cookies")
                return True

        # If cookie restore didn't work, attempt credential login using env credentials
        print("Cookie restore failed — attempting credential login using TW_USER/TW_PASSWORD from environment...")
        tw_user = os.getenv('TW_USER')
        tw_pass = os.getenv('TW_PASSWORD')
        if not tw_user or not tw_pass:
            print("No TW_USER/TW_PASSWORD available in environment to attempt login")
            return False

        try:
            logged_in = self.login_with_credentials(tw_user, tw_pass)
            if logged_in:
                # save cookies back to Mongo
                try:
                    cookies = self.driver.get_cookies()
                    upsert_session({"type": "twitter_cookies"}, {"type": "twitter_cookies", "cookies": cookies, "updated_at": int(time.time())})
                    print(f"Saved {len(cookies)} cookies to sessions collection")
                except Exception as e:
                    print(f"Could not save cookies to MongoDB: {e}")

                return True
            else:
                print("Credential login attempt failed or session still logged out")
                return False
        except Exception as e:
            print(f"Exception during credential login attempt: {e}")
            return False

    def login_with_credentials(self, username: str, password: str) -> bool:
        """Attempt to log in to X/Twitter using provided credentials via Selenium.
        Returns True if login appears successful.
        """
        try:
            uname = username.strip()
            if uname.startswith('@'):
                uname = uname[1:]

            # navigate to login
            try:
                self.driver.get('https://x.com/login')
            except Exception:
                try:
                    self.driver.get('https://twitter.com/login')
                except Exception:
                    pass
            time.sleep(3)

            # Try several username input selectors
            username_selectors = [
                (By.NAME, 'text'),
                (By.XPATH, "//input[@name='session[username_or_email]']"),
                (By.XPATH, "//input[@autocomplete='username']"),
            ]
            username_elem = None
            for by, sel in username_selectors:
                try:
                    username_elem = self.driver.find_element(by, sel)
                    break
                except Exception:
                    username_elem = None

            if username_elem:
                try:
                    username_elem.clear()
                    username_elem.send_keys(uname)
                    time.sleep(0.6)
                except Exception:
                    pass

            # Click Next / Continue if present
            try:
                for txt in ['Next', 'Log in', 'Continue', 'Sign in']:
                    try:
                        btn = self.driver.find_element(By.XPATH, f"//div[@role='button' and contains(., '{txt}')] | //span[contains(., '{txt}')]//ancestor::div[@role='button']")
                        if btn:
                            btn.click()
                            time.sleep(1.2)
                            break
                    except Exception:
                        pass
            except Exception:
                pass

            # Find password input
            pw_elem = None
            try:
                pw_elem = self.driver.find_element(By.XPATH, "//input[@type='password']")
            except Exception:
                try:
                    pw_elem = self.driver.find_element(By.NAME, 'password')
                except Exception:
                    pw_elem = None

            if not pw_elem:
                print('Password field not found automatically during login')
                return False

            try:
                pw_elem.clear()
                pw_elem.send_keys(password)
                time.sleep(0.6)
            except Exception:
                pass

            try:
                pw_elem.submit()
            except Exception:
                try:
                    for txt in ['Log in', 'Log in to X', 'Sign in', 'Continue']:
                        try:
                            btn = self.driver.find_element(By.XPATH, f"//div[@role='button' and contains(., '{txt}')] | //span[contains(., '{txt}')]//ancestor::div[@role='button']")
                            btn.click()
                            break
                        except Exception:
                            pass
                except Exception:
                    pass

            time.sleep(5)
            # check login state
            if self.is_logged_in():
                return True
            return False
        except Exception:
            return False
    
    def search_twitter_users(self, brand_name, max_results=5):
        """Search for Twitter users/profiles using DuckDuckGo"""
        print(f"Searching Twitter for users related to '{brand_name}'...")
        user_urls = []
        # Use DuckDuckGo search results but be tolerant of different result shapes
        with DDGS() as ddgs:
            query = f"site:twitter.com OR site:x.com {brand_name}"
            try:
                for result in ddgs.text(query, max_results=max_results * 3):  # fetch extra to filter
                    # ddgs may return the link under different keys
                    url = result.get("href") or result.get("url") or result.get("link") or ""

                    if not url:
                        continue

                    # Try to extract username from common twitter/x URL patterns
                    m = re.search(r"https?://(?:mobile\.)?(?:twitter\.com|x\.com)/@?([A-Za-z0-9_]{1,50})(?:/.*)?", url)
                    if not m:
                        # sometimes URLs are incomplete or use other formats; skip those
                        # also skip individual tweet/status URLs
                        if any(x in url for x in ["/status/", "status/", "/statuses/", "intent", "i/", "/hashtag", "compose"]):
                            continue
                        # fallback: look for twitter/x domain then take the last path segment
                        try:
                            parts = url.split("/")
                            # last non-empty segment
                            segs = [p for p in parts if p]
                            if len(segs) >= 2 and ("twitter.com" in url or "x.com" in url):
                                candidate = segs[-1]
                                # validate candidate
                                if re.match(r"^[A-Za-z0-9_]{1,50}$", candidate):
                                    username = candidate
                                else:
                                    continue
                            else:
                                continue
                        except Exception:
                            continue
                    else:
                        username = m.group(1)

                    profile_url = f"https://x.com/{username}"
                    if profile_url not in user_urls and len(user_urls) < max_results:
                        user_urls.append(profile_url)
            except Exception as e:
                print(f"Search error: {e}")

        # If search didn't find anything, try direct profile checks using the browser
        if not user_urls:
            print("DDGS returned no profiles — trying direct profile URL checks in browser...")
            # Candidate username forms to try
            candidates = set()
            base = brand_name.strip()
            if base:
                candidates.add(base)
                candidates.add(base.replace(" ", ""))
                candidates.add(base.replace(" ", "_").lower())
                candidates.add(base.replace(" ", "-").lower())
                # try first token
                candidates.add(base.split()[0].lower())

            # also try lowercased brand name directly
            candidates.add(brand_name.lower())

            for cand in list(candidates):
                if len(user_urls) >= max_results:
                    break
                # sanitize candidate to username chars
                if not re.match(r"^[A-Za-z0-9_\-]{1,50}$", cand):
                    continue
                profile_url = f"https://x.com/{cand}"
                try:
                    if not self.driver:
                        continue
                    print(f"   Trying {profile_url}...")
                    self.driver.get(profile_url)
                    time.sleep(3)
                    # check for tweet elements or profile header
                    tweets = self.driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
                    header = self.driver.find_elements(By.XPATH, "//div[contains(@data-testid,'primaryColumn')]//div[contains(@class,'css-')]")
                    title = None
                    try:
                        title = self.driver.title
                    except:
                        title = None

                    if tweets and len(tweets) > 0:
                        if profile_url not in user_urls:
                            user_urls.append(profile_url)
                            print(f"   Found profile via browser: {profile_url}")
                    elif title and (cand.lower() in title.lower() or brand_name.lower() in title.lower()):
                        if profile_url not in user_urls:
                            user_urls.append(profile_url)
                            print(f"   Found profile via page title: {profile_url}")
                    else:
                        print(f"   No profile content at {profile_url}")
                except Exception as e:
                    print(f"   Error checking {profile_url}: {e}")

        print(f"Found {len(user_urls)} Twitter profiles:")
        for u in user_urls:
            print(f"   • {u}")
        return user_urls
    
    def get_tweets_from_profile(self, profile_url, max_tweets=10):
        """Extract recent tweets from a user profile"""
        print(f"\nScraping tweets from: {profile_url}")
        # Ensure we are logged in (reload cookies if necessary)
        if not self.ensure_logged_in(profile_url):
            print("Cannot scrape tweets because session is not authenticated")
            return []
        tweets = []
        
        try:
            self.driver.get(profile_url)
            time.sleep(5)
            
            # Scroll to load tweets
            for i in range(3):
                self.driver.execute_script("window.scrollBy(0, 1000);")
                time.sleep(2)
                print(f"   Scrolling... ({i+1}/3)")
            
            # Find tweet elements
            tweet_elements = self.driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
            print(f"   Found {len(tweet_elements)} tweet elements")
            
            for tweet_elem in tweet_elements[:max_tweets]:
                try:
                    # Get tweet text
                    text_elem = tweet_elem.find_element(By.XPATH, ".//div[@data-testid='tweetText']")
                    content = text_elem.text.strip()
                    
                    # Get tweet URL
                    time_elem = tweet_elem.find_element(By.XPATH, ".//time")
                    tweet_link = time_elem.find_element(By.XPATH, "./..").get_attribute("href")
                    
                    # Get timestamp
                    timestamp = time_elem.get_attribute("datetime")
                    
                    if content and tweet_link:
                        tweets.append({
                            "post_url": tweet_link,
                            "content": content,
                            "timestamp": timestamp
                        })
                except Exception as e:
                    continue
            
            print(f"Extracted {len(tweets)} tweets")
            
        except Exception as e:
            print(f"Error scraping profile: {e}")
        
        return tweets

    def get_media_from_profile(self, profile_url, max_posts=20):
        """Navigate to the user's Media tab and collect image/video post URLs (media posts)."""
        print(f"\nScraping media posts from: {profile_url}")
        # Ensure we are logged in (reload cookies if necessary)
        if not self.ensure_logged_in(profile_url):
            print("Cannot scrape media because session is not authenticated")
            return []
        media_urls = []
        try:
            media_url = profile_url.rstrip('/') + '/media'
            self.driver.get(media_url)
            time.sleep(4)

            # Scroll and collect article links that look like media posts
            last_count = 0
            attempts = 0
            while attempts < 20 and len(media_urls) < max_posts:
                articles = self.driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
                for a in articles:
                    try:
                        # Prefer anchors that point directly to media variants (photo/video)
                        link = None
                        try:
                            media_anchors = a.find_elements(By.XPATH, ".//a[contains(@href, '/photo/') or contains(@href, '/video/')]")
                            for ma in media_anchors:
                                href = ma.get_attribute('href')
                                if href and href not in media_urls:
                                    link = href
                                    break
                        except Exception:
                            link = None

                        # If no explicit media anchor was found, scan any anchors for hrefs
                        # that look like tweet URLs and keep the canonical tweet URL as candidate
                        tweet_candidate = None
                        try:
                            anchors = a.find_elements(By.XPATH, ".//a[@href]")
                            for an in anchors:
                                href = an.get_attribute('href')
                                if href and ('/status/' in href or '/statuses/' in href):
                                    tweet_candidate = href
                                    break
                        except Exception:
                            tweet_candidate = None

                        # Fallback: use the time element's parent link (canonical tweet URL)
                        if not link:
                            time_el = a.find_element(By.XPATH, ".//time")
                            tweet_href = time_el.find_element(By.XPATH, './..').get_attribute('href')
                            if tweet_candidate:
                                link = tweet_candidate
                            else:
                                link = tweet_href

                        # Normalize to full https URL if needed
                        if link and link.startswith('/'):
                            link = 'https://x.com' + link

                        # If we have a canonical tweet URL but no explicit media anchor and
                        # the post contains image/video elements, append a media suffix
                        # like '/photo/1' or '/video/1' as a best-effort fallback.
                        if link and ('/photo/' not in link and '/video/' not in link):
                            try:
                                # quick check inside the article for image/video tags
                                has_img = len(a.find_elements(By.XPATH, ".//img")) > 0
                                has_video = len(a.find_elements(By.XPATH, ".//video")) > 0
                                if has_img:
                                    candidate = link.rstrip('/') + '/photo/1'
                                    link = candidate
                                elif has_video:
                                    candidate = link.rstrip('/') + '/video/1'
                                    link = candidate
                            except Exception:
                                pass
                        if link and link not in media_urls:
                            media_urls.append(link)
                            if len(media_urls) >= max_posts:
                                break
                    except Exception:
                        continue

                # scroll a bit
                try:
                    self.driver.execute_script("window.scrollBy(0, 1000);")
                except Exception:
                    pass
                time.sleep(1.2)
                if len(media_urls) == last_count:
                    attempts += 1
                else:
                    attempts = 0
                last_count = len(media_urls)

            print(f"Found {len(media_urls)} media posts")
        except Exception as e:
            print(f"Error scraping media tab: {e}")

        # Now visit each post and extract direct media URLs (images / videos)
        media_posts = []
        for link in media_urls:
            media_list = []
            try:
                self.driver.get(link)
                time.sleep(2)
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # Collect images
                imgs = soup.find_all('img')
                for img in imgs:
                    src = img.get('src') or img.get('data-src') or img.get('data-image-url')
                    if not src:
                        srcset = img.get('srcset')
                        if srcset:
                            # pick the highest-res candidate (last)
                            parts = [p.strip() for p in srcset.split(',') if p.strip()]
                            if parts:
                                last = parts[-1].split(' ')[0]
                                src = last
                    if src and src not in media_list:
                        media_list.append(src)

                # Collect video sources/poster
                videos = soup.find_all('video')
                for v in videos:
                    # try poster or source
                    poster = v.get('poster')
                    if poster and poster not in media_list:
                        media_list.append(poster)
                    for source in v.find_all('source'):
                        s = source.get('src')
                        if s and s not in media_list:
                            media_list.append(s)

            except Exception:
                pass
            media_posts.append({'post_url': link, 'media_urls': media_list})

        # return the collected media posts (one dict per post)
        return media_posts
    
    def scrape_comments(self, tweet_url, max_comments=999999):
        """Scrape ALL comments from a specific tweet including nested replies"""
        print(f"Scraping ALL comments from: {tweet_url}")
        # Ensure we're logged in before scraping comments
        if not self.ensure_logged_in(tweet_url):
            print("Cannot scrape comments because session is not authenticated")
            return []
        # Use a map of unique_id -> text to deduplicate reliably across DOM changes
        all_comments = {}
        
        try:
            self.driver.get(tweet_url)
            # Longer initial wait to let replies render; give extra time when headless
            initial_wait = 18 if getattr(self, 'is_headless', False) else 12
            time.sleep(initial_wait)
            
            # Get the original tweet content
            original_tweet_content = ""
            try:
                first_tweet = self.driver.find_element(By.XPATH, "//div[@data-testid='tweetText']")
                original_tweet_content = first_tweet.text.strip()
                print(f"   Original tweet: {original_tweet_content[:50]}...")
            except:
                pass
            
            # Phase 1/2: Unified aggressive expansion + scrolling loop
            print(f"   Phase 1/2: Expanding threads and deep scrolling until no new comments appear...")

            # Detect if this is a media view (image/video opened in its own dialog/tab)
            media_view = False
            try:
                if '/photo/' in tweet_url or '/video/' in tweet_url:
                    media_view = True
                else:
                    # also detect presence of media dialog/image element
                    dlg_imgs = self.driver.find_elements(By.XPATH, "//div[@role='dialog']//img | //div[contains(@class,'media')]/img")
                    if dlg_imgs and len(dlg_imgs) > 0:
                        media_view = True
            except Exception:
                media_view = False

            if media_view:
                # Replies are often rendered as articles under a conversation/region next to the media
                comment_selectors = [
                    "//div[contains(@aria-label,'Conversation')]//article[@data-testid='tweet']",
                    "//div[@role='region']//article[@data-testid='tweet']",
                    "//section//article[@data-testid='tweet']",
                    "//article[@data-testid='tweet']",
                ]
            else:
                # Narrow selectors to tweet articles' tweetText to avoid sidebar/profile tokens
                comment_selectors = [
                    "//article[@data-testid='tweet']//div[@data-testid='tweetText']",
                    "//div[@role='region']//article[@data-testid='tweet']//div[@data-testid='tweetText']",
                ]

            expand_patterns = [
                "//span[contains(text(),'Show this thread')]//ancestor::div[@role='button']",
                "//span[contains(text(),'Show replies')]//ancestor::div[@role='button']",
                "//span[contains(text(),'Show more replies')]//ancestor::div[@role='button']",
                "//span[contains(text(),'Read') and contains(text(),'replies')]//ancestor::div[@role='button']",
                "//div[contains(text(),'Read') and contains(text(),'replies')]//ancestor::div[@role='button']",
                "//a[contains(text(),'Read') and contains(text(),'replies')]",
                "//div[@role='button'][contains(.,'Show')]",
                "//div[@role='button'][contains(.,'replies')]",
                "//div[@role='button'][contains(.,'more')]",
                "//div[@aria-label and contains(@aria-label,'replies')]",
            ]

            max_rounds = 80
            no_progress_limit = 12
            round_idx = 0
            last_count = len(all_comments)
            no_progress = 0

            while round_idx < max_rounds:
                # Attempt to click expand buttons (limited per round)
                clicked = 0
                for pattern in expand_patterns:
                    try:
                        buttons = self.driver.find_elements(By.XPATH, pattern)
                        for btn in buttons[:8]:
                            try:
                                # Use safer scrolling and clicking helpers to avoid jumping and accidental likes
                                try:
                                    self.safe_scroll_into_view(btn)
                                except Exception:
                                    pass
                                # allow anchors for expansion-like buttons
                                try:
                                    txt = btn.text or ''
                                except Exception:
                                    txt = ''
                                if any(k in txt for k in ['Read', 'Show', 'replies', 'Show more', 'Show this thread']):
                                    try:
                                        setattr(btn, '_allow_anchor_click', True)
                                    except Exception:
                                        pass
                                clicked_flag = self.safe_click(btn)
                                if clicked_flag:
                                    clicked += 1
                            except Exception:
                                pass
                    except Exception:
                        pass

                # Additionally, try clicking buttons that match the regex 'Read <number> replies'
                try:
                    read_buttons = self.driver.find_elements(By.XPATH, "//div[contains(.,'Read') and contains(.,'replies')] | //span[contains(.,'Read') and contains(.,'replies')] | //a[contains(.,'replies') and contains(.,'Read')]")
                    for rb in read_buttons[:6]:
                        try:
                            try:
                                self.safe_scroll_into_view(rb)
                            except Exception:
                                pass
                            try:
                                txt = rb.text or ''
                            except Exception:
                                txt = ''
                            if any(k in txt for k in ['Read', 'Show', 'replies']):
                                try:
                                    setattr(rb, '_allow_anchor_click', True)
                                except Exception:
                                    pass
                            clicked_flag = self.safe_click(rb)
                            if clicked_flag:
                                clicked += 1
                        except Exception:
                            pass
                except Exception:
                    pass

                # Do a gentle scroll to load more replies inside the thread
                try:
                    self.driver.execute_script("window.scrollBy(0, 800);")
                except Exception:
                    pass
                time.sleep(1.0)

                # Collect comments using all selectors; when articles are returned, extract their tweetText children
                for selector in comment_selectors:
                    try:
                        elements = self.driver.find_elements(By.XPATH, selector)
                        for elem in elements:
                            try:
                                text = ''
                                # If element is an article (media replies), try to find its tweetText child
                                tag = elem.tag_name.lower()
                                if tag == 'article':
                                    try:
                                        tt = elem.find_element(By.XPATH, ".//div[@data-testid='tweetText']")
                                        text = tt.text.strip()
                                    except Exception:
                                        # fallback to article text
                                        text = elem.text.strip()
                                else:
                                    text = elem.text.strip()

                                if (text and len(text) > 3 and
                                    text != original_tweet_content and
                                    not text.startswith('Replying to') and
                                    not text.startswith('Show this thread')):
                                    # Try to find a stable identifier for this reply: time element's parent href
                                    uid = None
                                    try:
                                        art = elem if elem.tag_name.lower() == 'article' else elem.find_element(By.XPATH, './ancestor::article')
                                    except Exception:
                                        art = None
                                    if art:
                                        try:
                                            time_el = art.find_element(By.XPATH, ".//time")
                                            parent = time_el.find_element(By.XPATH, './..')
                                            href = parent.get_attribute('href')
                                            if href:
                                                uid = href.rstrip('/')
                                        except Exception:
                                            uid = None

                                    # Fallback to normalized text hash-like key
                                    if not uid:
                                        norm = ' '.join(text.split())
                                        uid = f"text:{norm[:240]}"

                                    all_comments[uid] = text
                            except Exception:
                                pass
                    except Exception:
                        pass

                current_count = len(all_comments)
                if current_count > last_count:
                    no_progress = 0
                    last_count = current_count
                else:
                    no_progress += 1

                if round_idx % 5 == 0:
                    print(f"     Round {round_idx+1}/{max_rounds} - Found {current_count} comments (clicked {clicked} buttons)")

                # If we've had many rounds with no progress, break out
                if no_progress >= no_progress_limit:
                    print(f"     No progress for {no_progress} rounds, stopping aggressive collection")
                    break

                round_idx += 1
            
            # Phase 3: Alternative extraction methods
            print(f"   Phase 3: Alternative extraction methods...")
            
            # Try to extract from page source
            try:
                page_source = self.driver.page_source
                # Use BeautifulSoup to extract tweet text inside article nodes
                try:
                    soup = BeautifulSoup(page_source, 'html.parser')
                    articles = soup.find_all('article', attrs={'data-testid': 'tweet'})
                    for art in articles:
                        # look for tweetText divs inside article
                        tt = art.find(attrs={'data-testid': 'tweetText'})
                        if tt:
                            clean_text = tt.get_text(separator=' ').strip()
                            if clean_text and len(clean_text) > 3 and clean_text != original_tweet_content:
                                all_comments.add(clean_text)
                        else:
                            # fallback: collect significant span/textnodes under article
                            text_nodes = art.find_all('span')
                            for sp in text_nodes:
                                st = sp.get_text().strip()
                                if st and len(st) > 8 and st != original_tweet_content:
                                    all_comments.add(st)
                except Exception:
                    pass
            except:
                pass
            
            # Phase 4: Final targeted collection — one systematic pass without resetting scroll
            print(f"   Phase 4: Final targeted collection (single pass)...")
            # perform a finite number of gentle scrolls from current position
            for final_scroll in range(10):
                try:
                    self.driver.execute_script("window.scrollBy(0, 500);")
                except Exception:
                    pass
                time.sleep(0.8)
                
                # Final collection with all possible selectors
                final_selectors = [
                    # Target tweetText inside article elements only
                    "//article[@data-testid='tweet']//div[@data-testid='tweetText']",
                    "//div[@role='region']//article[@data-testid='tweet']//div[@data-testid='tweetText']",
                ]
                
                for selector in final_selectors:
                    try:
                        elements = self.driver.find_elements(By.XPATH, selector)
                        for elem in elements:
                            try:
                                text = elem.text.strip()
                                if (text and len(text) > 5 and text != original_tweet_content and
                                    not any(skip in text.lower() for skip in 
                                           ['replying to', 'show this', 'view', 'more replies'])):
                                    # derive stable uid similar to earlier logic
                                    uid = None
                                    try:
                                        art = elem.find_element(By.XPATH, './ancestor::article')
                                    except Exception:
                                        art = None
                                    if art:
                                        try:
                                            time_el = art.find_element(By.XPATH, ".//time")
                                            parent = time_el.find_element(By.XPATH, './..')
                                            href = parent.get_attribute('href')
                                            if href:
                                                uid = href.rstrip('/')
                                        except Exception:
                                            uid = None
                                    if not uid:
                                        norm = ' '.join(text.split())
                                        uid = f"text:{norm[:240]}"
                                    all_comments[uid] = text
                            except:
                                pass
                    except:
                        pass
            
            # Filter out known UI/promotional strings that appear when logged-out or in page chrome
            ui_blacklist = [
                'Sign up', 'Sign up with', 'Privacy Policy', 'Cookie', 'Terms of Service',
                'Create account', 'Read replies', 'Read', 'Trending', 'People on X',
                'New to X?', 'Like a post to share the love', 'Views', '©', 'Accessibility',
                'See new posts', 'Don’t miss what’s happening', 'Show more', 'Create account',
                'Sign up now', 'Open in new tab', 'Sign up with Google', 'Sign up with Apple'
            ]

            def is_ui_text(t: str) -> bool:
                if not t:
                    return True
                low = t.lower()
                # ignore purely numeric counts or very short labels
                if re.fullmatch(r"[0-9,\.\s]+(views)?", low):
                    return True
                # ignore profile sidebar tokens like '82.7K posts' or '18.4K posts'
                if re.search(r"\bposts\b", low) or re.search(r"\bfollowers?\b", low) or re.search(r"\bfollowing\b", low):
                    return True
                for bad in ui_blacklist:
                    if bad.lower() in low:
                        return True
                # phrases like 'Read 152 replies' should be excluded
                if re.search(r"read\s+\d+\s+replies", low):
                    return True
                # skip copyright/footer lines
                if low.startswith('©') or low.endswith('corp.') or low.endswith('x corp.'):
                    return True
                return False

            # Convert dedup map to list and filter UI texts
            deduped = list(all_comments.values())
            filtered_comments = [c for c in deduped if not is_ui_text(c)]

            print(f"Found {len(filtered_comments)} unique comments total (filtered from {len(deduped)})")
            print(f"Comment scraping summary: collected {len(filtered_comments)} comments from {tweet_url}")
            
        except Exception as e:
            print(f"Error scraping comments: {e}")
        
        return list(filtered_comments)
    
    def scrape_brand_comments(self, brand_name, max_users=3, max_tweets_per_user=5):
        """Complete workflow: search users, get tweets, scrape comments (ALL comments)"""
        print(f"\n🚀 Starting Twitter comment scraping for brand: {brand_name}")
        
        # Setup driver
        if not self.setup_driver():
            return []
        
        try:
            # Step 1: Search for Twitter users
            user_urls = self.search_twitter_users(brand_name, max_results=max_users)
            
            if not user_urls:
                print("No Twitter profiles found")
                return []
            
            all_results = []
            
            # Step 2: For each user, get tweets and comments
            for user_url in user_urls:
                print(f"\n👤 Processing user: {user_url}")
                
                # Get tweets from this user
                tweets = self.get_tweets_from_profile(user_url, max_tweets=max_tweets_per_user)
                
                # Get comments for each tweet
                for tweet in tweets:
                    post_url = tweet.get("post_url")
                    # If the post_url is a canonical tweet URL, try to derive the media-specific URL
                    media_url = post_url
                    if post_url and ('/photo/' not in post_url and '/video/' not in post_url):
                        media_url = post_url.rstrip('/') + '/photo/1'

                    tweet_comments = []
                    try:
                        if not self.ensure_logged_in(media_url):
                            print("Skipping post - not authenticated")
                        else:
                            # Open the tweet/media URL directly in a new tab and scrape comments
                            try:
                                self.driver.execute_script("window.open(arguments[0], '_blank');", media_url)
                            except Exception:
                                # As a fallback, navigate in the same tab
                                try:
                                    self.driver.get(media_url)
                                except Exception:
                                    pass

                            time.sleep(2)
                            tabs = self.driver.window_handles
                            if len(tabs) > 0:
                                # switch to the newest tab
                                self.driver.switch_to.window(tabs[-1])
                                time.sleep(2)
                                tweet_comments = self.scrape_comments(media_url)
                                # close the tab and return to first tab if multiple tabs
                                try:
                                    if len(tabs) > 1:
                                        self.driver.close()
                                        self.driver.switch_to.window(tabs[0])
                                except Exception:
                                    pass
                    except Exception as e:
                        print(f"Error while scraping comments from media tab: {e}")
                    
                    result = {
                        "user_profile": user_url,
                        "post_url": tweet["post_url"],
                        "content": tweet["content"],
                        "timestamp": tweet.get("timestamp"),
                        "comments": tweet_comments,
                        "comment_count": len(tweet_comments)
                    }
                    all_results.append(result)
                    
                    time.sleep(2)  # Rate limiting
            
            return all_results
            
        except Exception as e:
            print(f"Error in scraping workflow: {e}")
            return []
        
        finally:
            if self.driver:
                self.driver.quit()

    def save_to_mongo(self, results, brand_name):
        """Save results to MongoDB Twitter collection"""
        try:
            # Delegate saving/upserting to helper
            res = upsert_twitter_profile(brand_name, results)
            return res.get("inserted_id") or str(res)
            
        except Exception as e:
            print(f"Error saving to MongoDB: {e}")
            raise


# FastAPI Endpoints
@router.post("/scrape", response_model=TwitterScrapeResponse)
async def scrape_twitter_comments(request: TwitterScrapeRequest):
    """Scrape Twitter comments for a brand and save to MongoDB"""
    try:
        # Initialize scraper
        scraper = TwitterCommentScraper()
        
        # Run scraping
        results = scraper.scrape_brand_comments(
            brand_name=request.brand_name,
            max_users=request.max_users,
            max_tweets_per_user=request.max_tweets_per_user
        )
        
        if not results:
            raise HTTPException(status_code=404, detail="No Twitter data found for the specified brand")
        
        # Save to MongoDB (fall back to local backup if DB write fails)
        mongo_id = None
        backup_path = None
        try:
            mongo_id = scraper.save_to_mongo(results, request.brand_name)
            save_message = None
        except Exception as e:
            # Write local backup and continue
            try:
                ts = int(time.time())
                backup_dir = os.path.join(os.getcwd(), 'backups')
                os.makedirs(backup_dir, exist_ok=True)
                backup_path = os.path.join(backup_dir, f'twitter_backup_{request.brand_name}_{ts}.json')
                with open(backup_path, 'w', encoding='utf-8') as bf:
                    json.dump({'brand_name': request.brand_name, 'scraped_at': ts, 'results': results}, bf, ensure_ascii=False, indent=2)
                print(f"Error saving to MongoDB: {e}")
                print(f"Saved local backup to {backup_path}")
                save_message = f"Saved results to local backup: {backup_path} (MongoDB write failed: {e})"
            except Exception as bf_err:
                # If backup also fails, raise
                raise HTTPException(status_code=500, detail=f"Failed to save to MongoDB and failed to write local backup: {bf_err}")

        # Calculate totals
        total_tweets = len(results)
        total_comments = sum(r["comment_count"] for r in results)

        message = f"Successfully scraped {total_tweets} tweets with {total_comments} comments for '{request.brand_name}'"
        if backup_path:
            message = message + ". " + save_message

        return TwitterScrapeResponse(
            success=True,
            message=message,
            brand_name=request.brand_name,
            total_tweets=total_tweets,
            total_comments=total_comments,
            results=results
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")


class MediaScrapeRequest(BaseModel):
    brand_name: str
    max_media_posts: int = 10


class MediaScrapeResponse(BaseModel):
    success: bool
    message: str
    brand_name: str
    total_media: int
    total_comments: int
    results: List[Dict[str, Any]]


@router.post('/scrape_media', response_model=MediaScrapeResponse)
async def scrape_twitter_media(request: MediaScrapeRequest):
    try:
        scraper = TwitterCommentScraper()
        if not scraper.setup_driver():
            raise HTTPException(status_code=500, detail='Failed to initialize browser or load cookies')

        user_urls = scraper.search_twitter_users(request.brand_name, max_results=1)
        if not user_urls:
            raise HTTPException(status_code=404, detail='No Twitter profiles found for brand')

        profile = user_urls[0]
        media_posts = scraper.get_media_from_profile(profile, max_posts=request.max_media_posts)

        results = []
        total_comments = 0
        for mp in media_posts:
            post_url = mp.get('post_url') if isinstance(mp, dict) else mp
            if not post_url:
                continue
            comments = scraper.scrape_comments(post_url)
            results.append({
                'post_url': post_url,
                'media_urls': mp.get('media_urls', []) if isinstance(mp, dict) else [],
                'comments': comments,
                'comment_count': len(comments)
            })
            total_comments += len(comments)

        # Save to Mongo (reusing existing save logic) — create a minimal results wrapper
        save_results = [{
            'user_profile': profile,
            'post_url': r['post_url'],
            'content': '',
            'timestamp': None,
            'comments': r['comments'],
            'comment_count': r['comment_count']
        } for r in results]

        # Try to save; fallback to local backup on failure
        try:
            # Delegate to helper upsert for twitter profile
            upsert_twitter_profile(request.brand_name, save_results)
            save_message = None
        except Exception as e:
            ts = int(time.time())
            backup_dir = os.path.join(os.getcwd(), 'backups')
            os.makedirs(backup_dir, exist_ok=True)
            backup_path = os.path.join(backup_dir, f'twitter_media_backup_{request.brand_name}_{ts}.json')
            with open(backup_path, 'w', encoding='utf-8') as bf:
                json.dump({'profile': request.brand_name, 'scraped_at': ts, 'results': save_results}, bf, ensure_ascii=False, indent=2)
            save_message = f'Saved local backup to {backup_path} due to DB error: {e}'

        message = f"Scraped {len(results)} media posts with {total_comments} comments for {request.brand_name}"
        if save_message:
            message = message + '. ' + save_message

        return MediaScrapeResponse(success=True, message=message, brand_name=request.brand_name, total_media=len(results), total_comments=total_comments, results=results)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Media scraping failed: {e}")


