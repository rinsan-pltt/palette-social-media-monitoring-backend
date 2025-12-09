from playwright.sync_api import sync_playwright
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time as _time
from bs4 import BeautifulSoup
import logging
import json
import time
import re
from typing import List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from helpers.mongo_helper import insert_youtube_doc, upsert_youtube_doc, upsert_youtube_profile

router = APIRouter(prefix="/youtube", tags=["youtube"])


class YouTubeScrapeRequest(BaseModel):
    brand: str
    max_videos: int = 5


class YouTubeScrapeResponse(BaseModel):
    success: bool
    message: str
    videos_scraped: int


def clean_description(raw_text, max_chars=1000):
    if not raw_text:
        return ""

    text = re.sub(r"Show more|Show less", "", raw_text, flags=re.IGNORECASE)
    text = re.sub(r"\d{1,3}(,\d{3})* views[^\n]*", "", text)
    text = re.sub(r"\n{2,}", "\n", text)
    text = re.sub(r"\s{2,}", " ", text)
    text = text.strip()

    if len(text) > max_chars:
        trimmed = text[:max_chars]
        last_space = trimmed.rfind(" ")
        if last_space != -1:
            trimmed = trimmed[:last_space]
        text = trimmed + "..."
    return text


class YouTubeCommentScraper:
    def __init__(self, headless=True, timeout=10, scroll_pause_time=1.5, 
                 enable_logging=False, return_page_source=False):
        self.timeout = timeout
        self.scroll_pause_time = scroll_pause_time
        self.return_page_source = return_page_source
        self.driver = self._init_driver(headless)
        self.enable_logging = enable_logging
        if self.enable_logging:
            logging.basicConfig(
                filename='youtube_scraper.log', 
                level=logging.INFO, 
                format='%(asctime)s - %(levelname)s - %(message)s'
            )
            self.log_info("Logging is enabled.")

    def log_info(self, message):
        if self.enable_logging:
            logging.info(message)
    def log_warning(self, message):
        if self.enable_logging:
            logging.warning(message)
    def log_error(self, message):
        if self.enable_logging:
            logging.error(message)

    def _init_driver(self, headless):
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)

    def wait_for_element(self, by, value):
        try:
            element = WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            self.log_warning(f"Element not found: {value}")
            return None

    def scroll_until_all_comments_loaded(self):
        last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
        self.log_info("Scrolling to load comments...")
        while True:
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
            _time.sleep(self.scroll_pause_time)
            new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                self.log_info("All comments loaded.")
                break
            last_height = new_height

    def extract_comments(self):
        page_source = self.get_page_source()
        soup = BeautifulSoup(page_source, 'html.parser')
        comment_elements = soup.select('#content-text')
        comments = [element.get_text(strip=True) for element in comment_elements]
        self.log_info(f"Extracted {len(comments)} comments.")
        return comments

    def get_page_source(self):
        self.log_info("Fetching page source.")
        return self.driver.page_source

    def scrape_comments(self, video_url, scroll=True):
        try:
            self.log_info(f"Opening URL: {video_url}")
            self.driver.get(video_url)
            self.wait_for_element(By.TAG_NAME, 'ytd-comments')
            if scroll:
                self.scroll_until_all_comments_loaded()
            comments = self.extract_comments()
            if self.return_page_source:
                page_source = self.get_page_source()
                return comments, page_source
            return comments
        except Exception as e:
            self.log_error(f"An error occurred: {e}")
            return ([], "") if self.return_page_source else []
        finally:
            try:
                self.driver.quit()
            except:
                pass
            self.log_info("Driver closed.")


def search_videos(brand, scroll_count=5):
    video_urls = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.youtube.com/", timeout=60000)
        page.wait_for_load_state("domcontentloaded")

        try:
            page.click("button:has-text('Accept all')", timeout=5000)
        except:
            pass

        try:
            input_box = (
                page.query_selector("yt-searchbox input#search")
                or page.query_selector("yt-searchbox input[type='text']")
                or page.locator("yt-searchbox input").first
            )
            input_box.click()
            page.wait_for_timeout(500)
            input_box.fill(brand)
            page.wait_for_timeout(300)
        except Exception as e:
            raise Exception(f"Could not type into search box: {e}")

        try:
            page.click("button.ytSearchboxComponentSearchButton", timeout=10000)
        except Exception as e:
            raise Exception(f"Could not click search button: {e}")

        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(3000)

        try:
            page.wait_for_selector("ytd-video-renderer", timeout=15000)
        except:
            print("No video results found on page.")
            browser.close()
            return []

        seen = set()
        for i in range(scroll_count):
            video_elements = page.query_selector_all("ytd-video-renderer a#video-title")
            for elem in video_elements:
                href = elem.get_attribute("href")
                if href and "/watch" in href:
                    full_url = f"https://www.youtube.com{href.split('&')[0]}"
                    if full_url not in seen:
                        seen.add(full_url)
                        video_urls.append(full_url)
            page.mouse.wheel(0, 4000)
            time.sleep(2)

        browser.close()
    return video_urls


def scrape_comments(url):
    scraper = YouTubeCommentScraper(
        headless=True,
        timeout=20,
        scroll_pause_time=2,
        enable_logging=False,
        return_page_source=False
    )
    try:
        comments = scraper.scrape_comments(url)
    except Exception as e:
        print(f"Error scraping comments for {url}: {e}")
        comments = []
    return comments


@router.post("/scrape", response_model=YouTubeScrapeResponse)
def scrape_youtube(req: YouTubeScrapeRequest):
    if not req.brand:
        raise HTTPException(status_code=400, detail="Brand is required")

    videos = search_videos(req.brand, scroll_count=6)
    print(f"Found {len(videos)} videos. Will scrape comments for each.")
    
    if not videos:
        return YouTubeScrapeResponse(success=False, message="No videos found", videos_scraped=0)

    scraped_posts = []
    for url in videos[:req.max_videos]:
        print("\n🎬 Processing:", url)
        # quick title/desc fetch
        title = ""
        desc = ""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, timeout=60000)
                page.wait_for_timeout(2500)
                try:
                    title = page.locator("h1.title yt-formatted-string").text_content() or ""
                except:
                    title = ""
                try:
                    desc_loc = page.locator("div#bottom-row > div#description")
                    desc = desc_loc.text_content() or ""
                except:
                    desc = ""
                browser.close()
        except Exception:
            pass

        desc = clean_description(desc, max_chars=1000)
        comments = scrape_comments(url)
        if not comments:
            comments = []

        # Structure post data (remove profile field, will be added at profile level)
        post_data = {
            "post_url": url,
            "content": f"{title} - {desc}",
            "comments": comments,
            "scraped_at": time.time()
        }

        scraped_posts.append(post_data)
        time.sleep(1)

    # Save to MongoDB using profile-based structure
    try:
        result = upsert_youtube_profile(req.brand, scraped_posts)
        operation = result.get("operation", "unknown")
        
        if operation == "inserted":
            message = f"Created new profile '{req.brand}' with {len(scraped_posts)} videos"
        else:
            new_posts = result.get("new_posts", 0)
            updated_posts = result.get("updated_posts", 0)
            message = f"Updated profile '{req.brand}': {new_posts} new videos, {updated_posts} updated videos"
            
    except Exception as e:
        print(f"Failed to save to MongoDB: {e}")
        return YouTubeScrapeResponse(
            success=False, 
            message=f"Scraping completed but failed to save to database: {str(e)}", 
            videos_scraped=len(scraped_posts)
        )

    return YouTubeScrapeResponse(
        success=True, 
        message=f"Successfully scraped and saved {len(scraped_posts)} videos to MongoDB. {message}", 
        videos_scraped=len(scraped_posts)
    )
