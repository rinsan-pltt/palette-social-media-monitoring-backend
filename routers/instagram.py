import os, json, time, re
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from helpers.mongo_helper import get_sessions_collection, get_instagram_collection, insert_instagram_doc, get_session, upsert_session, upsert_instagram_profile
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/instagram", tags=["instagram"])

class ScrapeRequest(BaseModel):
    target_username: str
    max_posts: int = 5

class ScrapeResponse(BaseModel):
    success: bool
    message: str
    posts_scraped: int

# ---------------- SESSION MANAGEMENT ----------------
async def load_session(context, page):
    """Load session cookies from MongoDB if available"""
    sessions_collection = get_sessions_collection()
    
    # Find Instagram session in database
    session_doc = sessions_collection.find_one({"platform": "instagram"})
    
    if not session_doc:
        raise Exception("No Instagram session found in database. Please save session cookies first.")
    
    cookies = session_doc.get("cookies", [])
    if not cookies:
        raise Exception("No cookies found in Instagram session.")
    
    # Convert MongoDB cookie format to Playwright format
    playwright_cookies = []
    for cookie in cookies:
        playwright_cookie = {
            "name": cookie["name"],
            "value": cookie["value"],
            "domain": cookie["domain"],
            "path": cookie["path"],
            "secure": cookie.get("secure", False),
            "httpOnly": cookie.get("httpOnly", False),
            "sameSite": cookie.get("sameSite", "Lax")
        }
        
        # Add expires if present and valid
        if "expires" in cookie and cookie["expires"] > 0:
            playwright_cookie["expires"] = cookie["expires"]
            
        playwright_cookies.append(playwright_cookie)
    
    await context.add_cookies(playwright_cookies)
    await page.goto("https://www.instagram.com/", timeout=60000)
    await page.wait_for_timeout(5000)
    
    # Check if session is still valid
    if "login" in page.url.lower():
        # Session expired - need to update cookies
        print("Session expired. Please update cookies in database.")
        raise Exception("Session expired or invalid — please update cookies in database.")
    else:
        print("Logged in using MongoDB session cookies.")
        return session_doc.get("ig_user", "unknown")

async def update_session_cookies(context):
    """Update session cookies in MongoDB after successful login"""
    try:
        # Get current cookies from browser context
        current_cookies = await context.cookies()
        
        # Filter Instagram-related cookies
        ig_cookies = [cookie for cookie in current_cookies if 
                     ".instagram.com" in cookie.get("domain", "") or 
                     ".facebook.com" in cookie.get("domain", "") or
                     ".doubleclick.net" in cookie.get("domain", "")]
        
        if not ig_cookies:
            return False
        
        sessions_collection = get_sessions_collection()
        
        # Update or create session document
        session_doc = {
            "platform": "instagram",
            "cookies": ig_cookies,
            "created_at": datetime.now().isoformat()
        }
        
        # Try to extract user ID from cookies
        ds_user_cookie = next((cookie for cookie in ig_cookies if cookie["name"] == "ds_user_id"), None)
        if ds_user_cookie:
            session_doc["ig_user"] = ds_user_cookie["value"]
        
        sessions_collection.update_one(
            {"platform": "instagram"},
            {"$set": session_doc},
            upsert=True
        )
        
        print("Updated Instagram session cookies in database")
        return True
        
    except Exception as e:
        print(f"Failed to update session cookies: {e}")
        return False

# ---------------- COMMENT PARSING HELPERS ----------------
import re

USERNAME_RE = re.compile(r'^[A-Za-z0-9_.@-]{2,50}$')
TIME_RE = re.compile(r'^(\d+\s*[wdhm]|\d+\s*w\d+|\d+\s*w|\d+\s*d|\d+\s*h)', re.I)
LIKES_RE = re.compile(r'^(\d+\s*likes?|\d+\s*like)$', re.I)
UI_TOKENS = set(['reply', 'see translation', 'view replies', 'view all', 'load more', 'reply\nsee translation'])

def is_username_token(s: str) -> bool:
    s = (s or '').strip()
    if not s:
        return False
    if '\n' in s:
        return False
    if ' ' in s:
        return False
    return USERNAME_RE.match(s) is not None

def is_time_token(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    return bool(TIME_RE.search(s))

def is_likes_token(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    return bool(LIKES_RE.match(s)) or ('like' in s.lower() and any(ch.isdigit() for ch in s))

def is_ui_token(s: str) -> bool:
    if not s:
        return False
    t = s.strip().lower()
    if t in UI_TOKENS:
        return True
    if t.startswith('view replies') or t.startswith('see translation'):
        return True
    return False

def extract_time_and_likes(s: str):
    if not s:
        return '', ''
    s = s.strip()
    likes = ''
    m_like = re.search(r'(\d+\s*likes?)', s, re.I)
    if m_like:
        likes = m_like.group(1)
    # Capture common time tokens like '4w', '4w14', '3d', '2h', '10m'
    m_time = re.search(r'(\d+\s*w\d*|\d+\s*w|\d+\s*d|\d+\s*h|\d+\s*m)', s, re.I)
    posted = m_time.group(1) if m_time else ''
    # Normalize week tokens: convert '4w14' -> '4w' (strip numbers after the unit)
    if posted:
        try:
            # If pattern like '4w14' or '4w14something', keep only '4w'
            mw = re.match(r'^\s*(\d+)\s*w\d+\s*$', posted, re.I)
            if mw:
                posted = f"{mw.group(1)}w"
            else:
                # normalize spacing and unit to compact form like '4w' or '3d'
                m_simple = re.match(r'^\s*(\d+)\s*([wdhm])', posted, re.I)
                if m_simple:
                    posted = f"{m_simple.group(1)}{m_simple.group(2).lower()}"
        except Exception:
            pass
    if not posted:
        m = re.match(r'^(\d+\s*w)', s, re.I)
        if m:
            posted = m.group(1)
    return posted, likes

def parse_comments_array(arr):
    i = 0
    out = []
    n = len(arr)
    
    # Special handling for Instagram captions: the first few elements usually form the caption
    # Structure: [username, tagged_users, text_content, time] then real comments start
    caption_elements = []
    caption_collected = False
    
    while i < n:
        token = arr[i]
        if not isinstance(token, str) or token.strip() == '' or is_ui_token(token):
            i += 1
            continue

        # If we haven't collected the caption yet and this looks like the start
        if not caption_collected and is_username_token(token):
            # Collect the caption block (username + tagged users + text + time)
            caption_username = token
            caption_parts = []
            i += 1
            
            # Collect everything until we hit a substantial time token, then look for more text
            time_found = False
            posted_before = ''
            
            while i < n:
                current_token = arr[i]
                
                if is_time_token(current_token) and not time_found:
                    posted_before, _ = extract_time_and_likes(current_token)
                    time_found = True
                    i += 1
                    # Continue collecting text after time token
                    continue
                elif is_username_token(current_token) and time_found:
                    # If we hit another username after time, caption is complete
                    break
                elif is_ui_token(current_token):
                    i += 1
                    continue
                else:
                    # Add to caption content
                    if not is_likes_token(current_token):
                        caption_parts.append(str(current_token))
                    i += 1
            
            # Create the caption comment
            caption_text = ' '.join(p.strip() for p in caption_parts).strip()
            if caption_text or posted_before:  # Only add if there's content
                out.append({
                    'username': caption_username,
                    'text': caption_text,
                    'posted_before': posted_before,
                    'likes': '',
                    'reply': []
                })
            
            caption_collected = True
            continue

        # Regular comment parsing for actual user comments
        if is_username_token(token) and i+1 < n and arr[i+1] == token:
            username = token
            i += 2
            text_parts = []
            while i < n and not is_time_token(arr[i]) and not is_username_token(arr[i]) and not is_ui_token(arr[i]):
                if is_likes_token(arr[i]):
                    break
                text_parts.append(str(arr[i]))
                i += 1
            text = ' '.join(p.strip() for p in text_parts).strip()
            posted_before = ''
            likes = ''
            if i < n and is_time_token(arr[i]):
                posted_before, likes_from_token = extract_time_and_likes(arr[i])
                if likes_from_token:
                    likes = likes_from_token
                i += 1
            if i < n and is_likes_token(arr[i]):
                likes = arr[i].strip()
                i += 1
            text = text or ''
            out.append({
                'username': username,
                'text': text,
                'posted_before': posted_before,
                'likes': likes,
                'reply': []
            })
            continue

        if is_username_token(token):
            username = token
            i += 1
            text_parts = []
            posted_before = ''
            likes = ''
            
            # First collect any immediate text after username
            while i < n and not is_username_token(arr[i]) and not is_time_token(arr[i]) and not is_ui_token(arr[i]):
                if is_likes_token(arr[i]):
                    break
                text_parts.append(str(arr[i]))
                i += 1
            
            # Check for time token
            if i < n and is_time_token(arr[i]):
                posted_before, likes_from_token = extract_time_and_likes(arr[i])
                if likes_from_token:
                    likes = likes_from_token
                i += 1
                
                # Continue collecting text AFTER the time token
                while i < n and not is_username_token(arr[i]) and not is_time_token(arr[i]) and not is_ui_token(arr[i]):
                    if is_likes_token(arr[i]):
                        if not likes:
                            likes = arr[i].strip()
                        i += 1
                        break
                    text_parts.append(str(arr[i]))
                    i += 1
            
            # Check for likes token if we haven't found one yet
            if i < n and is_likes_token(arr[i]) and not likes:
                likes = arr[i].strip()
                i += 1
                
            text = ' '.join(p.strip() for p in text_parts).strip()
            out.append({
                'username': username,
                'text': text,
                'posted_before': posted_before,
                'likes': likes,
                'reply': []
            })
            continue

        if is_time_token(token):
            posted_before, likes_from_token = extract_time_and_likes(token)
            if out:
                if posted_before:
                    out[-1]['posted_before'] = out[-1].get('posted_before') or posted_before
                if likes_from_token:
                    out[-1]['likes'] = out[-1].get('likes') or likes_from_token
            i += 1
            continue

        if is_likes_token(token):
            if out:
                out[-1]['likes'] = out[-1].get('likes') or token.strip()
            i += 1
            continue

        if out:
            if token and isinstance(token, str) and token.strip():
                prev = out[-1]
                if prev['text']:
                    prev['text'] = prev['text'] + ' ' + token.strip()
                else:
                    prev['text'] = token.strip()
        i += 1

    return out


async def safe_click(page, text):
    try:
        await page.get_by_text(text, exact=False).click(timeout=2000)
        await page.wait_for_timeout(500)
    except:
        pass

# ---------------- LOGIN ----------------
async def login(page, ig_user, ig_password):
    print("Logging in to Instagram...")
    await page.goto("https://www.instagram.com/accounts/login/", timeout=60000)
    await page.wait_for_selector("input[name='username']", timeout=20000)
    # Ensure values passed to fill are strings
    await page.fill("input[name='username']", str(ig_user))
    await page.fill("input[name='password']", str(ig_password))
    await page.click("button[type='submit']")
    await page.wait_for_timeout(8000)
    for txt in ["Not Now", "Save Info", "Allow all cookies", "Cancel"]:
        await safe_click(page, txt)
    if "login" in page.url:
        raise Exception("Login failed — check credentials.")
    print("Logged in successfully.")


# ---------------- SESSION MGMT ----------------
async def ensure_logged_in(context, page, ig_user, ig_password):
    """Ensure user is logged in using MongoDB stored session cookies"""
    try:
        # Try to load session from MongoDB first
        logged_in_user = await load_session(context, page)
        print(f"Logged in via MongoDB session for user: {logged_in_user}")
        return logged_in_user
    except Exception as e:
        print(f"MongoDB session failed: {e}")
        print("Attempting fresh login...")
        
        # If MongoDB session fails, do fresh login
        await login(page, ig_user, ig_password)
        
        # Save new session to MongoDB
        await update_session_cookies(context)
        return ig_user

# ---------------- LOAD ALL COMMENTS ----------------
async def load_all_comments(page):
    """Repeatedly click 'Load more comments' or similar until all loaded"""
    print("Loading all comments (expanding replies and scrolling)...")
    prev_count = -1
    stable_rounds = 0
    attempts = 0
    # try until counts stabilize or max attempts reached
    while attempts < 20 and stable_rounds < 4:
        clicked_any = False

        # 1) First try the SVG 'Load more comments' inside the dialog (most reliable)
        try:
            svg_btn = page.locator("div[role='dialog'] svg[aria-label='Load more comments']").first
            if await svg_btn.is_visible():
                try:
                    await svg_btn.click()
                    clicked_any = True
                    await page.wait_for_timeout(1200)
                except:
                    pass
        except:
            pass

        # 2) Try generic svg selector (outside dialog) as a fallback
        if not clicked_any:
            try:
                svg_btn2 = page.locator("svg[aria-label='Load more comments']").first
                if await svg_btn2.is_visible():
                    try:
                        await svg_btn2.click()
                        clicked_any = True
                        await page.wait_for_timeout(1200)
                    except:
                        pass
            except:
                pass

        # 3) Try clicking textual expansion buttons
        for txt in ["View replies", "View more comments", "Show replies", "More replies", "Show more replies", "Load more", "View all comments", "View all"]:
            try:
                btns = page.get_by_text(txt, exact=False)
                btn_count = await btns.count()
                if btn_count > 0:
                    try:
                        # click up to first three occurrences
                        for j in range(min(3, btn_count)):
                            try:
                                await btns.nth(j).click(timeout=1500)
                                clicked_any = True
                                await page.wait_for_timeout(800)
                            except:
                                pass
                    except:
                        pass
            except:
                pass

        # 4) Scroll to trigger lazy loading
        try:
            await page.evaluate("window.scrollBy(0, 1000)")
        except:
            pass
        await page.wait_for_timeout(1200)

        # Extra pass: click any buttons inside the dialog whose text mentions 'comment'
        try:
            dlg_buttons = await page.locator("div[role='dialog'] button").all()
            for b in dlg_buttons[:6]:
                try:
                    bt = (await b.inner_text()).strip().lower()
                    if 'comment' in bt or 'comments' in bt or 'view all' in bt:
                        try:
                            await b.click()
                            clicked_any = True
                            await page.wait_for_timeout(1000)
                        except:
                            pass
                except:
                    continue
        except:
            pass

        # 5) Count comment elements inside dialog if present
        try:
            elems = await page.locator("div[role='dialog'] ul li div div span").all()
        except:
            try:
                elems = await page.locator("ul ul div span").all()
            except:
                elems = []

        curr_count = len(elems)

        # If nothing changed and we didn't click anything, increment stable counter faster
        if curr_count == prev_count:
            if not clicked_any:
                stable_rounds += 1
            else:
                stable_rounds += 0
        else:
            stable_rounds = 0
            prev_count = curr_count

        attempts += 1

    print(f"Finished expanding/loading comments (found approx {prev_count if prev_count>=0 else 0} elements)")

# ---------------- SCRAPE POST ----------------
async def scrape_post_data(page, target_username=None):
    """Extract caption, hashtags, post_url, and all comments."""
    await page.wait_for_selector("div[role='dialog']", timeout=10000)
    await page.wait_for_timeout(3000)
    await load_all_comments(page)

    # Extract post URL to get username
    current_url = page.url
    post_username = ""
    try:
        # For URLs like https://www.instagram.com/p/DQksrWIEnWm/, use target_username
        # For URLs like https://www.instagram.com/nike/p/..., extract from URL
        import re
        url_match = re.search(r'instagram\.com/([^/]+)/p/', current_url)
        if url_match and url_match.group(1) != 'p':
            post_username = url_match.group(1)
        else:
            # Use the target username passed to the function
            post_username = target_username or ""
        print(f"Using post username: '{post_username}' (target: '{target_username}', URL: {current_url})")
    except:
        post_username = target_username or ""
        print(f"Using fallback username: '{post_username}'")

    # Extract all comment text elements first
    comments_arr = []
    try:
        # Get all comment elements first
        comment_elements = await page.locator("div[role='dialog'] ul li div div span").all()
        if not comment_elements:
            comment_elements = await page.locator("ul ul div span").all()

        for c in comment_elements:
            try:
                text = (await c.inner_text()).strip()
            except:
                text = ""
            if text:
                # filter out UI tokens like 'Reply' / 'See Translation'
                if text.lower() in ("reply", "see translation", "view replies", "load more"):
                    continue
                comments_arr.append(text)
    except:
        pass

    # Caption: Look for caption in raw comments before parsing
    caption = ""
    try:
        if comments_arr:
            # Debug: show raw comment array first
            print(f"Raw comments_arr (first 15 items): {comments_arr[:15]}")
            
            # Try to find the caption text directly in the raw array
            # Look for substantial text that contains the actual caption content
            for i, text in enumerate(comments_arr):
                if isinstance(text, str) and len(text.strip()) > 30:
                    # Check if this looks like caption text (not just usernames or UI elements)
                    text_clean = text.strip()
                    if (not is_username_token(text_clean) and 
                        not is_time_token(text_clean) and 
                        not is_likes_token(text_clean) and
                        not is_ui_token(text_clean) and
                        not text_clean.lower() in ['reply', 'see translation', 'view replies']):
                        
                        # This looks like actual caption content
                        caption = text_clean
                        print(f"✓ Found caption text directly in raw array at index {i}: {caption[:100]}...")
                        # Remove the caption token and any immediate preceding mention tokens
                        # Find a safe start index to splice out (include usernames/mentions directly before)
                        start = i
                        # Walk backwards to include preceding username/mention tokens
                        while start > 0 and (is_username_token(comments_arr[start-1]) or comments_arr[start-1].strip().startswith('@')):
                            start -= 1
                        # Remove slice start..i (inclusive caption token)
                        del comments_arr[start:i+1]
                        break
            
            # If no direct caption found, try parsing approach
            if not caption:
                parsed_comments = parse_comments_array(comments_arr)
                print(f"Parsed {len(parsed_comments)} comments, looking for username: '{post_username}'")
                
                # Debug: show first few comments with more details
                for i, comment in enumerate(parsed_comments[:5]):
                    username = comment.get('username', '')
                    text = comment.get('text', '')[:80]
                    likes = comment.get('likes', '')
                    posted_before = comment.get('posted_before', '')
                    print(f"  Comment {i+1}: username='{username}', text='{text}...', likes='{likes}', time='{posted_before}'")
                
                # Look for the first comment with substantial text from brand account
                if parsed_comments:
                    for i, comment in enumerate(parsed_comments[:3]):
                        username = comment.get('username', '').strip()
                        text = comment.get('text', '').strip()
                        likes = comment.get('likes', '').strip()
                        posted_before = comment.get('posted_before', '').strip()
                        
                        if text and len(text) > 20:
                            if (username.lower().startswith(post_username.lower()) or 
                                post_username.lower() in username.lower() or
                                username.lower() == post_username.lower()):
                                caption = text
                                print(f"✓ Found caption from related account '{username}': {caption[:100]}...")
                                # Remove tokens up to the start of the next comment in the raw array to avoid duplication
                                # Conservative approach: remove the first N raw tokens until next username token
                                # Find index of next username token in comments_arr
                                next_idx = 0
                                for idx, tok in enumerate(comments_arr):
                                    if is_username_token(tok):
                                        # if this username matches the parsed first comment username, skip it
                                        if tok.strip().lower() == username.lower():
                                            # advance past this block later
                                            next_idx = idx
                                            break
                                # Now remove tokens from 0..next_idx-1 (caption block)
                                if next_idx > 0:
                                    del comments_arr[0:next_idx]
                                break
                            elif i == 0:
                                caption = text
                                print(f"✓ Using first substantial comment as caption from '{username}': {caption[:100]}...")
                                break
        
        # Last fallback to meta description if no caption found
        if not caption:
            meta_caption = await page.get_attribute("meta[property='og:description']", "content") or ""
            if meta_caption and "Followers" not in meta_caption and "Following" not in meta_caption:
                caption = meta_caption
                print(f"Using meta description as caption: {caption[:100]}...")
            
    except Exception as e:
        print(f"Error extracting caption: {e}")
        caption = ""

    # Extract hashtags
    hashtags = re.findall(r"#\w+", caption)

    # Convert flattened comments into structured objects using parser
    cleaned_comments = parse_comments_array(comments_arr)
    
    print(f"   Raw comment elements: {len(comments_arr)}, Parsed comments: {len(cleaned_comments)}")

    return {
        "post_url": page.url,
        "caption": caption,
        "hashtags": hashtags,
        "comments": cleaned_comments
    }

@router.post("/scrape", response_model=ScrapeResponse)
async def scrape_instagram(request: ScrapeRequest):
    """
    Scrape Instagram posts for a given username.
    """
    try:
        # Read credentials from environment (still needed as fallback)
        ig_user = os.getenv("IG_USER")
        ig_password = os.getenv("IG_PASSWORD")
        
        if not ig_user or not ig_password:
            raise HTTPException(status_code=400, detail="Instagram credentials not found in environment variables")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Always headless
            context = await browser.new_context(viewport={"width": 1280, "height": 900})
            page = await context.new_page()

            logged_in_user = await ensure_logged_in(context, page, ig_user, ig_password)

            profile_url = f"https://www.instagram.com/{request.target_username}/"
            print(f"Opening profile: {profile_url}")
            await page.goto(profile_url, timeout=60000)
            await page.wait_for_timeout(5000)
            await safe_click(page, "Allow all cookies")

            print("Scrolling to load posts...")
            for _ in range(5):
                await page.mouse.wheel(0, 2000)
                await page.wait_for_timeout(1000)

            posts = await page.locator("a[href*='/p/']").all()
            # Build a list of unique hrefs (preserve order) to avoid clicking the same post twice
            hrefs = []
            seen = set()
            for el in posts:
                try:
                    href = await el.get_attribute('href')
                except:
                    href = None
                if not href:
                    continue
                # Normalize href to absolute-ish form if needed (Instagram uses path like /username/p/..)
                if href.startswith('http'):
                    normalized = href
                else:
                    normalized = href
                if normalized in seen:
                    continue
                seen.add(normalized)
                hrefs.append(normalized)

            print(f"🔎 Found {len(hrefs)} unique post URLs (raw anchors: {len(posts)}).")
            if not hrefs:
                await browser.close()
                return ScrapeResponse(
                    success=False,
                    message="No posts found",
                    posts_scraped=0,
                    data=[]
                )
            num_to_scrape = min(len(hrefs), request.max_posts)
            results = []

            for i in range(num_to_scrape):
                href = hrefs[i]
                print(f"\nScraping post {i+1}/{num_to_scrape}...")
                try:
                    # Use a fresh locator by href to avoid stale element handles
                    locator = page.locator(f"a[href='{href}']").first
                    try:
                        await locator.scroll_into_view_if_needed(timeout=5000)
                    except:
                        pass

                    print(f"About to click post with URL: {href}")
                    try:
                        await locator.click(timeout=5000)
                    except:
                        # Fallback to navigating directly if click fails
                        target = href if href.startswith('http') else f"https://www.instagram.com{href}"
                        await page.goto(target, timeout=60000)

                    await page.wait_for_timeout(3000)

                    current_url = page.url
                    print(f"Post dialog opened with URL: {current_url}")

                    post_data = await scrape_post_data(page, request.target_username)
                    post_data["username"] = request.target_username
                    post_data["post_index"] = i + 1

                    # Verify this isn't a duplicate URL in this scraping run
                    is_duplicate = any(existing_post.get("post_url") == post_data.get("post_url") for existing_post in results)
                    if is_duplicate:
                        print(f"Duplicate URL detected in-run: {post_data.get('post_url')}, skipping...")
                    else:
                        results.append(post_data)
                        print(f"Successfully scraped post {i+1}: {post_data.get('post_url')}")

                    # Close dialog and return to feed
                    try:
                        await page.keyboard.press("Escape")
                        await page.wait_for_timeout(1000)
                    except:
                        pass

                    await page.wait_for_timeout(500)

                except Exception as e:
                    print(f"Failed to scrape post {i+1}: {e}")
                    try:
                        await page.keyboard.press("Escape")
                        await page.wait_for_timeout(1000)
                    except:
                        pass

            await browser.close()

            # Save to MongoDB via helper
            try:
                if results:
                    # Add scraped_at timestamp to each post
                    for post in results:
                        post["scraped_at"] = time.time()
                        post.pop("username", None)
                    res = upsert_instagram_profile(request.target_username, results)
                    print(f"Saved Instagram profile '{request.target_username}': {res}")
            except Exception as e:
                print(f"Failed to save to MongoDB: {e}")

            return ScrapeResponse(
                success=True,
                message=f"Successfully scraped and saved {len(results)} posts from @{request.target_username} to MongoDB",
                posts_scraped=len(results)
            )

    except Exception as e:
        error_msg = str(e)
        if "Session expired" in error_msg or "please update cookies" in error_msg.lower():
            return ScrapeResponse(
                success=False,
                message="Instagram session expired. Please update cookies in database and try again.",
                posts_scraped=0
            )
        return ScrapeResponse(
            success=False,
            message=f"Scraping failed: {error_msg}",
            posts_scraped=0
        )