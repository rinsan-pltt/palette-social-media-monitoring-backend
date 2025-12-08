
import logging
import os
import time
from typing import Any, Dict, Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


class MongoResources:
    """Lazy initialization for MongoDB clients and collections."""

    def __init__(self) -> None:
        self._sync_client: Optional[MongoClient] = None

    def _require_config(self) -> Dict[str, str]:
        uri = os.getenv("MONGO_URI")
        db = os.getenv("MONGO_DB")
        if not uri or not db:
            raise RuntimeError(
                "MongoDB configuration missing: MONGO_URI and/or MONGO_DB"
            )
        return {"uri": uri, "db": db}

    def get_sync_collection(self, name: str):
        cfg = self._require_config()
        if self._sync_client is None:
            self._sync_client = MongoClient(cfg["uri"])
        return self._sync_client[cfg["db"]][name]




mongo = MongoResources()


# --- Instagram helpers (sync) ---

def get_sessions_collection() -> Collection:
    """Return the sync sessions collection for Instagram sessions."""
    sessions_coll_name = os.getenv("MONGO_SESSIONS_COLLECTION", "sessions")
    return mongo.get_sync_collection(sessions_coll_name)


def get_instagram_collection() -> Collection:
    """Return the sync instagram collection for scraped data."""
    instagram_coll_name = os.getenv("MONGO_INSTAGRAM_COLLECTION", "instagram")
    return mongo.get_sync_collection(instagram_coll_name)


def insert_instagram_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Insert a document into the configured instagram collection and return inserted id info."""
    try:
        coll = get_instagram_collection()
        res = coll.insert_one(doc)
        return {"inserted_id": str(res.inserted_id)}
    except PyMongoError:
        logger.exception("Failed to insert instagram document")
        raise


def get_youtube_collection() -> Collection:
    """Return the sync youtube collection for scraped data."""
    youtube_coll_name = os.getenv("MONGO_YOUTUBE_COLLECTION", "youtube")
    return mongo.get_sync_collection(youtube_coll_name)


def insert_youtube_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Insert a document into the configured youtube collection and return inserted id info."""
    try:
        coll = get_youtube_collection()
        res = coll.insert_one(doc)
        return {"inserted_id": str(res.inserted_id)}
    except PyMongoError:
        logger.exception("Failed to insert youtube document")
        raise


def upsert_youtube_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Upsert a document into the youtube collection based on post_url to avoid duplicates."""
    try:
        coll = get_youtube_collection()
        post_url = doc.get("post_url")
        if not post_url:
            # If no post_url, just insert normally
            res = coll.insert_one(doc)
            return {"inserted_id": str(res.inserted_id), "operation": "inserted"}
        
        # Try to update existing document with same post_url
        res = coll.update_one(
            {"post_url": post_url},
            {"$set": doc},
            upsert=True
        )
        
        if res.upserted_id:
            return {"inserted_id": str(res.upserted_id), "operation": "inserted"}
        else:
            return {"modified_count": res.modified_count, "operation": "updated"}
            
    except PyMongoError:
        logger.exception("Failed to upsert youtube document")
        raise


def upsert_youtube_profile(profile: str, posts: list) -> Dict[str, Any]:
    """Upsert YouTube profile with posts array, merging new posts with existing ones."""
    try:
        coll = get_youtube_collection()
        
        # Check if profile already exists
        existing_profile = coll.find_one({"profile": profile})
        
        if existing_profile:
            print(f"🔍 Found existing profile '{profile}' in database")
            existing_posts = existing_profile.get("posts", [])
            existing_urls = {post.get("post_url"): i for i, post in enumerate(existing_posts)}
            
            new_posts_added = 0
            posts_updated = 0
            
            # Process each scraped post
            for scraped_post in posts:
                scraped_url = scraped_post.get("post_url")
                
                if scraped_url in existing_urls:
                    # Update existing post
                    post_index = existing_urls[scraped_url]
                    existing_posts[post_index] = scraped_post
                    posts_updated += 1
                    print(f"📝 Updated existing video: {scraped_url}")
                else:
                    # Add new post
                    existing_posts.append(scraped_post)
                    new_posts_added += 1
                    print(f"➕ Added new video: {scraped_url}")
            
            # Update the existing document
            updated_doc = {
                "posts": existing_posts,
                "total_posts": len(existing_posts),
                "last_scraped_at": posts[0].get("scraped_at") if posts else existing_profile.get("last_scraped_at")
            }
            
            coll.update_one(
                {"profile": profile},
                {"$set": updated_doc}
            )
            
            print(f"💾 Updated profile '{profile}': {new_posts_added} new videos, {posts_updated} videos updated")
            return {"operation": "updated", "new_posts": new_posts_added, "updated_posts": posts_updated}
            
        else:
            # Create new profile document
            structured_doc = {
                "profile": profile,
                "posts": posts,
                "total_posts": len(posts),
                "scraped_at": posts[0].get("scraped_at") if posts else None,
                "last_scraped_at": posts[0].get("scraped_at") if posts else None
            }
            
            res = coll.insert_one(structured_doc)
            print(f"💾 Created new profile '{profile}' with {len(posts)} videos")
            return {"operation": "inserted", "inserted_id": str(res.inserted_id), "posts_added": len(posts)}
            
    except PyMongoError:
        logger.exception("Failed to upsert youtube profile")
        raise


def get_facebook_collection() -> Collection:
    """Return the sync facebook collection for scraped data."""
    facebook_coll_name = os.getenv("MONGO_FACEBOOK_COLLECTION", "facebook")
    return mongo.get_sync_collection(facebook_coll_name)


def upsert_facebook_profile(profile: str, posts: list) -> Dict[str, Any]:
    """Upsert Facebook profile with posts array, merging new posts with existing ones.

    Behavior mirrors upsert_youtube_profile: update existing posts by post_url, append new ones,
    and set total_posts and last_scraped_at.
    """
    try:
        coll = get_facebook_collection()

        existing_profile = coll.find_one({"profile": profile})

        if existing_profile:
            existing_posts = existing_profile.get("posts", [])
            existing_urls = {post.get("post_url"): i for i, post in enumerate(existing_posts)}

            new_posts_added = 0
            posts_updated = 0

            for scraped_post in posts:
                scraped_url = scraped_post.get("post_url")
                if scraped_url in existing_urls:
                    idx = existing_urls[scraped_url]
                    existing_posts[idx] = scraped_post
                    posts_updated += 1
                else:
                    existing_posts.append(scraped_post)
                    new_posts_added += 1

            updated_doc = {
                "posts": existing_posts,
                "total_posts": len(existing_posts),
                "last_scraped_at": posts[0].get("scraped_at") if posts else existing_profile.get("last_scraped_at")
            }

            coll.update_one({"profile": profile}, {"$set": updated_doc})

            return {"operation": "updated", "new_posts": new_posts_added, "updated_posts": posts_updated}
        else:
            structured_doc = {
                "profile": profile,
                "posts": posts,
                "total_posts": len(posts),
                "scraped_at": posts[0].get("scraped_at") if posts else None,
                "last_scraped_at": posts[0].get("scraped_at") if posts else None,
            }
            res = coll.insert_one(structured_doc)
            return {"operation": "inserted", "inserted_id": str(res.inserted_id), "posts_added": len(posts)}

    except PyMongoError:
        logger.exception("Failed to upsert facebook profile")
        raise


# ---------------- Session helpers ----------------
def get_session(filter_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a session document matching the provided filter from the sessions collection."""
    try:
        coll = get_sessions_collection()
        return coll.find_one(filter_doc)
    except PyMongoError:
        logger.exception("Failed to read session from sessions collection")
        return None


def upsert_session(filter_doc: Dict[str, Any], doc: Dict[str, Any]):
    """Upsert a session document into the sessions collection.
    `filter_doc` is the query to match, `doc` is the document to set (full doc recommended).
    """
    try:
        coll = get_sessions_collection()
        coll.update_one(filter_doc, {"$set": doc}, upsert=True)
        return True
    except PyMongoError:
        logger.exception("Failed to upsert session document")
        return False


# ---------------- Twitter helpers (sync) ----------------
def get_twitter_collection() -> Collection:
    twitter_coll_name = os.getenv("MONGO_TWITTER_COLLECTION", "twitter")
    return mongo.get_sync_collection(twitter_coll_name)


def upsert_twitter_profile(profile: str, results: list) -> Dict[str, Any]:
    """Upsert a Twitter profile document merging results by post_url.
    Returns a summary dict similar to other upsert helpers.
    """
    try:
        coll = get_twitter_collection()
        existing = coll.find_one({"profile": profile})

        if not existing:
            doc = {
                "profile": profile,
                "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "total_tweets": len(results),
                "results": results,
            }
            res = coll.insert_one(doc)
            return {"operation": "inserted", "inserted_id": str(res.inserted_id)}

        # Merge into existing
        existing_results = existing.get("results", [])
        idx = {r.get("post_url"): i for i, r in enumerate(existing_results) if r.get("post_url")}
        updated = 0
        added = 0
        for new_r in results:
            purl = new_r.get("post_url")
            if not purl:
                continue
            if purl in idx:
                existing_results[idx[purl]] = new_r
                updated += 1
            else:
                existing_results.append(new_r)
                added += 1

        coll.update_one({"_id": existing["_id"]}, {"$set": {"results": existing_results, "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "total_tweets": len(existing_results)}})
        return {"operation": "updated", "updated": updated, "added": added}
    except PyMongoError:
        logger.exception("Failed to upsert twitter profile")
        raise


# ---------------- Instagram helpers (sync) ----------------
def upsert_instagram_profile(profile: str, posts: list) -> Dict[str, Any]:
    """Upsert Instagram profile document merging posts by post_url.

    For each scraped post in `posts`: if a post with the same `post_url` exists,
    update that entry (merge fields, prefer incoming values). Otherwise append
    the post to the profile's `posts` array. Returns a summary dict with counts.
    """
    try:
        coll = get_instagram_collection()
        existing = coll.find_one({"profile": profile})

        now = int(time.time())

        if existing:
            existing_posts = existing.get("posts", [])
            # map post_url -> index for quick lookup
            url_to_index = {p.get("post_url"): i for i, p in enumerate(existing_posts) if p.get("post_url")}

            added = 0
            updated = 0

            for scraped_post in posts:
                scraped_url = scraped_post.get("post_url")
                if not scraped_url:
                    # no URL -- append
                    scraped_post.setdefault("scraped_at", now)
                    existing_posts.append(scraped_post)
                    added += 1
                    continue

                if scraped_url in url_to_index:
                    idx = url_to_index[scraped_url]
                    merged = existing_posts[idx].copy()
                    merged.update(scraped_post)
                    merged.setdefault("scraped_at", now)
                    existing_posts[idx] = merged
                    updated += 1
                else:
                    scraped_post.setdefault("scraped_at", now)
                    existing_posts.append(scraped_post)
                    url_to_index[scraped_url] = len(existing_posts) - 1
                    added += 1

            updated_doc = {
                "posts": existing_posts,
                "total_posts": len(existing_posts),
                "last_scraped_at": posts[0].get("scraped_at") if posts else existing.get("last_scraped_at"),
            }
            coll.update_one({"profile": profile}, {"$set": updated_doc})
            return {"operation": "updated", "added": added, "updated": updated}

        else:
            structured_doc = {
                "profile": profile,
                "posts": posts,
                "total_posts": len(posts),
                "scraped_at": posts[0].get("scraped_at") if posts else None,
                "last_scraped_at": posts[0].get("scraped_at") if posts else None,
            }
            res = coll.insert_one(structured_doc)
            return {"operation": "inserted", "inserted_id": str(res.inserted_id), "posts_added": len(posts)}
    except PyMongoError:
        logger.exception("Failed to upsert instagram profile")
        raise
