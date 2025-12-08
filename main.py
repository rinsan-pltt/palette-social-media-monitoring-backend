"""
FastAPI entrypoint for the social media monitoring backend.
"""

import logging
import os
import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger("social_media_monitoring")
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

app = FastAPI(title="Social Media Monitoring Backend", version="1.0")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from routers.instagram import router as instagram_scrape_router
from routers.analysis import router as analytics_router
from routers.twitter import router as twitter_scrape_router
from routers.youtube import router as youtube_scrape_router
from routers.facebook import router as facebook_scrape_router

app.include_router(facebook_scrape_router)
app.include_router(instagram_scrape_router)
app.include_router(twitter_scrape_router)
app.include_router(youtube_scrape_router)
app.include_router(analytics_router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 6688)))
