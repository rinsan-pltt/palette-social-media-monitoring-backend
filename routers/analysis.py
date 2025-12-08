from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from pydantic import BaseModel
import openai
import os
from collections import Counter

from helpers.mongo_helper import get_instagram_collection, get_youtube_collection, get_facebook_collection, mongo

router = APIRouter(tags=["analytics"])



class AnalyticsRequest(BaseModel):
    platform: str 
    target_username: str
    post_url: str


class AnalyticsResponse(BaseModel):
    profile: str
    total_posts: int
    total_comments: int
    sentiment_analysis: Dict[str, Any]
    common_factors: List[str]
    top_commenters: List[Dict[str, Any]]
    engagement_insights: Dict[str, Any]


def analyze_comments_with_openai(comments_text: str) -> Dict[str, Any]:
    """Use OpenAI to analyze sentiment and extract insights from comments."""
    openai.api_key = os.getenv("OPENAI_API_KEY")
    
    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a social media analytics expert. Analyze the provided Instagram comments and return ONLY a valid JSON response with sentiment analysis, common themes, and insights. Do not include any other text or explanations outside the JSON."
                },
                {
                    "role": "user",
                    "content": f"""Analyze these Instagram comments and provide a JSON response with:
1. Sentiment percentages (positive, negative, neutral)
2. Top 5 common themes or topics
3. Overall engagement quality assessment
4. Any notable patterns or insights

Comments to analyze:
{comments_text}

Return ONLY valid JSON with these exact keys:
{{
  "sentiment_percentages": {{"positive": number, "negative": number, "neutral": number}},
  "common_themes": ["theme1", "theme2", "theme3", "theme4", "theme5"],
  "engagement_quality": "description of engagement quality",
  "notable_patterns": "description of patterns found"
}}"""
                }
            ],
            max_tokens=1000,
            temperature=0.3
        )
        
        response_text = response.choices[0].message.content.strip()
        
       
        import re
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)
        
        import json
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        print(f"Raw response: {response.choices[0].message.content}")
        
        return {
            "sentiment_percentages": {"positive": 60, "negative": 20, "neutral": 20},
            "common_themes": ["General appreciation", "Product feedback", "Brand loyalty", "Emojis", "Support"],
            "engagement_quality": "Good engagement with mixed sentiment",
            "notable_patterns": "Unable to parse detailed analysis from AI response"
        }
    except Exception as e:
        
        return {
            "sentiment_percentages": {"positive": 60, "negative": 20, "neutral": 20},
            "common_themes": ["General appreciation", "Product feedback", "Brand loyalty", "Emojis", "Support"],
            "engagement_quality": "Good engagement with mixed sentiment",
            "notable_patterns": f"Analysis unavailable due to API error: {str(e)}"
        }


def extract_comment_insights(posts_data: List[Dict]) -> Dict[str, Any]:
    """Extract insights from posts and comments data."""
    all_comments = []
    commenter_counts = Counter()
    total_likes = 0
    
    for post in posts_data:
        for comment in post.get('comments', []):
            if comment.get('text'):
                all_comments.append(comment['text'])
                if comment.get('username'):
                    commenter_counts[comment['username']] += 1
                if comment.get('likes'):
                   
                    likes_str = comment['likes']
                    if isinstance(likes_str, str):
                        
                        import re
                        likes_match = re.search(r'(\d+)', likes_str)
                        if likes_match:
                            total_likes += int(likes_match.group(1))
                    elif isinstance(likes_str, int):
                        total_likes += likes_str
    
    
    comments_text = "\n".join(all_comments[:200])  
    
    
    ai_analysis = analyze_comments_with_openai(comments_text)
    
   
    top_commenters = [
        {"username": username, "comment_count": count}
        for username, count in commenter_counts.most_common(10)
    ]
    
    return {
        "total_comments": len(all_comments),
        "total_comment_likes": total_likes,
        "top_commenters": top_commenters,
        "ai_analysis": ai_analysis
    }


@router.get("/posts", response_model=Dict[str, List[str]], tags=["analytics"])
def get_social_media_posts(platform: str, target_username: str):
    """
    Get all post URLs for a specific profile from MongoDB.
    Supports Instagram, Twitter, and YouTube platforms.
    """
    try:
        
        if platform.lower() == "instagram":
            coll = get_instagram_collection()
        elif platform.lower() == "twitter":
            coll = mongo.get_sync_collection(os.getenv("MONGO_TWITTER_COLLECTION", "twitter"))
        elif platform.lower() == "youtube":
            coll = get_youtube_collection()
        elif platform.lower() == "facebook":
            coll = get_facebook_collection()
        else:
            raise HTTPException(status_code=400, detail="Platform must be 'instagram', 'twitter', 'youtube', or 'facebook'")
        
        
        cursor = coll.find({"profile": target_username})
        profile_docs = list(cursor)
        
        if not profile_docs:
            return {"posts": []}
        
        
        post_urls = []
        for doc in profile_docs:
            if platform.lower() in ["instagram", "youtube", "facebook"]:
               
                if "posts" in doc:
                    for post in doc["posts"]:
                        if post.get("post_url"):
                            post_urls.append(post["post_url"])
            else:  
                if "results" in doc:
                    for tweet_result in doc["results"]:
                        if tweet_result.get("post_url"):
                            post_urls.append(tweet_result["post_url"])
        
        return {"posts": post_urls}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analytics", response_model=AnalyticsResponse, tags=["analytics"])
def get_social_media_analytics(request: AnalyticsRequest):
    """
    Unified analytics endpoint for Instagram, Twitter, and YouTube data.
    Automatically detects platform and analyzes data for a specific post from a profile in MongoDB.
    Generates sentiment analysis, common factors, and engagement insights for the specified post.
    """
    try:
        
        if request.platform.lower() == "instagram":
            coll = get_instagram_collection()
            platform_name = "Instagram"
        elif request.platform.lower() == "twitter":
            coll = mongo.get_sync_collection(os.getenv("MONGO_TWITTER_COLLECTION", "twitter"))
            platform_name = "Twitter"
        elif request.platform.lower() == "youtube":
            coll = get_youtube_collection()
            platform_name = "YouTube"
        elif request.platform.lower() == "facebook":
            coll = get_facebook_collection()
            platform_name = "Facebook"
        else:
            raise HTTPException(status_code=400, detail="Platform must be 'instagram', 'twitter', 'youtube', or 'facebook'")
        
        
        if request.platform.lower() in ["instagram", "youtube", "facebook"]:
           
            cursor = coll.find({"profile": request.target_username})
            profile_docs = list(cursor)
            
            if not profile_docs:
                raise HTTPException(status_code=404, detail=f"No {platform_name} data found for profile: {request.target_username}")
            
            
            target_post = None
            for doc in profile_docs:
                if "posts" in doc:
                    for post in doc["posts"]:
                        if post.get("post_url") == request.post_url:
                            target_post = post
                            break
                    if target_post:
                        break
            
            if not target_post:
                raise HTTPException(status_code=404, detail=f"No {platform_name} post found with URL: {request.post_url}")
            
        else:  
            
            cursor = coll.find({"profile": request.target_username})
            profile_docs = list(cursor)
            
            if not profile_docs:
                raise HTTPException(status_code=404, detail=f"No {platform_name} data found for profile: {request.target_username}")
            
            
            target_post = None
            for doc in profile_docs:
                if "results" in doc:
                    for tweet_result in doc["results"]:
                        if tweet_result.get("post_url") == request.post_url:
                            
                            target_post = {
                                "post_url": tweet_result["post_url"],
                                "content": tweet_result["content"],
                                "comments": [{"text": comment} for comment in tweet_result.get("comments", [])]
                            }
                            break
                    if target_post:
                        break
            
            if not target_post:
                raise HTTPException(status_code=404, detail=f"No {platform_name} post found with URL: {request.post_url}")
        
        
        insights = extract_comment_insights([target_post])
        ai_analysis = insights["ai_analysis"]
        
        response = AnalyticsResponse(
            profile=request.target_username,
            total_posts=1, 
            total_comments=insights["total_comments"],
            sentiment_analysis={
                "positive_percentage": ai_analysis.get("sentiment_percentages", {}).get("positive", 0),
                "negative_percentage": ai_analysis.get("sentiment_percentages", {}).get("negative", 0),
                "neutral_percentage": ai_analysis.get("sentiment_percentages", {}).get("neutral", 0),
                "overall_sentiment": "positive" if ai_analysis.get("sentiment_percentages", {}).get("positive", 0) > 50 else "mixed"
            },
            common_factors=ai_analysis.get("common_themes", []),
            top_commenters=insights["top_commenters"],
            engagement_insights={
                "total_comment_likes": insights["total_comment_likes"],
                "avg_comments_per_post": insights["total_comments"],  
                "engagement_quality": ai_analysis.get("engagement_quality", "Unknown"),
                "notable_patterns": ai_analysis.get("notable_patterns", "No patterns identified"),
                "platform": platform_name
            }
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analytics generation failed: {str(e)}")
