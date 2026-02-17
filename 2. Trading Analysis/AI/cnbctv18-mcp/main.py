import feedparser
import httpx
import asyncio
from mcp.server.fastmcp import FastMCP
from typing import List, Dict, Any

# Create an MCP server
mcp = FastMCP("CNBC News")

# Base URL for CNBC RSS feeds
BASE_URL = "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/{category}.xml"
CATEGORIES = ["market", "business", "economy", "india", "world", "latest"]

async def fetch_feed(client, category):
    url = BASE_URL.format(category=category)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    try:
        response = await client.get(url, headers=headers, timeout=10.0)
        if response.status_code == 200:
            feed = feedparser.parse(response.text)
            return [{
                "title": entry.get("title", "").strip(),
                "summary": entry.get("description", entry.get("summary", "")).strip(),
                "link": entry.get("link", ""),
                "date": entry.get("published", entry.get("pubDate", "")),
                "source": category
            } for entry in feed.entries]
    except Exception:
        pass
    return []

@mcp.tool()
async def get_cnbc_news(category: str = "latest", count: int = 50) -> Dict[str, Any]:
    """
    Fetch news from CNBC TV18 RSS feeds ("market", "business", "economy", "india", "world", "latest", "all"), Use 'all' for all categories.
    Minimum count is 20 and Maximum is 50, Mostly use 50 count otherwise user ask full feed, Use count=-1 for full feed. 
    """
    category = category.lower().strip()
    
    async with httpx.AsyncClient() as client:
        if category == "all":
            # Concurrent fetching of all categories
            tasks = [fetch_feed(client, cat) for cat in CATEGORIES]
            results = await asyncio.gather(*tasks)
            
            # Flatten, deduplicate by link
            all_articles = []
            seen_links = set()
            for feed_results in results:
                for article in feed_results:
                    if article["link"] not in seen_links:
                        all_articles.append(article)
                        seen_links.add(article["link"])
            
            articles = all_articles
        else:
            # Single category fetch
            articles = await fetch_feed(client, category)

        if not articles or (len(articles) == 1 and "error" in articles[0]):
            return {"error": f"No news found for '{category}'. Available categories: {', '.join(CATEGORIES)}, all."}
        
        # Apply count slicing
        if count != -1:
            articles = articles[:count]
        
        return {"category": category, "count": len(articles), "articles": articles}

@mcp.tool()
async def get_custom_rss_news(url: str, count: int = 50) -> Dict[str, Any]:
    """
    Fetch and parse news from any given RSS URL.
    Minimum count is 20 and Maximum is 50, Mostly use 50 count otherwise user ask full feed, Use count=-1 for full feed. 
    """
    async with httpx.AsyncClient() as client:
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = await client.get(url, headers=headers, timeout=10.0)
            response.raise_for_status()
            feed = feedparser.parse(response.text)
            
            articles = [{
                "title": entry.get("title", ""),
                "summary": entry.get("description", entry.get("summary", "")).strip(),
                "link": entry.get("link", ""),
                "date": entry.get("published", entry.get("pubDate", ""))
            } for entry in feed.entries]
            
            if not articles:
                return {"error": "No articles found in the provided RSS feed."}

            # Apply count slicing
            if count != -1:
                articles = articles[:count]

            return {"url": url, "count": len(articles), "articles": articles}
        except Exception as e:
            return {"error": f"Error fetching RSS feed: {str(e)}"}

# if __name__ == "__main__":
#     mcp.run()

if __name__ == "__main__":
    print("🔵 Starting CNBC News MCP server...")
    mcp.run(transport="stdio")






# import feedparser
# import httpx
# import asyncio
# from mcp.server.fastmcp import FastMCP
# from typing import List, Dict, Any

# # Create an MCP server
# mcp = FastMCP("CNBC News")

# # Base URL for CNBC RSS feeds
# BASE_URL = "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/{category}.xml"
# CATEGORIES = ["market", "business", "economy", "india", "world", "latest"]

# async def fetch_feed(client, category):
#     url = BASE_URL.format(category=category)
#     headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
#     try:
#         response = await client.get(url, headers=headers, timeout=10.0)
#         if response.status_code == 200:
#             feed = feedparser.parse(response.text)
#             return [{
#                 "title": entry.get("title", "").strip(),
#                 "summary": entry.get("description", entry.get("summary", "")).strip(),
#                 "link": entry.get("link", ""),
#                 "date": entry.get("published", entry.get("pubDate", "")),
#                 "source": category
#             } for entry in feed.entries]
#     except Exception:
#         pass
#     return []

# @mcp.tool()
# async def get_cnbc_news(category: str = "latest") -> List[Dict[str, Any]]:
#     """
#     Fetch news from CNBC TV18 RSS feeds "market", "business", "economy", "india", "world", "latest". Use 'all' to get news from all categories.
#     """
#     category = category.lower().strip()
    
#     async with httpx.AsyncClient() as client:
#         if category == "all":
#             # Concurrent fetching of all categories
#             tasks = [fetch_feed(client, cat) for cat in CATEGORIES]
#             results = await asyncio.gather(*tasks)
            
#             # Flatten, deduplicate by link, and sort (if possible, though pubDate format varies)
#             all_articles = []
#             seen_links = set()
#             for feed_results in results:
#                 for article in feed_results:
#                     if article["link"] not in seen_links:
#                         all_articles.append(article)
#                         seen_links.add(article["link"])
            
#             return all_articles[:50]  # Return more for 'all', but still limit to avoid context blowup
        
#         # Single category fetch
#         articles = await fetch_feed(client, category)
#         if not articles:
#             return [{"error": f"No news found for '{category}'. Try: {', '.join(CATEGORIES)} or 'all'."}]
        
#         return articles[:20]

# @mcp.tool()
# async def get_custom_rss_news(url: str) -> List[Dict[str, Any]]:
#     """
#     Fetch and parse news from any given RSS URL.
#     """
#     async with httpx.AsyncClient() as client:
#         try:
#             headers = {"User-Agent": "Mozilla/5.0"}
#             response = await client.get(url, headers=headers, timeout=10.0)
#             response.raise_for_status()
#             feed = feedparser.parse(response.text)
            
#             return [{
#                 "title": entry.get("title", ""),
#                 "summary": entry.get("description", ""),
#                 "link": entry.get("link", ""),
#                 "date": entry.get("published", "")
#             } for entry in feed.entries[:20]]
#         except Exception as e:
#             return [{"error": str(e)}]

# if __name__ == "__main__":
#     mcp.run()