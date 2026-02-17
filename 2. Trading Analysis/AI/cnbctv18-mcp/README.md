# CNBC News MCP Server

A Model Context Protocol (MCP) server that provides access to the latest news from CNBC TV18.

## Features

- **get_cnbc_news**: Tool to fetch news for various categories.
- **Categories supported**: `latest`, `market`, `business`, `economy`, `india`, `world`.
- **Fast and Asynchronous**: Built with `FastMCP` and `httpx`.

## Setup

1. **Clone/Copy the files** into your project directory.
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration for MCP Clients (e.g., Claude Desktop)

Add the following to your MCP configuration file:

```json
{
  "mcpServers": {
    "cnbc-news-mcp": {
      "command": "mcp",
      "args": [
        "run",
        "D:\\Prasad\\Trading\\01. Py\\MCP\\cnbctv18-mcp\\main.py"
      ]
    }
  }
}
```

## Tools

### 1. `get_cnbc_news`
Fetches news for a specific category or all categories at once.
- **Parameters**:
  - `category`: `market`, `business`, `economy`, `india`, `world`, `latest`, or `all`.
  - `count`: Number of articles to return (Default: 50). Use `-1` for the full feed.
- **Logic**: Automatically builds the URL: `https://www.cnbctv18.com/commonfeeds/v1/cne/rss/{category}.xml`

### 2. `get_custom_rss_news`
Fetches news from any provided RSS URL.
- **Parameters**:
  - `url`: The RSS feed URL.
  - `count`: Number of articles to return (Default: 50). Use `-1` for the full feed.

## Example Usage
- "Latest 5 market news" -> calls `get_cnbc_news(category="market", count=5)`
- "Show me all latest news (full feed)" -> calls `get_cnbc_news(category="latest", count=-1)`
- "Show me all news from CNBC" -> calls `get_cnbc_news(category="all")`
- "Give me business updates" -> calls `get_cnbc_news(category="business")`