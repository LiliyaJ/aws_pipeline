import pandas as pd
import json
import os
from client import RestClient

DFS_LOGIN = os.getenv('DFS_LOGIN')
print(DFS_LOGIN)
DFS_KEY = os.getenv('DFS_KEY')
print(DFS_KEY)

def fetch_and_extract_search_volume(client, login, key, keywords, location, language):
    """
    Fetches historical search volume data from DataForSEO API and extracts results in JSON format.

    Args:
        client (RestClient): Initialized RestClient instance for API communication.
        login (str): DataForSEO API login.
        key (str): DataForSEO API key.
        keywords (list): List of keywords to fetch data for.
        location (str): Location name (e.g., "Germany").
        language (str): Language name (e.g., "German").

    Returns:
        dict: Extracted search volume results or error response.
    """
    # Initialize client with credentials
    client = RestClient(login, key)
    
    # Prepare POST data
    post_data = {
        len(post_data): {
            "keywords": keywords,
            "location_name": location,
            "language_name": language
        }
    }
    
    # Make POST request
    response = client.post("/v3/dataforseo_labs/google/historical_search_volume/live", post_data)

    # Check response status
    if response.get("status_code") == 20000:
        # Extract search volume data
        results = []
        for task in response.get("tasks", []):
            for result in task.get("result", []):
                for item in result.get("items", []):
                    keyword = item.get("keyword")
                    monthly_searches = item["keyword_info"].get("monthly_searches", [])
                    for monthly in monthly_searches:
                        results.append({
                            "keyword": keyword,
                            "year": monthly["year"],
                            "month": monthly["month"],
                            "search_volume": monthly["search_volume"]
                        })
        return {"status": "success", "data": results}
    else:
        # Return error response
        return {
            "status": "error",
            "code": response.get("status_code"),
            "message": response.get("status_message")
        }
    

def extract_search_volume_to_json(json_data):
    """
    Extracts search volume data from the provided JSON object and formats it as a JSON structure.

    Args:
        json_data (dict): The JSON object containing search volume data.

    Returns:
        list: A list of dictionaries with extracted search volume data.
    """
    results = []
    for task in json_data.get("tasks", []):
        for result in task.get("result", []):
            for item in result.get("items", []):
                keyword = item.get("keyword")
                monthly_searches = item["keyword_info"].get("monthly_searches", [])
                for monthly in monthly_searches:
                    results.append({
                        "keyword": keyword,
                        "year": monthly["year"],
                        "month": monthly["month"],
                        "search_volume": monthly["search_volume"]
                    })
    return results