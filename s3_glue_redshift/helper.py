import json
from datetime import datetime

def validate_json(json_data):
    try:
        # If it's a string, load it into JSON format
        if isinstance(json_data, str):
            json_data = json.loads(json_data)

        # Check for errors in the JSON data
        for entry in json_data:
            if 'error' in entry:
                print(f"Error found: {entry['error']}")
                return False  # Return False if there's an error

        return True  # Return True if no errors found

    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {e}")
        return False


# 1. Tasks Table
# Purpose: To store task-level information.

def transform_tasks_data(data):
    tasks = []
    for task in data:
        task_info = task.get("tasks", [])
        
        for task_entry in task_info:
            keywords = task_entry.get("data", {}).get("keywords", [])
        
            for keyword in keywords:

                entry = {
                    "task_id": task_entry.get("id"),
                    "status_message": task_entry.get("status_message"),
                    "path": task_entry.get("path"),
                    "location_name": task_entry.get("data", {}).get("location_name"),
                    "language_name": task_entry.get("data", {}).get("language_name"),
                    "keywords": keyword,
                    "api_function": task_entry.get("data", {}).get("function"),
                    "cost": task_entry.get("cost"),
                    "time": task_entry.get("time")
                }
                tasks.append(entry)
    return tasks

# 2. Keyword Info Table
# Purpose: To store information related to individual keywords.

def transform_keyword_info(data):
    keyword_info = []
    
    # Loop through the list of responses
    for entry in data:
        # Each entry in the list has a 'tasks' key
        tasks = entry.get("tasks", [])
        
        for task in tasks:
            task_id = task.get("id")
            location_name = task.get("data", {}).get("location_name")
            language_name = task.get("data", {}).get("language_name")
            result_items = task.get("result", [])
            
            # Iterate through the result items
            for result_item in result_items:
                for item in result_item.get("items", []):
                    keyword = item.get("keyword")
                    search_volume = item.get("keyword_info", {}).get("search_volume")
                    compettiton = item.get("keyword_info", {}).get("competition")
                    cpc = item.get("keyword_info", {}).get("cpc")
                    low_top_of_page_bid = item.get("keyword_info", {}).get("low_top_of_page_bid")
                    high_top_of_page_bid = item.get("keyword_info", {}).get("high_top_of_page_bid")
                    competition_level = item.get("keyword_info", {}).get("competition_level")
                    categories = item.get("keyword_info", {}).get("categories", [])
                    monthly_searches = item.get("keyword_info", {}).get("monthly_searches", [])
                    
                    # Flatten monthly_searches and include each as a row with formatted 'year-month'
                    for search_data in monthly_searches:
                        year = search_data.get("year")
                        month = search_data.get("month")
                        if year and month:
                            year_month = datetime(year, month, 1).strftime('%Y-%m')
                            
                            # Flatten categories and include each category as a separate row
                            for category in categories:
                                keyword_info.append({
                                    "task_id": task_id,
                                    "keyword": keyword,
                                    "location_name": location_name,
                                    "language_name": language_name,
                                    "search_volume": search_volume,
                                    "cpc": cpc,
                                    "low_top_of_page_bid": low_top_of_page_bid,
                                    "high_top_of_page_bid": high_top_of_page_bid,
                                    "competition": compettiton,
                                    "competition_level": competition_level,
                                    "category": category,
                                    "year_month": year_month,  # Combined column for year and month
                                })

    return keyword_info


# 3. Monthly Search Volume Table
#Purpose: To track search volume trends over time for each keyword.

def transform_monthly_search_volume(data):
    monthly_search_volume_data = []
    
    # Process each entry in the list
    for entry in data:
        tasks = entry.get("tasks", [])
        for task in tasks:
            task_id = task.get("id", None)
            location_name = task.get("data", {}).get("location_name")
            language_name = task.get("data", {}).get("language_name")
            results = task.get("result", [])
            for result in results:
                # Extract keywords and their monthly search data
                for item in result.get("items", []):
                    keyword = item.get("keyword", None)
                    monthly_searches = item.get("keyword_info", {}).get("monthly_searches", [])
                    
                    # Loop through the monthly search volumes and create individual rows
                    for monthly_data in monthly_searches:
                        year = monthly_data.get("year", None)
                        month = monthly_data.get("month", None)
                        
                        if year and month:
                            # Combine year and month into the %Y-%m format
                            year_month = datetime(year, month, 1).strftime('%Y-%m')
                            
                            # Prepare a dictionary for each entry
                            entry = {
                                "task_id": task_id,
                                "keyword": keyword,
                                "year_month": year_month,  # Combined year-month column
                                "search_volume": monthly_data.get("search_volume", None),
                                "location_name": location_name,
                                "language_name": language_name 
                            }
                            
                            # Append to the result list
                            monthly_search_volume_data.append(entry)

    return monthly_search_volume_data


# 4. Impressions Table
# Purpose: To store information about ad impressions for each keyword.

def transform_impressions_data(data):
    impressions_data = []

    # Process each entry in the list
    for entry in data:
        tasks = entry.get("tasks", [])
        for task in tasks:
            task_id = task.get("id", None)
            location_name = task.get("data", {}).get("location_name")
            language_name = task.get("data", {}).get("language_name")
            results = task.get("result", [])
            
            for result in results:
                # Extract impressions data
                for item in result.get("items", []):
                    keyword = item.get("keyword")
                    impressions_info = item.get("impressions_info", {})
                    last_updated_time = impressions_info.get("last_updated_time").split(' ')[0]
                    
                    # Prepare an entry for each keyword and its impressions
                    entry = {
                        "task_id": task_id,
                        "keyword": keyword,
                        "location_name": location_name,
                        "language_name": language_name,
                        "last_updated_time": last_updated_time,
                        "bid": impressions_info.get("bid"),
                        "match_type": impressions_info.get("match_type"),
                        "ad_position_min": impressions_info.get("ad_position_min"),
                        "ad_position_max": impressions_info.get("ad_position_max"),
                        "ad_position_average": impressions_info.get("ad_position_average"),
                        "cpc_min": impressions_info.get("cpc_min"),
                        "cpc_max": impressions_info.get("cpc_max"),
                        "cpc_average": impressions_info.get("cpc_average"),
                        "impressions_min": impressions_info.get("impressions_min"),
                        "impressions_max": impressions_info.get("impressions_max"),
                        "impressions_average": impressions_info.get("impressions_average"),
                        "daily_clicks_min": impressions_info.get("daily_clicks_min"),
                        "daily_clicks_max": impressions_info.get("daily_clicks_max"),
                        "daily_clicks_average": impressions_info.get("daily_clicks_average"),
                        "daily_cost_min": impressions_info.get("daily_cost_min"),
                        "daily_cost_max": impressions_info.get("daily_cost_max"),
                        "daily_cost_average": impressions_info.get("daily_cost_average")
                    }
                    impressions_data.append(entry)

    return impressions_data
