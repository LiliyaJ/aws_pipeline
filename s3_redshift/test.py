import json
import pandas as pd
from datetime import datetime
#from transformation_helper import transform_tasks_data, transform_keyword_info, transform_monthly_search_volume, transform_impressions_data, validate_json


with open('/Users/liliyajeromin/Documents/GitHubProjects/aws_pipeline/data/test_json.json') as f:
    data = json.load(f)  # Use json.load() to read the data

import pandas as pd

def transform_impressions_data(data):
    impressions_data = []

    # Process each entry in the list
    for entry in data:
        tasks = entry.get("tasks", [])
        for task in tasks:
            task_id = task.get("id", None)
            location_name = task.get("data", {}).get("location_name")
            language_name = task.get("data", {}).get("language_name")
            result_items = task.get("result", [])

            # Ensure result_items is always a list, even if it's None
            if result_items is None:
                result_items = []

            for result in result_items:
                # Extract impressions data
                for item in result.get("items", []):
                    keyword = item.get("keyword")
                    # Use `or {}` to handle None for impressions_info
                    impressions_info = item.get("impressions_info") or {}
                    last_updated_time = impressions_info.get("last_updated_time", "").split(' ')[0] if impressions_info.get("last_updated_time") else None
                    
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

    return pd.DataFrame(impressions_data)

print(transform_impressions_data(data))
#print(transform_impressions_data(data))
#formatted_data = json.dumps(data, indent=4)
#print(formatted_data)

#print(("2022-04-18 05:57:53 +00:00").split(' ')[0])