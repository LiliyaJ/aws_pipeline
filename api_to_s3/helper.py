from client import RestClient

def fetch_historical_search_volume(keywords, locations_languages, dfs_login, dfs_key):
    """
    Fetch historical search volume using DataForSEO API.

    Parameters:
        keywords (list): List of keywords to search.
        locations_languages (dict): Dictionary mapping locations to language names.
        dfs_login (str): DataForSEO API login.
        dfs_key (str): DataForSEO API key.

    Returns:
        dict: Combined responses from DataForSEO API.
    """
    client = RestClient(dfs_login, dfs_key)
    combined_responses = []

    for location, language in locations_languages.items():
        post_data = dict()

        # Adding task to post_data
        post_data[len(post_data)] = dict(
            keywords=keywords,
            location_name=location,
            language_name=language  # Dynamically set the language based on location
        )

        # POST /v3/dataforseo_labs/google/historical_search_volume/live
        response = client.post("/v3/dataforseo_labs/google/historical_search_volume/live", post_data)

        if response["status_code"] == 20000:
            print(f"Success for location: {location}")
            combined_responses.append(response)
        else:
            error_message = f"Error for location {location}. Code: {response['status_code']} Message: {response['status_message']}"
            print(error_message)
            combined_responses.append({"error": error_message})

    return combined_responses