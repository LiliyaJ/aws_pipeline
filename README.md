# Example Pipeline in AWS environment

Example project for ingesting, analysing and visualising data inside aws environment

Function 'fetch_and_extract_search_volume' fetches historical search volume data from DataForSEO API and extracts results in JSON format. The function is stored in helper.py which will be used later in the main Lambda function.

Function 'extract_search_volume_to_json' extracts search volume data from the provided JSON object and formats it as a JSON structure.

Function 'extract_search_volume' extracts search volume data from the provided JSON object and saves it into a pandas df.
