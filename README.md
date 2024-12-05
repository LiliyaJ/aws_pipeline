# Example Pipeline in AWS environment

Example project for ingesting, analysing and visualising data inside aws environment

Function 'fetch_and_extract_search_volume' makes an API request and saves results a json file. The function is stored in helper.py which will be used later in the main Lambda function.

Function 'extract_search_volume_to_json' takes only those nformation from the response which is relevant to the end user and saves it into a new json.

Function 'extract_search_volume' takes the information from teh response and saves it into a pandas df.
