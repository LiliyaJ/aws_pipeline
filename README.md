# Example Pipeline in AWS environment

Example project for ingesting, analysing and visualising data inside aws environment

Function 'fetch_and_extract_search_volume' fetches historical search volume data from DataForSEO API and extracts results in JSON format. The function is stored in helper.py which will be used later in the main Lambda function.

Function 'extract_search_volume_to_json' extracts search volume data from the provided JSON object and formats it as a JSON structure.

Function 'extract_search_volume' extracts search volume data from the provided JSON object and saves it into a pandas df.

Normalization Plan

Tasks Table

Purpose: To store task-level information.
Columns:
Task ID
Status Code
Status Message
Path
Location Name
Language Name
Keywords (as an array or separate rows)
API Function
Cost
Time

Keyword Info Table
Purpose: To store information related to individual keywords.
Columns:
Task ID (foreign key)
Keyword
Location Code
Language Code
Search Volume
CPC (Cost Per Click)
Competition Level
Categories (as an array or separate rows)

Monthly Search Volume Table
Purpose: To track search volume trends over time for each keyword.
Columns:
Task ID (foreign key)
Keyword
Year
Month
Search Volume

Impressions Table

Purpose: To store information about ad impressions for each keyword.
Columns:
Task ID (foreign key)
Keyword
Bid
CPC (min/max/average)
Ad Position (min/max/average)
Daily Impressions (min/max/average)
Daily Clicks (min/max/average)
Daily Cost (min/max/average)

Error Log Table (Optional)
Purpose: To log errors for failed tasks.
Columns:
Task ID
Error Code
Error Message
Timestamp
