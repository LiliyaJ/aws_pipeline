import pandas as pd
import json
import os
from client import RestClient

DFS_LOGIN = os.getenv('DFS_LOGIN')
print(DFS_LOGIN)
DFS_KEY = os.getenv('DFS_KEY')
print(DFS_KEY)

# client = RestClient(DFS_LOGIN, DFS_KEY)
# post_data = dict()
# # simple way to set a task
# post_data[len(post_data)] = dict(
#     keywords=[
#         "data analyst",
#         "analytics engineer"
#     ],
#     location_name="Germany",
#     language_name="Geman"
# )
# # POST /v3/dataforseo_labs/google/historical_search_volume/live
# response = client.post("/v3/dataforseo_labs/google/historical_search_volume/live", post_data)
# # you can find the full list of the response codes here https://docs.dataforseo.com/v3/appendix/errors
# if response["status_code"] == 20000:
#     print(response)
#     # do something with result
# else:
#     print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))