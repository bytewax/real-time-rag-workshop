
import os
from dotenv import load_dotenv  
import json
import requests
from haystack import component 
from typing import Any, Dict, List, Union
from pathlib import Path

load_dotenv(override=True)
api_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")

index_name = "bytewax-index"
search_api_version = '2023-11-01'
search_api_key = api_key

@component
class PopulateAzureAISearch:
    # Azure Search endpoint and headers for uploading documents  
    
    def __init__(self):  

        search_endpoint = f'https://bytewax-workshop.search.windows.net/indexes/{index_name}/docs/index?api-version={search_api_version}'  
        headers = {  
            'Content-Type': 'application/json',  
            'api-key': search_api_key  
        }  
    
        self.search_endpoint = search_endpoint
        self.headers = headers

    @component.output_types(documents=Dict)
    def run(self, sources: List[Union[str, Path]]):
        # Convert DataFrame to the format expected by Azure Search  
        body = json.dumps({  
            "value": [  
                {  
                    "@search.action": "upload",  
                    "id": sources['id'],  
                    "meta": sources['meta'],  
                    "content": sources['content'],  
                    "vector": sources['vector']  # Include the generated embeddings  
                } 
            ]  
        })  
        
        # Upload documents to Azure Search  
        response = requests.post(self.search_endpoint, 
                                 headers=self.headers, data=body)  
        if response.status_code == 200:  
            print("Documents uploaded successfully")  
        else:  
            print(f"Failed to upload documents. Status code: {response.status_code}, Details: {response.text}")