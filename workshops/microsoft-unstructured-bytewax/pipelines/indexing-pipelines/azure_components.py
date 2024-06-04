
import os
from dotenv import load_dotenv  
import json
import requests
from haystack import component 
from typing import Any, Dict, List, Union
from pathlib import Path
from haystack.dataclasses import ByteStream

load_dotenv(override=True)
api_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")

index_name = "bytewax-index"
search_api_version = '2023-11-01'
search_api_key = api_key

@component
class PopulateAzureAISearch:
    # Azure Search endpoint and headers for uploading documents  
    
    def __init__(self, search_endpoint, headers):  

        
    
        self.search_endpoint = search_endpoint
        self.headers = headers

    @component.output_types(documents=List[Dict[str, Any]])
    def run(self, sources:List[Union[str, Path, ByteStream]]):
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

        return {"status": "success" if response.status_code == 200 else response.text} 
