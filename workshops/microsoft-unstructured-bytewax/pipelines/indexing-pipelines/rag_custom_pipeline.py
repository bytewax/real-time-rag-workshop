from haystack import Pipeline
from haystack.components.embedders import AzureOpenAIDocumentEmbedder
from haystack.components.preprocessors import DocumentCleaner
from pathlib import Path
from haystack.utils import Secret
from haystack_integrations.components.converters.unstructured import UnstructuredFileConverter


from unstructured_component import UnstructuredParser
import logging
import requests
from haystack import component, Document
from typing import Any, Dict, List, Optional, Union
from haystack.dataclasses import ByteStream

import json
from dotenv import load_dotenv
import os

load_dotenv("../.env")
unstructured_api_key = os.environ.get("UNSTRUCTURED_API_KEY")
api_key = os.environ.get("news_api")
open_ai_key = os.environ.get("OPENAI_API_KEY")
unstructured = os.environ.get("UNSTRUCTURED")

AZURE_OPENAI_KEY = os.getenv('AZURE_OPENAI_API_KEY')
AZURE_OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')
AZURE_OPENAI_SERVICE = os.getenv('AZURE_OPENAI_SERVICE')
AZURE_OPENAI_EMBEDDING_SERVICE= os.getenv('AZURE_OPENAI_EMBEDDING_SERVICE')

search_api_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def flatten_meta(meta):
    """
    Flatten a nested dictionary to a single-level dictionary.
    
    :param meta: Nested dictionary to flatten.
    :return: Flattened dictionary.
    """
    def _flatten(d, parent_key=''):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}_{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(_flatten(v, new_key).items())
            else:
                items.append((new_key, str(v) if not isinstance(v, (str, int, float, bool)) else v))
        return dict(items)
    
    return _flatten(meta)


def safe_deserialize(data):
    """
    Safely deserialize JSON data, handling various formats.
    
    :param data: JSON data to deserialize.
    :return: Deserialized data or None if an error occurs.
    """
    try:
        parsed_data = json.loads(data)
        if isinstance(parsed_data, list):
            if len(parsed_data) == 2 and (parsed_data[0] is None or isinstance(parsed_data[0], str)):
                event = parsed_data[1]
            else:
                logger.info(f"Skipping unexpected list format: {data}")
                return None
        elif isinstance(parsed_data, dict):
            event = parsed_data
        else:
            logger.info(f"Skipping unexpected data type: {data}")
            return None
        
        if 'link' in event:
            event['url'] = event.pop('link')
        
        if "url" in event:
            return event
        else:
            logger.info(f"Missing 'url' key in data: {data}")
            return None

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error ({e}) for data: {data}")
        return None
    except Exception as e:
        logger.error(f"Error processing data ({e}): {data}")
        return None


class JSONLReader:
    def __init__(self, metadata_fields=None):
        """
        Initialize the JSONLReader with optional metadata fields and a link keyword.
        
        :param metadata_fields: List of fields in the JSONL to retain as metadata.
        """
        self.metadata_fields = metadata_fields or []
        
        unstructured_parser = UnstructuredParser(unstructured_key=unstructured_api_key,
                                          chunking_strategy="by_page",
                                          strategy="auto",
                                          model="yolox")
        regex_pattern = (
            r'<.*?>'  # HTML tags
            r'|\t'  # Tabs
            r'|\n+'  # Newlines
            r'|&nbsp;'  # Non-breaking spaces
            r'|[^a-zA-Z0-9\s]'  # Any non-alphanumeric character (excluding whitespace)
        )
        document_cleaner = DocumentCleaner(
                            remove_empty_lines=True,
                            remove_extra_whitespaces=True,
                            remove_repeated_substrings=False,
                            remove_substrings=None,  
                            remove_regex=regex_pattern
                        )

        document_embedder = AzureOpenAIDocumentEmbedder(azure_endpoint=AZURE_OPENAI_ENDPOINT,
                                                                api_key=Secret.from_token(AZURE_OPENAI_KEY),
                                                                azure_deployment=AZURE_OPENAI_EMBEDDING_SERVICE) 


        # Initialize pipeline
        self.pipeline = Pipeline()

        # Add components
        self.pipeline.add_component("unstructured", unstructured_parser)
        self.pipeline.add_component("cleaner", document_cleaner)
        self.pipeline.add_component("embedder", document_embedder)

        # Connect components
        self.pipeline.connect("unstructured", "cleaner")
        self.pipeline.connect("cleaner", "embedder")

    @component.output_types(documents=List[Document])
    def run(self, event: List[Union[str, Path, ByteStream]]):
        """
        Process each source file, read URLs and their associated metadata,
        fetch HTML content using a pipeline, and convert to Haystack Documents.
        :param event: A list of source files, URLs, or ByteStreams.
        :return: A dictionary containing the processed document and the result of writing to Azure Search.
        """

        # Extract URL and modify it if necessary
        url = event.get("url")
        if url and '-index.html' in url:
            url = url.replace('-index.html', '.txt')

        # else:
        metadata = {field: event.get(field) for field in self.metadata_fields if field in event}
        # Assume a pipeline fetches and processes this URL
        try:
            doc = self.pipeline.run({"unstructured": {"sources": [url]}})
        except Exception as e:
            logger.error(f"Error running pipeline for URL {url}: {e}")
            raise
        document_obj = doc['embedder']['documents'][0]
        content = document_obj.content
        additional_metadata = document_obj.meta

        
        embedding = document_obj.embedding
        # Safely access the embedding metadata
        embedding_metadata = doc.get('embedder', {}).get('meta', {})
        metadata.update(embedding_metadata) 
        document = Document(id=document_obj.id, content=content, meta=metadata, embedding=embedding)

        dictionary = self.document_to_dict(document)

        # # write to Azure Search
        result = self.write_to_ai_search(dictionary)

        results = {"document": dictionary, "result": result}
        return results
    
    def document_to_dict(self, document: Document) -> Dict:
        """
        Convert a Haystack Document object to a dictionary.
        """
        # Ensure embedding is converted to a list, if it is a NumPy array
        embedding = document.embedding
        if embedding is not None and hasattr(embedding, 'tolist'):
            embedding = embedding.tolist()
        
        flattened_meta = flatten_meta(document.meta)
        
        return {
            "id": document.id,
            "content": document.content,
            "meta": json.dumps(flattened_meta),  # Serialize the flattened meta to a JSON string
            "vector": embedding
        }

    def write_to_ai_search(self, dictionary):
        index_name = "bytewax-index"
        search_api_version = '2023-11-01'
        search_endpoint = f'https://bytewax-workshop.search.windows.net/indexes/{index_name}/docs/index?api-version={search_api_version}'  
        headers = {  
            'Content-Type': 'application/json',  
            'api-key': search_api_key  
        }  

        # Use the flattened meta directly
        flattened_meta = dictionary['meta']
        
        # Convert DataFrame to the format expected by Azure Search  
        body = json.dumps({  
            "value": [  
                {  
                    "@search.action": "upload",  
                    "id": dictionary['id'],  
                    "content": dictionary['content'],  
                    "meta": flattened_meta,  # Use flattened meta
                    "vector": dictionary['vector']  # Include the generated embeddings  
                } 
            ]  
        })  
        
        # Upload documents to Azure Search  
        response = requests.post(search_endpoint, 
                                 headers=headers, data=body) 

        return {"status": "success" if response.status_code == 200 else response.text}
       