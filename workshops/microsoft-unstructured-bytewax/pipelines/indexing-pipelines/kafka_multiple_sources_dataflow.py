from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.connectors.files import FileSource
from bytewax.testing import run_main

from haystack import Pipeline
from haystack.components.embedders import AzureOpenAIDocumentEmbedder
from haystack.components.preprocessors import DocumentCleaner
from haystack.components.preprocessors import DocumentSplitter
from haystack.components.writers import DocumentWriter
from pathlib import Path
from haystack.document_stores.types import DuplicatePolicy
from haystack.utils import Secret
from haystack_integrations.components.converters.unstructured import UnstructuredFileConverter
from haystack.components.fetchers import LinkContentFetcher
from haystack.components.converters import HTMLToDocument
from haystack.document_stores.in_memory import InMemoryDocumentStore 

from unstructured_component import UnstructuredParser
import logging

from haystack import component, Document
from typing import Any, Dict, List, Optional, Union
from haystack.dataclasses import ByteStream
from dotenv import load_dotenv
import os
import json 

from azure_components import PopulateAzureAISearch

load_dotenv("../.env")
unstructured_api_key = os.environ.get("UNSTRUCTURED_API_KEY")
api_key = os.environ.get("news_api")
open_ai_key = os.environ.get("OPENAI_API_KEY")
unstructured = os.environ.get("UNSTRUCTURED")

AZURE_OPENAI_KEY = os.getenv('AZURE_OPENAI_API_KEY')
AZURE_OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')
AZURE_OPENAI_SERVICE = os.getenv('AZURE_OPENAI_SERVICE')
AZURE_OPENAI_EMBEDDING_SERVICE= os.getenv('AZURE_OPENAI_EMBEDDING_SERVICE')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def safe_deserialize(data):
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
        :param sources: File paths or ByteStreams to process.
        :return: A list of Haystack Documents.
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

        return document
    
    def document_to_dict(self, document: Document, ) -> Dict:
        """
        Convert a Haystack Document object to a dictionary.
        """
        # Ensure embedding is converted to a list, if it is a NumPy array
        embedding = document.embedding
        if embedding is not None and hasattr(embedding, 'tolist'):
            embedding = embedding.tolist()
       
        return {
                "id": document.id,
                "content": document.content,
                "meta": document.meta,
                "vector": embedding
            }
       


jsonl_reader = JSONLReader(metadata_fields=['title', 'form_type', 'url'])

def process_event(event):
    """Wrapper to handle the processing of each event."""
    if event:
        document = jsonl_reader.run(event)
        return jsonl_reader.document_to_dict(document)
    return None

flow = Dataflow("rag-pipeline")
edgar_k_input = op.input("input", flow, KafkaSource())
news_input = op.input("input", flow, KafkaSource())

edgar_deser = op.map("deserialize", edgar_input, safe_deserialize)
edgar_dicts = op.map("extract_html", edgar_deser, process_event)

news_deser = op.map("deserialize", news_input, safe_deserialize)
news_dicts = op.map("extract_html", news_deser, process_event)

merged_stream = op.merge("merge", news_dicts, edgar_dicts)
op.output("", merged_stream, PopulateAzureAISearch())