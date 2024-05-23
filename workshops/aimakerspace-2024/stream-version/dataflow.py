from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.connectors.files import FileSource
from bytewax.testing import run_main

from haystack import Pipeline
from haystack.components.embedders import OpenAIDocumentEmbedder
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
from haystack.components.embedders import AzureOpenAIDocumentEmbedder

from haystack import component, Document
from typing import Any, Dict, List, Optional, Union
from haystack.dataclasses import ByteStream
from dotenv import load_dotenv
import os
import json 

load_dotenv(".env")
open_ai_key = os.environ.get("OPENAI_API_KEY")

def safe_deserialize(data):
    try:
        # Attempt to load the JSON data
        parsed_data = json.loads(data)
        
        # Check if the data is in the list format with a possible null as the first item
        if isinstance(parsed_data, list):
            if len(parsed_data) == 2 and (parsed_data[0] is None or isinstance(parsed_data[0], str)):
                event = parsed_data[1]  # Select the dictionary which is the second item
            else:
                print(f"Skipping unexpected list format: {data}")
                return None
        elif isinstance(parsed_data, dict):
            # It's the simple dictionary format
            event = parsed_data
        else:
            print(f"Skipping unexpected data type: {data}")
            return None
        
        if 'link' in event:
            event['url'] = event.pop('link')
        # Check for the presence of 'url' key to further validate the data
        if "url" in event:
            return event  # Return the entire event dict or adapt as needed
        else:
            print(f"Missing 'url' key in data: {data}")
            return None

    except json.JSONDecodeError as e:
        print(f"JSON decode error ({e}) for data: {data}")
        return None
    except Exception as e:
        print(f"Error processing data ({e}): {data}")
        return None


class JSONLReader:
    def __init__(self, metadata_fields=None, open_ai_key=None, embedding_flag=False):
        """
        Initialize the JSONLReader with optional metadata fields and a link keyword.
        
        :param metadata_fields: List of fields in the JSONL to retain as metadata.
        """
        self.metadata_fields = metadata_fields or []
        self.embedding_flag = embedding_flag
        
        # Set up cleaning mechanism
        regex_pattern = r"(?i)\bloading\s*\.*\s*|(\s*--\s*-\s*)+"

        fetcher = LinkContentFetcher(retry_attempts=3, timeout=10)
        converter = HTMLToDocument()
        document_cleaner = DocumentCleaner(
                            remove_empty_lines=True,
                            remove_extra_whitespaces=True,
                            remove_repeated_substrings=False,
                            remove_substrings=None,  
                            remove_regex=regex_pattern
                        )
        
        document_splitter = DocumentSplitter(split_by="passage")        
        document_embedder = OpenAIDocumentEmbedder(api_key=Secret.from_token(open_ai_key))                                                   

        # Initialize pipeline
        self.pipeline = Pipeline()

        # Add components
        self.pipeline.add_component("fetcher", fetcher)
        self.pipeline.add_component("converter", converter)
        self.pipeline.add_component("cleaner", document_cleaner)
        self.pipeline.add_component("splitter", document_splitter)
        self.pipeline.add_component("embedder", document_embedder)

        # Connect components
        self.pipeline.connect("fetcher", "converter")
        self.pipeline.connect("converter", "cleaner")
        self.pipeline.connect("cleaner", "splitter")
        self.pipeline.connect("splitter", "embedder")

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
        doc = self.pipeline.run({"fetcher": {"urls": [url]}})
        print(doc)
        document_obj = doc['embedder']['documents'][0]
        content = document_obj.content
        additional_metadata = document_obj.meta

        if self.embedding_flag:
            embedding = document_obj.embedding
            # Safely access the embedding metadata
            embedding_metadata = doc.get('embedder', {}).get('meta', {})
            metadata.update(embedding_metadata) 
            document = Document(id=document_obj.id, content=content, meta=metadata, embedding=embedding)

        metadata.update(additional_metadata)
         # Merge embedding metadata
        document = Document(id=document_obj.id, content=content, meta=metadata)
        return document
    
    def document_to_dict(self, document: Document, ) -> Dict:
        """
        Convert a Haystack Document object to a dictionary.
        """
        # Ensure embedding is converted to a list, if it is a NumPy array
        embedding = document.embedding
        if embedding is not None and hasattr(embedding, 'tolist'):
            embedding = embedding.tolist()
        if self.embedding_flag:
            return {
                "id": document.id,
                "content": document.content,
                "meta": document.meta,
                "embedding": embedding
            }
        else:

            return {
                "id": document.id,
                "content": document.content,
                "meta": document.meta,
             
            }

    

jsonl_reader = JSONLReader(metadata_fields=['symbols', 'headline', 'url'],
                           
                           open_ai_key=open_ai_key,
                           embedding_flag=False)

def process_event(event):
    """Wrapper to handle the processing of each event."""
    if event:
        document = jsonl_reader.run(event)
        return jsonl_reader.document_to_dict(document)
    return None


flow = Dataflow("rag-pipeline")
input_data = op.input("input", flow, FileSource("data/news_out.jsonl"))
deserialize_data = op.map("deserialize", input_data, safe_deserialize)
extract_html = op.map("extract_html", deserialize_data, process_event)

op.output("output", extract_html, StdOutSink())
