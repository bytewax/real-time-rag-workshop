from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.connectors.files import FileSource

from haystack.components.preprocessors import DocumentCleaner
from haystack.components.embedders import OpenAIDocumentEmbedder
from haystack import Pipeline
from haystack.components.embedders import OpenAIDocumentEmbedder
from haystack.components.preprocessors import DocumentCleaner
from haystack.components.preprocessors import DocumentSplitter
from haystack.components.writers import DocumentWriter
from haystack.document_stores.types import DuplicatePolicy
from haystack.document_stores.in_memory import InMemoryDocumentStore
from haystack_integrations.document_stores.elasticsearch import ElasticsearchDocumentStore
from haystack.utils import Secret


from haystack import component, Document
from typing import Any, Dict, List, Optional, Union
from haystack.dataclasses import ByteStream

import json
from dotenv import load_dotenv
import os

import re
from bs4 import BeautifulSoup
from pathlib import Path

import logging


load_dotenv(".env")
open_ai_key = os.environ.get("OPENAI_API_KEY")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        
@component
class BenzingaNews:
    
    @component.output_types(documents=List[Document])
    def run(self, sources: Dict[str, Any]) -> None:
             
        documents = []
        for source in sources:
        
            for key in source:
                if type(source[key]) == str:
                    source[key] = self.clean_text(source[key])
                    
            if source['content'] == "":
                continue

            #drop content from source dictionary
            content = source['content']
            document = Document(content=content, meta=source) 
            
            documents.append(document)
         
        return {"documents": documents}
               
    def clean_text(self, text):
        # Remove HTML tags using BeautifulSoup
        soup = BeautifulSoup(text, "html.parser")
        text = soup.get_text()
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
@component
class BenzingaEmbeder:
    
    def __init__(self):
        get_news = BenzingaNews()
        document_store = ElasticsearchDocumentStore(embedding_similarity_function="cosine", hosts = "http://localhost:9200")
        document_cleaner = DocumentCleaner(
                            remove_empty_lines=True,
                            remove_extra_whitespaces=True,
                            remove_repeated_substrings=False
                        )
        document_splitter = DocumentSplitter(split_by="passage", split_length=5)
        document_writer = DocumentWriter(document_store=document_store,
                                        policy = DuplicatePolicy.OVERWRITE)
        embedding = OpenAIDocumentEmbedder(api_key=Secret.from_token(open_ai_key))

        self.pipeline = Pipeline()
        self.pipeline.add_component("get_news", get_news)
        self.pipeline.add_component("document_cleaner", document_cleaner)
        self.pipeline.add_component("document_splitter", document_splitter)
        #self.pipeline.add_component("embedding", embedding)
        self.pipeline.add_component("document_writer", document_writer)

        self.pipeline.connect("get_news", "document_cleaner")
        self.pipeline.connect("document_cleaner", "document_splitter")
        self.pipeline.connect("document_splitter", "document_writer")
        #self.pipeline.connect("embedding", "document_writer")
        
        
    @component.output_types(documents=List[Document])
    def run(self, event: List[Union[str, Path, ByteStream]]):
        
        documents = self.pipeline.run({"get_news": {"sources": [event]}})
        
        self.pipeline.draw("benzinga_pipeline.png")
        return documents
    
    
embed_benzinga = BenzingaEmbeder()

def process_event(event):
    """Wrapper to handle the processing of each event."""
    if event:
        document = embed_benzinga.run(event)
        return document
    return None


flow = Dataflow("rag-pipeline")
input_data = op.input("input", flow, FileSource("data/news_out.jsonl"))
deserialize_data = op.map("deserialize", input_data, safe_deserialize)
get_content = op.map("embed_content", deserialize_data, process_event)
op.output("output", get_content, StdOutSink())


