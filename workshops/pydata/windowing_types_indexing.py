import time
from datetime import datetime, timedelta, timezone
import json
import logging
import re

import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators import windowing as wop
from bytewax.operators.windowing import EventClock, TumblingWindower, SessionWindower, SlidingWindower
from haystack import Pipeline
from haystack.components.embedders import OpenAIDocumentEmbedder
from haystack.components.preprocessors import DocumentCleaner, DocumentSplitter
from haystack.components.writers import DocumentWriter
from haystack.document_stores.types import DuplicatePolicy
from haystack_integrations.document_stores.elasticsearch import ElasticsearchDocumentStore
from haystack.utils import Secret
from haystack import component, Document
from typing import Any, Dict, List, Optional, Union
from haystack.dataclasses import ByteStream
from pathlib import Path
from dotenv import load_dotenv
import os
from bs4 import BeautifulSoup

load_dotenv(".env")
open_ai_key = os.environ.get("OPENAI_API_KEY")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_deserialize(data):
    """Safely deserialize JSON data, handling various formats."""
    try:
        parsed_data = json.loads(data)

        if isinstance(parsed_data, dict):
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

def parse_time(parsed_data):
    """Convert time from string to datetime"""
    for item in ['created_at', 'updated_at']:
        time_min_t = re.sub("T", " ", parsed_data[item])
        time_min_ms = re.sub(r":*Z", "", time_min_t)
        time_ = time.strptime(time_min_ms, "%Y-%m-%d %H:%M:%S")

        parsed_data[item] = datetime(year=time_.tm_year,
                                     month=time_.tm_mon,
                                     day=time_.tm_mday,
                                     hour=time_.tm_hour,
                                     minute=time_.tm_min,
                                     second=time_.tm_sec,
                                     tzinfo=timezone.utc)

    return parsed_data

# Haystack components
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

            content = source['content']
            document = Document(content=content, meta=source)
            documents.append(document)

        return {"documents": documents}

    def clean_text(self, text):
        soup = BeautifulSoup(text, "html.parser")
        text = soup.get_text()
        text = re.sub(r'\s+', ' ', text).strip()
        return text

@component
class BenzingaEmbeder:

    def __init__(self):
        get_news = BenzingaNews()
        document_cleaner = DocumentCleaner(remove_empty_lines=True, remove_extra_whitespaces=True, remove_repeated_substrings=False)
        document_splitter = DocumentSplitter(split_by="passage", split_length=5)
        embedding = OpenAIDocumentEmbedder(api_key=Secret.from_token(open_ai_key))

        self.pipeline = Pipeline()
        self.pipeline.add_component("get_news", get_news)
        self.pipeline.add_component("document_cleaner", document_cleaner)
        self.pipeline.add_component("document_splitter", document_splitter)


        self.pipeline.connect("get_news", "document_cleaner")
        self.pipeline.connect("document_cleaner", "document_splitter")


    @component.output_types(documents=List[Document])
    def run(self, event: List[Union[str, Path, ByteStream]]):
        documents = self.pipeline.run({"get_news": {"sources": [event]}})
        self.pipeline.draw("benzinga_pipeline.png")
        return documents

embed_benzinga = BenzingaEmbeder()

def process_event(event):
    # Unpack the tuple to get the event ID and list of dictionaries
    event_id, event_data = event
    
    try: 
       
        for single_event in event_data:
            
            # Ensure that each item in the list is a dictionary
            if isinstance(single_event, list):
                
                documents = embed_benzinga.run(single_event[0])
                return documents
    except Exception as e:
        print("Error", e)
        return None

# Set up the dataflow
flow = Dataflow("rag-pipeline")
input_data = op.input("input", flow, FileSource("data/news_out.jsonl"))
deserialize_data = op.filter_map("deserialize", input_data, safe_deserialize)
transform_data_time = op.map("timeconversion", deserialize_data, parse_time)

# Map the tuple to ensure consistent structure
map_tuple = op.map(
    "tuple_map",
    transform_data_time,
    lambda reading_data: (str(reading_data["id"]), {
        "created_at": reading_data['created_at'],
        "updated_at": reading_data['updated_at'],
        "headline": reading_data['headline'],
        "content": reading_data['content']}
    ),
)

event_time_config = EventClock(ts_getter=lambda e: e['updated_at'], wait_for_system_duration=timedelta(seconds=1))
align_to = datetime(2024, 5, 29, tzinfo=timezone.utc)
clock_config = SlidingWindower(length=timedelta(seconds=10), offset=timedelta(seconds=5), align_to=align_to)

window = wop.collect_window(
    "windowed_data", map_tuple, clock=event_time_config, windower=clock_config
)

calc = op.filter_map("embed_content", window.down, process_event)
op.output("output", calc, StdOutSink())
