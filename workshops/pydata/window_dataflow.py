from bytewax.operators.windowing import EventClock, TumblingWindower

import time

import bytewax.operators as op
import pandas as pd
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators import windowing as wop
from bytewax.operators.windowing import EventClock, TumblingWindower
from bytewax.operators.windowing import SystemClock, SessionWindower

from datetime import datetime, timedelta, timezone
import json

import logging
import re


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_deserialize(data):
        """
        Safely deserialize JSON data, handling various formats."""
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
    
    
    
    for item in ['created_at','updated_at']:
        
        time_min_t = re.sub("T", " ", parsed_data[item])
        time_min_ms = re.sub(r":*Z", "",time_min_t)
        time_ = time.strptime(time_min_ms,"%Y-%m-%d %H:%M:%S")
    
        parsed_data[item] = datetime(year=time_.tm_year, 
                                            month=time_.tm_mon, 
                                            day=time_.tm_mday, 
                                            hour=time_.tm_hour, 
                                            minute=time_.tm_min,
                                            second=time_.tm_sec, 
                                            tzinfo=timezone.utc)
        
    return parsed_data
            
    
flow = Dataflow("rag-pipeline")
input_data = op.input("input", flow, FileSource("data/news_out.jsonl"))
deserialize_data = op.filter_map("deserialize", input_data, safe_deserialize)
transform_data_time = op.map("timeconversion", deserialize_data, parse_time)

map_tuple = op.map(
    "tuple_map",
    transform_data_time,
    lambda reading_data: (str(reading_data["id"]), {"created_at":reading_data['created_at'],
                                                    "updated_at":reading_data['updated_at'],
                                                    "headline": reading_data['headline']} # ,"content": reading_data['content']
                                                    ),
)



event_time_config: EventClock = EventClock(
   ts_getter=lambda e: e['updated_at'], wait_for_system_duration=timedelta(seconds=1)
)
align_to = datetime(2024, 5, 29, 1,  tzinfo=timezone.utc)
clock_config = TumblingWindower(align_to=align_to, length=timedelta(seconds=19))

# Collect the windowed data
window = wop.collect_window(
    "windowed_data", map_tuple, clock=event_time_config, windower=clock_config
)

def find_duplicate_ids_in_window(window):
    """Identify duplicate news ID entries given a specific window"""
    
    # Unpack content of tuple
    id, search_session = window
    window_id, events = search_session

    # Collect duplicate entries
    searches = [event for event in events ]

    
    if len(searches)>1:
        return id, searches
    else:
        return None

calc = op.filter_map("calc_ctr", window.down, find_duplicate_ids_in_window)

op.output("output", calc, StdOutSink())
