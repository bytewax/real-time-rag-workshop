from haystack import component, Document
from typing import Any, Dict, List, Optional, Union
from haystack.dataclasses import ByteStream

import json
from dotenv import load_dotenv
import os

import html
import re
from haystack.dataclasses import ByteStream

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@component
class BezingaNews:
    
    def __init__(self, data):
        self.data = data
    
    @component.output_types(documents=List[Document])
    def run(self, sources: Dict[str, Any]) -> None:
        
     
        
        documents = []
        for news_event in sources:
            content = news_event.get("content")
            
            if len(content) < 20:
                content = news_event["headline"]
            else:
                content = content
                
            print("*****************")
            print(content)
            unescaped = html.unescape(content)
            cleaned = re.sub(r'<.*?>', '', unescaped).strip().replace("\n"," ")
            print("******************")
            print(cleaned)
            
            document = Document(content=cleaned)
            
            documents.append(document)
            
        return documents
    
    def _extract_content(self, source: Union[str, Path, ByteStream]) -> str:
        """
        Extracts content from the given data source.
        :param source: The data source to extract content from.
        :return: The extracted content as a string.
        """
        if isinstance(source, (str, Path)):
            with open(source, 'r', encoding='utf-8') as file:
                return file.read()
        elif isinstance(source, ByteStream):
            return source.data.decode('utf-8')
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")
    
    