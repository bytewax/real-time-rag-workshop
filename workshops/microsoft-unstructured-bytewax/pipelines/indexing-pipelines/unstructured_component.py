import re
from typing import Any, Dict, List, Union
from pathlib import Path
import requests
import time
import uuid
from dotenv import load_dotenv
from haystack import Document, component
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from unstructured_client.models.errors import SDKError
from unstructured.staging.base import dict_to_elements
# Setup logging
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv(".env")

@component
class UnstructuredParser:
    """
    A component generating personal welcome message and making it upper case
    """
    def __init__(self, unstructured_key: str, chunking_strategy, strategy, model):
        """
        Initialize the UnstructuredParser with an API key.
        :param unstructured_key: The API key for the Unstructured API.
        :param chunking_strategy: The chunking strategy to use. https://docs.unstructured.io/api-reference/api-services/chunking
        :param strategy: The strategy to use. https://docs.unstructured.io/api-reference/api-services/partitioning
        :param model: The model to use. 
        """
        self.unstructured_key = unstructured_key
        self.chunking_strategy = chunking_strategy
        self.strategy = strategy
        self.model = model

    @component.output_types(documents=List[Document])
    def run(self, sources: List[Union[str, Path]]):
        """
        Process each source file, read URLs and their associated metadata,
        fetch any unstructured content using a pipeline, and convert to Haystack Documents.
        :param sources: File paths or URLs to process.
        :return: A list of Haystack Documents.
        """
        
        regex_pattern = (
        r'<.*?>'  # HTML tags
        r'|\t'  # Tabs
        r'|\n+'  # Newlines
        r'|&nbsp;'  # Non-breaking spaces
        r'|[^a-zA-Z0-9\s-]'  # Any non-alphanumeric character (excluding whitespace)
        )
        
        client = UnstructuredClient(api_key_auth=self.unstructured_key)
        documents = []
        all_symbols = set()
        for source in sources:
            file_content = self.download_file(source)
            if file_content:  # Check if download was successful
                req = shared.PartitionParameters(
                    files=shared.Files(
                        content=file_content,
                        file_name=str(source),
                    ),
                    strategy=self.strategy,
                    hi_res_model_name=self.model,
                    chunking_strategy=self.chunking_strategy,
                )
                try:
                    resp = client.general.partition(req)
                    elements = dict_to_elements(resp.elements)
                    for item in elements:
                        doc_id = str(uuid.uuid4())
                        metadata = item.metadata.to_dict()
                        
                        # Extract CLASS-CONTRACT-TICKER-SYMBOL if present
                        symbol_matches = re.findall(r'<CLASS-CONTRACT-TICKER-SYMBOL>(\S+)', item.text)
                        if symbol_matches:
                            symbols = ','.join(symbol_matches)
                            all_symbols.update(symbol_matches)  # Add to all symbols set
                            metadata['symbol'] = symbols


                        cleaned_text = re.sub(regex_pattern, '', item.text)
                        if cleaned_text=="":  # Skip empty documents
                            continue

                        metadata.pop('orig_elements', None)
                        metadata['source_url'] = str(source)
                        documents.append(Document(content=item.text, id=doc_id, meta=metadata))

            
                except SDKError as e:
                    print(e)

        # Ensure all documents have the same symbols metadata if any were found
        if all_symbols:
            symbols_str = ','.join(all_symbols)
            for document in documents:
                document.meta['symbol'] = symbols_str

        return {"documents": documents}

    # Helper function to download file from URL
    def download_file(self, source: Union[str, Path]) -> bytes:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        try:
            response = requests.get(source, headers=headers)
            
            if response.status_code == 200:
                print("Download succeeded")
                return response.content
            else:
                print(f"Download failed with status code: {response.status_code}")
        except Exception as e:
            print("Download failed:", e)
        return None

# How to initialize the component
# unstructured_api_key = os.environ.get("UNSTRUCTURED_API_KEY")
# unstructured_parser = UnstructuredParser(unstructured_key=unstructured_api_key,
#                                           chunking_strategy="by_title",
#                                           strategy="hi_res",
#                                           model="yolox")

# Sample usage
# result = unstructured_parser.run(sources=["https://www.sec.gov/Archives/edgar/data/908695/000175272424095244/0001752724-24-095244.txt",
#                                           "https://www.sec.gov/Archives/edgar/data/1518042/000158064224002565/0001580642-24-002565.txt"
#                                           ,
#                                           ])

# https://www.sec.gov/Archives/edgar/data/1979372/000197937224000002/0001979372-24-000002-index.htm
# https://www.sec.gov/Archives/edgar/data/1665650/000121390024040074/0001213900-24-040074-index.htm
# https://www.sec.gov/Archives/edgar/data/1979372/000197937224000002/0001979372-24-000002-index.htm