
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

from haystack.components.embedders import OpenAITextEmbedder
from haystack.utils import Secret
from haystack.components.retrievers.in_memory import InMemoryEmbeddingRetriever
from haystack.components.builders import PromptBuilder
from haystack.components.generators import OpenAIGenerator

from haystack import component, Document
from typing import Any, Dict, List, Optional, Union
from haystack.dataclasses import ByteStream
from dotenv import load_dotenv
import os
import json 

load_dotenv("../.env")
api_key = os.environ.get("news_api")
open_ai_key = os.environ.get("OPENAI_API_KEY")
unstructured = os.environ.get("UNSTRUCTURED")


class JSONLReader():
    def __init__(self, metadata_fields=None, link_keyword='url'):
        """
        Initialize the JSONLReader with optional metadata fields and a link keyword.
        
        :param metadata_fields: List of fields in the JSONL to retain as metadata.
        :param link_keyword: The keyword to use to extract the URL from the JSONL.
        """
        self.metadata_fields = metadata_fields or []
        self.link_keyword = link_keyword
        
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

        # Initialize pipeline
        self.pipeline = Pipeline()

        # Add components
        self.pipeline.add_component("fetcher", fetcher)
        self.pipeline.add_component("converter", converter)
        self.pipeline.add_component("cleaner", document_cleaner)

        # Connect components
        self.pipeline.connect("fetcher", "converter")
        self.pipeline.connect("converter", "cleaner")

    @component.output_types(documents=List[Document])
    def run(self, sources: List[Union[str, Path, ByteStream]]):
        """
        Process each source file, read URLs and their associated metadata,
        fetch HTML content using a pipeline, and convert to Haystack Documents.
        :param sources: File paths or ByteStreams to process.
        :return: A list of Haystack Documents.
        """
        documents = []
        for source in sources:
            file_content = self._extract_content(source)
            for line in file_content.strip().split('\n'):
                if line.strip():
                    data = json.loads(line)
                    
                    # Handle both direct dictionaries and lists with [null, {dict}] format
                    if isinstance(data, list) and len(data) == 2 and isinstance(data[1], dict):
                        data = data[1]  # Use the dictionary from the list
                    elif not isinstance(data, dict):
                        print(f"Unexpected format or missing data in line: {data}")
                        continue  # Skip lines that do not match expected format
                    
                    # Extract URL and modify it if necessary
                    url = data.get(self.link_keyword)
                    if url and '-index.html' in url:
                        url = url.replace('-index.html', '.txt')

                    else:
                        metadata = {field: data.get(field) for field in self.metadata_fields if field in data}
                        # Assume a pipeline fetches and processes this URL
                        doc = self.pipeline.run({"fetcher": {"urls": [url]}})
                        document = doc['cleaner']['documents'][0].content

                        # Create a document with fetched content and extracted metadata
                        documents.append(Document(content=document, meta=metadata))

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

def build_indexing_pipeline(document_store):

    document_splitter = DocumentSplitter(split_by="passage")
                                                                    
    document_embedder = OpenAIDocumentEmbedder(api_key=Secret.from_token(open_ai_key))

    document_writer = DocumentWriter(document_store=document_store)

    indexing_pipeline = Pipeline() 
    indexing_pipeline.add_component("splitter", document_splitter)   
    indexing_pipeline.add_component("embedder",document_embedder )
    indexing_pipeline.add_component("writer", document_writer)

    indexing_pipeline.connect('splitter','embedder')
    indexing_pipeline.connect("embedder", "writer")

    return indexing_pipeline



def build_retriever_pipeline(document_store, open_ai_key):
    """
    Create a pipeline for retrieving documents from the document store.
    
    :param document_store: DocumentStore to read the documents from.
    :param open_ai_key: OpenAI API key.
    
    :return: Pipeline for retrieving documents.
    """

    text_embedder = OpenAITextEmbedder(api_key = Secret.from_token(open_ai_key))
    retriever = InMemoryEmbeddingRetriever(document_store)
    generator = OpenAIGenerator(api_key = Secret.from_token(open_ai_key), 
        model="gpt-3.5-turbo")

    template = """
    Your task is to generate a comprehensive report using the context provided, 
    answering the question below.

    Context:
    {% for document in documents %}
        {{ document.content }} symbols: {{ doc.meta['symbols'] }}
    {% endfor %}

    Question: {{question}}
    Answer:
    """

    prompt_builder = PromptBuilder(template=template)

    # Initialize pipeline
    retriever_pipeline = Pipeline()

    # Add components
    retriever_pipeline.add_component("text_embedder", text_embedder)
    retriever_pipeline.add_component("retriever", retriever)
    retriever_pipeline.add_component("prompt_builder", prompt_builder)
    retriever_pipeline.add_component("llm", generator)

    # Connect components to one another
    retriever_pipeline.connect("text_embedder.embedding", "retriever.query_embedding")
    retriever_pipeline.connect("retriever", "prompt_builder.documents")
    retriever_pipeline.connect("prompt_builder", "llm")

    return retriever_pipeline