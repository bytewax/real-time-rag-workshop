from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.testing import run_main
from bytewax.connectors.kafka import KafkaSource
from rag_custom_pipeline import safe_deserialize, JSONLReader

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

jsonl_reader = JSONLReader(metadata_fields=['title', 'form_type', 'url'])

def process_event(event):
    """Wrapper to handle the processing of each event."""
    if event:
        dict_document = jsonl_reader.run(event)
        return dict_document
    return None

flow = Dataflow("rag-pipeline")
edgar_k_input = op.input("input", flow, KafkaSource())
news_input = op.input("input", flow, KafkaSource())

edgar_deser = op.map("deserialize", edgar_k_input, safe_deserialize)
edgar_dicts = op.map("extract_html", edgar_deser, process_event)

news_deser = op.map("deserialize", news_input, safe_deserialize)
news_dicts = op.map("extract_html", news_deser, process_event)

merged_stream = op.merge("merge", news_dicts, edgar_dicts)
op.output("", merged_stream, PopulateAzureAISearch())