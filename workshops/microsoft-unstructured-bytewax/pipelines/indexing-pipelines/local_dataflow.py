from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from custom_connectors import SimulationSource, AzureSearchSink
from rag_custom_pipeline import safe_deserialize, JSONLReader



jsonl_reader = JSONLReader(metadata_fields=['title', \
                                            'form_type', \
                                            'symbol',
                                            'url'])


def process_event(event):
    """Wrapper to handle the processing of each event."""
    if event:
        dict_document = jsonl_reader.run(event)
        return dict_document
    return None


flow = Dataflow("rag-pipeline")
input_data = op.input("input", flow, SimulationSource("data/test.jsonl", batch_size=1))
deserialize_data = op.map("deserialize", input_data, safe_deserialize)
extract_html = op.filter_map("build_indeces", deserialize_data, process_event)
op.output("output", extract_html, StdOutSink())

