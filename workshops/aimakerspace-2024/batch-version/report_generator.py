from dotenv import load_dotenv
import os
from rag_pipelines import JSONLReader, build_retriever_pipeline, build_indexing_pipeline
from haystack.document_stores.in_memory import InMemoryDocumentStore

if __name__ == "__main__":
    load_dotenv(".env")
    open_ai_key = os.environ.get("OPENAI_API_KEY")

    # Data extraction
    # Data indexing
    converter = JSONLReader(
        metadata_fields=["cik", "form_type", "link", "url", "headline", "symbols"],
        link_keyword="url",
    )
    documents = converter.run(sources=["./data/news_out.jsonl"])

    # Data indexing
    document_store = InMemoryDocumentStore(embedding_similarity_function="cosine")
    indexing_pipeline = build_indexing_pipeline(document_store)

    indexing_pipeline.run({"splitter": {"documents": documents}})

    # Retriever pipeline
    retriever = build_retriever_pipeline(document_store, open_ai_key)
    question = "What can you tell me about the information you have"
    response = retriever.run(
        {"text_embedder": {"text": question}, "prompt_builder": {"question": question}}
    )
