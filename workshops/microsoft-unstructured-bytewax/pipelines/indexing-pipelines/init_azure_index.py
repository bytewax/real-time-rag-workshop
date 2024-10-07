from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient

from azure.search.documents.indexes.models import (
    AzureOpenAIVectorizer,
    AzureOpenAIVectorizerParameters,
    ExhaustiveKnnAlgorithmConfiguration,
    ExhaustiveKnnParameters,
    HnswAlgorithmConfiguration,
    HnswParameters,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SemanticConfiguration,
    SemanticField,
    SemanticSearch,
    SemanticPrioritizedFields,
    VectorSearch,
    VectorSearchAlgorithmMetric,
    VectorSearchProfile,
)

import os
from dotenv import load_dotenv

load_dotenv(override=True)

endpoint = os.environ["AZURE_SEARCH_SERVICE_ENDPOINT"]
api_key = os.environ["AZURE_SEARCH_ADMIN_KEY"]
DIMENSIONS = 1536
AZURE_OPENAI_KEY = os.environ["AZURE_OPENAI_API_KEY"]
AZURE_OPENAI_SERVICE = os.environ["AZURE_OPENAI_SERVICE"]
AZURE_OPENAI_ENDPOINT = os.environ["AZURE_OPENAI_ENDPOINT"]

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    api_version="2023-10-01-preview",
    azure_endpoint=f"https://{AZURE_OPENAI_SERVICE}.openai.azure.com/",
)
key = os.environ["AZURE_SEARCH_ADMIN_KEY"]
credential = AzureKeyCredential(key)

index_client = SearchIndexClient(endpoint, AzureKeyCredential(key))
search_indexer_client = SearchIndexerClient(
    endpoint=os.environ["AZURE_SEARCH_SERVICE_ENDPOINT"],
    credential=AzureKeyCredential(os.environ["AZURE_SEARCH_ADMIN_KEY"]),
)


fields = [
    # SearchField(name="chunk_id", type=SearchFieldDataType.String, searchable= True,filterable=True,retrievable=True,sortable=True,facetable=True,key=True,analyzer_name="keyword"),
    SearchField(
        name="id",
        type=SearchFieldDataType.String,
        searchable=True,
        filterable=True,
        sortable=True,
        facetable=True,
        key=True,
    ),
    SearchField(
        name="content",
        type=SearchFieldDataType.String,
        searchable=True,
        filterable=False,
        sortable=False,
        facetable=False,
        key=False,
    ),
    SearchField(
        name="meta",
        type=SearchFieldDataType.String,
        searchable=True,
        filterable=False,
        sortable=False,
        facetable=False,
        key=False,
    ),
    SearchField(
        name="vector",
        type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
        searchable=True,
        filterable=False,
        sortable=False,
        vector_search_dimensions=1536,
        vector_search_profile_name="myHnswProfile",
    ),
]

# Configure the vector search configuration
vector_search = VectorSearch(
    algorithms=[
        HnswAlgorithmConfiguration(
            name="myHnsw",
            parameters=HnswParameters(
                m=4,
                ef_construction=400,
                ef_search=500,
                metric=VectorSearchAlgorithmMetric.COSINE,
            ),
        ),
        ExhaustiveKnnAlgorithmConfiguration(
            name="myExhaustiveKnn",
            parameters=ExhaustiveKnnParameters(
                metric=VectorSearchAlgorithmMetric.COSINE,
            ),
        ),
    ],
    profiles=[
        VectorSearchProfile(
            name="myHnswProfile",
            algorithm_configuration_name="myHnsw",
            vectorizer_name="myOpenAI",
        ),
        VectorSearchProfile(
            name="myExhaustiveKnnProfile",
            algorithm_configuration_name="myExhaustiveKnn",
            vectorizer_name="myOpenAI",
        ),
    ],
    vectorizers=[
        AzureOpenAIVectorizer(
            vectorizer_name="myOpenAI",
            kind="azureOpenAI",
            parameters=AzureOpenAIVectorizerParameters(
                resource_url=os.environ["AZURE_OPENAI_ENDPOINT"],
                deployment_name="bytewax-workshop-ada",
                api_key=os.environ["AZURE_OPENAI_API_KEY"],
                model_name="text-embedding-ada-002",
            ),
        ),
    ],
)

semantic_config = SemanticConfiguration(
    name="my-semantic-config",
    prioritized_fields=SemanticPrioritizedFields(
        title_field=SemanticField(field_name="meta"),
        # prioritized_keywords_fields=[SemanticField(field_name="Category")],
        # prioritized_content_fields=[SemanticField(field_name="chunk")]
    ),
)

# Create the semantic settings with the configuration
semantic_search = SemanticSearch(configurations=[semantic_config])

# Create the search index with the semantic settings
index = SearchIndex(
    name="bytewax-index",
    fields=fields,
    vector_search=vector_search,
    semantic_search=semantic_search,
)
result = index_client.create_or_update_index(index)
# print(f"{result.name} created")

print("Creating bytewax-index search index")
