from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential  
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient  

from azure.search.documents.indexes.models import (  
    AzureOpenAIParameters,  
    AzureOpenAIVectorizer,  
    ExhaustiveKnnParameters,  
    ExhaustiveKnnVectorSearchAlgorithmConfiguration,
    HnswParameters,  
    HnswVectorSearchAlgorithmConfiguration,  
    PrioritizedFields,    
    SearchField,  
    SearchFieldDataType,  
    SearchIndex,  
    SemanticConfiguration,  
    SemanticField,  
    SemanticSettings,  
    VectorSearch,  
    VectorSearchAlgorithmKind,  
    VectorSearchAlgorithmMetric,  
    VectorSearchProfile,  
)  

import os
from dotenv import load_dotenv  
load_dotenv(override=True)

endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
service_name = os.getenv("AZURE_SEARCH_SERVICE")
api_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
DIMENSIONS = 1536
AZURE_OPENAI_KEY = os.getenv('AZURE_OPENAI_API_KEY')
AZURE_OPENAI_SERVICE = os.getenv('AZURE_OPENAI_SERVICE')
AZURE_OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,  
    api_version="2023-10-01-preview",
    azure_endpoint = f"https://{AZURE_OPENAI_SERVICE}.openai.azure.com/"
)
key = os.getenv("AZURE_SEARCH_ADMIN_KEY")  
credential = AzureKeyCredential(key)

index_client = SearchIndexClient(endpoint, AzureKeyCredential(key))
search_indexer_client = SearchIndexerClient(endpoint=os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT"),  
                                            credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY")))


fields = [  
    # SearchField(name="chunk_id", type=SearchFieldDataType.String, searchable= True,filterable=True,retrievable=True,sortable=True,facetable=True,key=True,analyzer_name="keyword"),  
    SearchField(name="id",type=SearchFieldDataType.String, searchable= True,filterable=True,retrievable=True,sortable=True,facetable=True,key=True), 
    SearchField(name="content", type=SearchFieldDataType.String, searchable= True,filterable=False,retrievable=True,sortable=False,facetable=False,key=False),  
    SearchField(name="meta", type=SearchFieldDataType.String,searchable= True,filterable=False,retrievable=True,sortable=False,facetable=False,key=False),    
    SearchField(name="vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single), searchable=True,filterable=False,retrievable=True,sortable=False,vector_search_dimensions=1536, vector_search_profile="myHnswProfile")
] 


# Configure the vector search configuration  
vector_search = VectorSearch(  
    algorithms=[  
        HnswVectorSearchAlgorithmConfiguration(  
            name="myHnsw",  
            kind=VectorSearchAlgorithmKind.HNSW,  
            parameters=HnswParameters(  
                m=4,  
                ef_construction=400,  
                ef_search=500,  
                metric=VectorSearchAlgorithmMetric.COSINE,  
            ),  
        ),  
        ExhaustiveKnnVectorSearchAlgorithmConfiguration(  
            name="myExhaustiveKnn",  
            kind=VectorSearchAlgorithmKind.EXHAUSTIVE_KNN,  
            parameters=ExhaustiveKnnParameters(  
                metric=VectorSearchAlgorithmMetric.COSINE,  
            ),  
        ),  
    ],  
    profiles=[  
        VectorSearchProfile(  
            name="myHnswProfile",  
            algorithm="myHnsw",
            vectorizer="myOpenAI",
        ),  
        VectorSearchProfile(  
            name="myExhaustiveKnnProfile",  
            algorithm="myExhaustiveKnn",
            vectorizer="myOpenAI",  
        ),  
    ],
    vectorizers=[  
        AzureOpenAIVectorizer(  
            name="myOpenAI",  
            kind="azureOpenAI",  
            azure_open_ai_parameters=AzureOpenAIParameters(  
                resource_uri=os.getenv("AZURE_OPENAI_ENDPOINT"),  
                deployment_id="bytewax-workshop-ada",  
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),  
            ),  
        ),  
    ],   
)  
  
semantic_config = SemanticConfiguration(
    name="my-semantic-config",
    prioritized_fields=PrioritizedFields(
        title_field=SemanticField(field_name="meta"),
        # prioritized_keywords_fields=[SemanticField(field_name="Category")],
        # prioritized_content_fields=[SemanticField(field_name="chunk")]
    )
)

# Create the semantic settings with the configuration  
semantic_settings = SemanticSettings(configurations=[semantic_config])

# Create the search index with the semantic settings  
index = SearchIndex(name="bytewax-index", fields=fields, vector_search=vector_search
                    , semantic_settings=semantic_settings)  
result = index_client.create_or_update_index(index)  
# print(f"{result.name} created")

print(f"Creating bytewax-index search index")