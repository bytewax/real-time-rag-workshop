## Executing the Indexing dataflow

The indexing dataflow is responsible for ingesting and transforming raw data from the data source, transforming it into a format that can be used by the model, and storing it in the data store. 

## Prerequisites and set up 

Create a virtual environment and install the required packages in this repository:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

To ensure the indeces can be built in Azure, please ensure you have set up the following resources:

1. Azure AI Search: Follow the instructions [here](https://learn.microsoft.com/en-us/azure/search/search-create-service-portal) to create an Azure AI Search service.
2. Azure OpenAI: Set up the Azure OpenAI service and deploy the models 'gpt4 (0613)' and 'text-ada-002-embedding' by following the instructions [here](https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/create-resource?pivots=web-portal).

Ensure you obtain and save the credentials. The scripts assume the following variable names:

```bash
# OpenAI
AZURE_OPENAI_ENDPOINT=
AZURE_OPENAI_API_KEY=
AZURE_OPENAI_API_URI = 
AZURE_OPENAI_API_VERSION = "2023-07-01-preview"
AZURE_OPENAI_SERVICE =
AZURE_OPENAI_EMBEDDING_SERVICE=
 
# Azure Cognitive Search
AZURE_SEARCH_ADMIN_KEY =
AZURE_SEARCH_SERVICE =
AZURE_SEARCH_SERVICE_ENDPOINT =
```

Ensure you obtain an API key from Unstructured by signing up on [their platform](https://unstructured.io/api-key-free).

Follow [API documentation](https://docs.unstructured.io/api-reference/api-services/overview) to set up your connectors and ingest your documents to your destination.

```bash
# Unstructured
UNSTRUCTURED_API_KEY=
```

## Running the dataflow

Ensure you can access all services you have set up from the Azure portal. Once this is complete, initialize the AI Search service by running:

```bash
python init_azure_index.py
```

Confirm the index has been created on the Azure portal. 

From there you can run the dataflow by running:

```bash
cd pipelines/indexing-pipelines
python -m bytewax.run local_dataflow:flow
```
