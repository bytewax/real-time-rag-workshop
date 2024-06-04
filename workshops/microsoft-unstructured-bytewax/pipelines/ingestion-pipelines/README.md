# Data Ingestion Pipelines

There are two data ingestion pipelines. One ingests data from Alpaca news websocket which has a Benzinga news feed showing news in real-time. The second is a custom built news source reading from SEC EDGAR for new filings as they are submitted to the SEC.

There are two ways to run these pipelines. In production the recommended way is to use Kafka/Redpanda, but for local testing and the purposes of the workshop, we use a local json lines file to remove the dependency on Kafka.

## Pre requisites

Install Python and requirements

```shell
pip install -r requirements.txt
```

download waxctl - https://bytewax.io/waxctl

```shell
brew tap bytewax/tap
brew install waxctl
```

## Local Configuration Kafka Version

### Setting up the Kafka

Run kafka or redpanda, we use redpanda in this demo. 

```shell
docker compose up -d
```

create 2 topics - news and filings with 3 partitions each

```shell
docker exec -it redpanda-0 rpk topic create news filings -p3
```

set env variables
_You will need an alpaca account and access to the API_

```
export BROKER_ADDRESS=0.0.0.0:19092
export NEWS_TOPIC=news
export FILINGS_TOPIC=filings
export ALPACA_API_KEY=*********
export ALPACA_SECRET=********
```

run sec_filings and news_data_loader during market hours

```shell
waxctl run news_data_loader.py
waxctl run sec_filings.py
```

## Local Configuration No Kafka

The local configuration will write the filings and news to a json lines file.

```shell
export ALPACA_API_KEY=*********
export ALPACA_SECRET=********
```

Run the pipelines with waxctl

```shell
waxctl run news_data_loader.py
waxctl run sec_filings.py
```

Deploying the pipelines remotely

One method to deploy these pipelines remotely is to use the Bytewax platform, which has a management dashboard and other features for resiliency and reduced operational burden. You can also deploy them to AWS or GCP with waxctl.

```shell
waxctl aws deploy --help
```