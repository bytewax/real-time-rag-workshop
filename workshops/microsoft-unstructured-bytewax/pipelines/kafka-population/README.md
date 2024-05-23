# Data Sources

## Local Configuration

### Setting up the sources

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

download waxctl - https://bytewax.io/waxctl

```shell
brew tap bytewax/tap
brew install waxctl
```

run sec_filings and news_data_loader during market hours

```shell
waxctl run news_data_loader.py
waxctl run sec_filings.py
```

## Remote Configuration