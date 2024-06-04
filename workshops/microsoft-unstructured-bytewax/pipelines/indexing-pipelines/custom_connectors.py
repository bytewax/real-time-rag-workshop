"""Connectors for local text files with delay."""
from pathlib import Path
from typing import Callable, List, Dict, Any, Optional, Union
from datetime import datetime, timedelta, timezone
import json
import os

import requests
from typing_extensions import override
from bytewax.connectors.files import FileSource, _FileSourcePartition
from bytewax.outputs import StatelessSinkPartition, DynamicSink
from dotenv import load_dotenv
load_dotenv(".env")
search_api_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")

from bytewax import inputs

def _get_path_dev(path: Path) -> str:
    return hex(path.stat().st_dev)

class _SimulationSourcePartition(_FileSourcePartition):
    def __init__(self, path: Path, batch_size: int, resume_state: Optional[int], delay: timedelta):
        super().__init__(path, batch_size, resume_state)
        self._delay = delay
        self._next_awake = datetime.now(timezone.utc)

    @override
    def next_batch(self) -> List[str]:
        if self._delay:
            self._next_awake += self._delay
        return super().next_batch()

    @override
    def next_awake(self) -> Optional[datetime]:
        return self._next_awake

class SimulationSource(FileSource):
    """Read a path line-by-line from the filesystem with a delay between batches."""

    def __init__(
        self,
        path: Union[Path, str],
        batch_size: int = 10,
        delay: timedelta = timedelta(seconds=5),
        get_fs_id: Callable[[Path], str] = _get_path_dev,
    ):
        """Init.

        :arg path: Path to file.

        :arg batch_size: Number of lines to read per batch. Defaults
            to 10.

        :arg delay: Delay between batches. Defaults to 1 second.

        :arg get_fs_id: Called with the parent directory and must
            return a consistent (across workers and restarts) unique
            ID for the filesystem of that directory. Defaults to using
            {py:obj}`os.stat_result.st_dev`.

            If you know all workers have access to identical files,
            you can have this return a constant: `lambda _dir:
            "SHARED"`.

        """
        super().__init__(path, batch_size, get_fs_id)
        self._delay = delay

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _SimulationSourcePartition:
        _fs_id, path = for_part.split("::", 1)
        assert path == str(self._path), "Can't resume reading from different file"
        return _SimulationSourcePartition(self._path, self._batch_size, resume_state, self._delay)

class _AzureSearchPartition(StatelessSinkPartition[Any]):
    @override
    def write_batch(self, dictionary) -> None:
        index_name = "bytewax-index"
        search_api_version = '2023-11-01'
        search_endpoint = f'https://bytewax-workshop.search.windows.net/indexes/{index_name}/docs/index?api-version={search_api_version}'  
        headers = {  
            'Content-Type': 'application/json',  
            'api-key': search_api_key  
        }  

        dictionary = dictionary[0]
        # Use the flattened meta directly
        flattened_meta = dictionary['meta']
        
        # Convert DataFrame to the format expected by Azure Search  
        body = json.dumps({  
            "value": [  
                {  
                    "@search.action": "upload",  
                    "id": dictionary['id'],  
                    "content": dictionary['content'],  
                    "meta": flattened_meta,  # Use flattened meta
                    "vector": dictionary['vector']  # Include the generated embeddings  
                } 
            ]  
        })  
        
        # Upload documents to Azure Search  
        response = requests.post(search_endpoint, 
                                 headers=headers, data=body) 

        return {"status": "success" if response.status_code == 200 else response.text}

class AzureSearchSink(DynamicSink[Any]):
    """Write each output item to Azure Search
    """

    @override
    def build(
        self, _step_id: str, _worker_index: int, _worker_count: int
    ) -> _AzureSearchPartition:
        return _AzureSearchPartition()

## Usage Example
# from simulated_connector import SimulationSource
# flow = Dataflow("simulate")
# inp = op.input("sim_inp", flow, SimulationSource("sec_out.jsonl"))
# op.inspect("inp", inp)