"""
Streaming Sink: nba_real_time_to_bronze.

Exports raw real-time events to MinIO (Bronze Layer) for archival.
Files are partitioned by date and time.
"""

from mage_ai.streaming.sinks.base_python import BasePythonSink
from typing import Callable, Dict, List
import boto3
from datetime import datetime
import json

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink

@streaming_sink
class BronzeNbaSink(BasePythonSink):
    """
    S3/MinIO Sink for archiving raw streaming data.
    """
    def init_client(self):
        self.client = boto3.client(
            's3',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            endpoint_url='http://minio:9000',
            region_name='us-east-1'
        )
        self.bucket = 'bronze'
        self.prefix = 'nba/streaming/pbp'
    
    def batch_write(self, messages: List[Dict]):
        if not messages: return
        
        # Agrupamos por timestamp para el archivo
        dt = datetime.now()
        jsonl_content = "\n".join([json.dumps(m) for m in messages])
        
        partition = dt.strftime('year=%Y/month=%m/day=%d')
        file_name = dt.strftime('%H%M%S_%f.jsonl')
        key = f"{self.prefix}/{partition}/{file_name}"

        self.client.put_object(Body=jsonl_content.encode('utf-8'), Bucket=self.bucket, Key=key)