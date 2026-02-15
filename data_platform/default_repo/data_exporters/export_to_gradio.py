from mage_ai.streaming.sinks.base_python import BasePythonSink
from kafka import KafkaProducer
import json
import os
from typing import List, Dict

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink

@streaming_sink
class RedpandaGoldSink(BasePythonSink):
    def init_client(self):
        self.topic = 'nba_gold_events'
        self.producer = KafkaProducer(
            bootstrap_servers=[os.getenv('REDPANDA_BOOTSTRAP', 'redpanda:9092')],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1
        )

    def batch_write(self, messages: List[Dict]):
            if not messages: 
                print("No hay mensajes para escribir.")
                return
            
            print(f"DEBUG: Intentando escribir {len(messages)} mensajes en Redpanda...")
            
            for msg in messages:
                try:
                    self.producer.send(self.topic, value=msg)
                except Exception as e:
                    print(f"ERROR enviando mensaje: {e}")
            
            self.producer.flush()
            print("DEBUG: Flush completado con éxito.")

    def close(self):
        if hasattr(self, 'producer'): self.producer.close()