"""
Streaming/real-time data sources.

Owner: Data Engineering Team
For Kafka, message queues, and streaming APIs.
"""

from typing import Iterator
import pandas as pd

from dmviz.core.registry import Registry
from dmviz.ingestion.base import BaseIngester


@Registry.register("kafka_consumer", category="ingestion")
class KafkaConsumer(BaseIngester):
    """
    Consume from Kafka topics.
    
    Config: bootstrap_servers, topic, group_id, auto_offset_reset
    INPUT:  dict (Kafka config)
    OUTPUT: Iterator[pd.DataFrame] (streaming)
    """
    
    name = "kafka_consumer"
    
    def _run(self, source) -> pd.DataFrame:
        raise NotImplementedError("TODO: Implement Kafka consumer")
    
    def stream(self, source) -> Iterator[pd.DataFrame]:
        """Yield DataFrames from Kafka topic continuously."""
        raise NotImplementedError("TODO: Implement Kafka streaming")
