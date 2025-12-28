"""Base extractor module for ETL processes."""

import logging
from abc import ABC, abstractmethod
from typing import Any

from pymongo import MongoClient


class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.

    Provides common functionality for MongoDB connections, checkpointing,
    and logging. All concrete extractors should inherit from this class.

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI
    db_name : str
        Database name
    collection_name : str
        Collection name for storing extracted data
    checkpoint_collection : str, optional
        Collection name for storing ETL checkpoints (default: "etl_checkpoints")
    client : pymongo.MongoClient, optional
        Existing MongoDB client instance
    """

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str,
        collection_name: str,
        checkpoint_collection: str = "etl_checkpoints",
        client: Any = None,
    ):
        self.mongodb_uri = mongodb_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.checkpoint_collection_name = checkpoint_collection

        if client:
            self.client = client
        else:
            self.client = MongoClient(self.mongodb_uri)

        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self.checkpoints = self.db[self.checkpoint_collection_name]

        self.logger = logging.getLogger(f"airflow.task.{self.__class__.__name__}")

    def get_checkpoint(self, source_id: str) -> Any | None:
        """
        Retrieve the last checkpoint for a given source.

        Parameters
        ----------
        source_id : str
            Unique identifier for the data source

        Returns
        -------
        Any or None
            Last checkpoint value if exists, None otherwise
        """
        checkpoint = self.checkpoints.find_one({"_id": source_id})
        return checkpoint.get("last_value") if checkpoint else None

    def save_checkpoint(self, source_id: str, value: Any) -> None:
        """
        Save checkpoint for a given source.

        Parameters
        ----------
        source_id : str
            Unique identifier for the data source
        value : Any
            Checkpoint value to store
        """
        self.checkpoints.update_one(
            {"_id": source_id}, {"$set": {"last_value": value}}, upsert=True
        )

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """
        Main execution method to be implemented by subclasses.

        Parameters
        ----------
        *args : Any
            Positional arguments
        **kwargs : Any
            Keyword arguments

        Returns
        -------
        Any
            Extraction result
        """
        pass

    def close(self) -> None:
        """Close MongoDB connection."""
        self.client.close()
