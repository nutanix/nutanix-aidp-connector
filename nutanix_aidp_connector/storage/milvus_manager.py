# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Milvus Manager for collection initialization and management
"""

import logging
from typing import Optional, Dict, Any
from urllib.parse import urlparse

try:
    from pymilvus import (
        connections,
        utility,
        Collection,
        FieldSchema,
        CollectionSchema,
        DataType,
        Index,
        Function,
        FunctionType,
        MilvusClient,
        IndexParams,
    )
    MILVUS_CLIENT_AVAILABLE = True
except ImportError as e:
    # Fallback for older pymilvus versions
    from pymilvus import (
        connections,
        utility,
        Collection,
        FieldSchema,
        CollectionSchema,
        DataType,
        Index,
    )
    MILVUS_CLIENT_AVAILABLE = False
    MilvusClient = None
    Function = None
    FunctionType = None
    IndexParams = None
    logger = logging.getLogger(__name__)
    logger.warning(f"Some pymilvus features not available: {e}. Using fallback mode.")

logger = logging.getLogger(__name__)

# Constants matching NeMo-Retriever
CONSISTENCY = "Bounded"  # NeMo-Retriever uses CONSISTENCY_BOUNDED

# Export truncate_text for use in other modules
__all__ = ['MilvusManager', 'truncate_text']


def truncate_text(text: str, max_length: int = 65535) -> str:
    """
    Truncate text to fit within VARCHAR field max_length limit.
    
    Args:
        text: Text to truncate
        max_length: Maximum length (default: 65535 for Milvus VARCHAR)
    
    Returns:
        Truncated text
    """
    if text is None:
        return ""
    if len(text) <= max_length:
        return text
    
    logger.warning(f"Truncating text from {len(text)} to {max_length} characters")
    # Truncate and add indicator
    truncated = text[:max_length - 3] + "..."
    return truncated


class MilvusManager:
    """Manages Milvus connections and collections"""
    
    def __init__(self, host: str, port: int, collection_name: str,
                 index_type: str = "GPU_CAGRA", metric_type: str = "L2",
                 nlist: int = 1024, nprobe: int = 10,
                 auto_id: bool = True, consistency_level: str = "Strong",
                 dimension: int = 1536,
                 sparse: bool = False,
                 gpu_index: bool = True,
                 gpu_search: bool = True,
                 local_index: bool = False):
        """
        Initialize Milvus manager
        
        Args:
            host: Milvus host
            port: Milvus port
            collection_name: Collection name
            index_type: Index type (GPU_CAGRA, IVF_FLAT, IVF_SQ8, etc.)
            metric_type: Distance metric (L2, IP, etc.)
            nlist: Number of clusters (for IVF indexes)
            nprobe: Number of clusters to search
            auto_id: Auto-generate IDs
            consistency_level: Consistency level
            dimension: Vector dimension (default: 1536, typically 2048 for embedqa-1b-v2)
            sparse: Enable sparse vector field and BM25 function (default: False)
            gpu_index: Use GPU_CAGRA index for dense vectors (default: True)
            gpu_search: Enable GPU-accelerated search (default: True)
            local_index: Use local index (FLAT) instead of server-side index (default: False)
        """
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.index_type = index_type
        self.metric_type = metric_type
        self.nlist = nlist
        self.nprobe = nprobe
        self.auto_id = auto_id
        self.consistency_level = consistency_level
        self.dimension = dimension
        self.sparse = sparse
        self.gpu_index = gpu_index
        self.gpu_search = gpu_search
        self.local_index = local_index
        self.connection_alias = "default"
        self.collection: Optional[Collection] = None
        
        logger.info(f"Initializing Milvus manager: {host}:{port}, collection: {collection_name}, "
                   f"sparse={sparse}, gpu_index={gpu_index}, gpu_search={gpu_search}")
    
    def connect(self) -> bool:
        """
        Connect to Milvus
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Disconnect existing connection if any
            try:
                connections.disconnect(self.connection_alias)
            except Exception:
                pass
            
            # Connect to Milvus
            connections.connect(
                alias=self.connection_alias,
                host=self.host,
                port=self.port
            )
            
            logger.info(f"Successfully connected to Milvus at {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Milvus: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from Milvus"""
        try:
            connections.disconnect(self.connection_alias)
            logger.info("Disconnected from Milvus")
        except Exception as e:
            logger.warning(f"Error disconnecting from Milvus: {e}")
    
    def collection_exists(self) -> bool:
        """
        Check if collection exists
        
        Returns:
            True if collection exists, False otherwise
        """
        try:
            if not connections.has_connection(self.connection_alias):
                if not self.connect():
                    return False
            
            exists = utility.has_collection(self.collection_name, using=self.connection_alias)
            logger.debug(f"Collection '{self.collection_name}' exists: {exists}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking if collection exists: {e}")
            return False
    
    def _create_nemo_retriever_schema(self) -> CollectionSchema:
        """
        Create NeMo-Retriever compatible schema matching create_nemo_retriever_schema()
        
        Returns:
            CollectionSchema matching NeMo-Retriever format
        """
        if MILVUS_CLIENT_AVAILABLE and MilvusClient is not None:
            # Use MilvusClient API (preferred, matches NeMo-Retriever exactly)
            try:
                schema = MilvusClient.create_schema(auto_id=True, enable_dynamic_field=True)
                schema.add_field(field_name="pk", datatype=DataType.INT64, is_primary=True, auto_id=True)
                schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=self.dimension)
                schema.add_field(field_name="source", datatype=DataType.JSON)
                schema.add_field(
                    field_name="content_metadata",
                    datatype=DataType.JSON,
                    nullable=True if not self.local_index else False,
                )
                
                if self.sparse and self.local_index:
                    # Local index: simple sparse field
                    schema.add_field(field_name="sparse", datatype=DataType.SPARSE_FLOAT_VECTOR)
                elif self.sparse:
                    # Server-side: sparse field + text with analyzer + BM25 function
                    schema.add_field(field_name="sparse", datatype=DataType.SPARSE_FLOAT_VECTOR)
                    schema.add_field(
                        field_name="text",
                        datatype=DataType.VARCHAR,
                        max_length=65535,
                        enable_analyzer=True,
                        analyzer_params={"type": "english"},
                        enable_match=True,
                    )
                    # Add BM25 function
                    if Function is not None and FunctionType is not None:
                        schema.add_function(
                            Function(
                                name="bm25",
                                function_type=FunctionType.BM25,
                                input_field_names=["text"],
                                output_field_names="sparse",
                            )
                        )
                else:
                    # No sparse: simple text field
                    schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=65535)
                
                return schema
            except Exception as e:
                logger.warning(f"Failed to use MilvusClient API, falling back to FieldSchema: {e}")
        
        # Fallback: Use FieldSchema API (works with older pymilvus versions)
        fields = []
        
        # Primary key field
        pk_field = FieldSchema(
            name="pk",
            dtype=DataType.INT64,
            is_primary=True,
            auto_id=True
        )
        fields.append(pk_field)
        
        # Vector field
        vector_field = FieldSchema(
            name="vector",
            dtype=DataType.FLOAT_VECTOR,
            dim=self.dimension
        )
        fields.append(vector_field)
        
        # Source field
        source_field = FieldSchema(
            name="source",
            dtype=DataType.JSON
        )
        fields.append(source_field)
        
        # Content metadata field
        # Try to set nullable if supported (matches NeMo-Retriever: nullable=True when not local_index)
        try:
            content_metadata_field = FieldSchema(
                name="content_metadata",
                dtype=DataType.JSON,
                nullable=True if not self.local_index else False
            )
        except TypeError:
            # Fallback if nullable parameter not supported in this pymilvus version
            content_metadata_field = FieldSchema(
                name="content_metadata",
                dtype=DataType.JSON
            )
            logger.warning("FieldSchema doesn't support nullable parameter, content_metadata will not be nullable")
        fields.append(content_metadata_field)
        
        # Text and sparse fields
        if self.sparse and self.local_index:
            # Local index: simple sparse field
            sparse_field = FieldSchema(
                name="sparse",
                dtype=DataType.SPARSE_FLOAT_VECTOR
            )
            fields.append(sparse_field)
        elif self.sparse:
            # Server-side: sparse field + text with analyzer
            sparse_field = FieldSchema(
                name="sparse",
                dtype=DataType.SPARSE_FLOAT_VECTOR
            )
            fields.append(sparse_field)
            
            # Text field - Note: enable_analyzer and enable_match may not be available in older pymilvus
            # We'll create basic field, but BM25 function won't be available
            text_field = FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535
            )
            fields.append(text_field)
            logger.warning("Using FieldSchema fallback: BM25 function and text analyzer may not be available. "
                          "Consider upgrading pymilvus or using MilvusClient API.")
        else:
            # No sparse: simple text field
            text_field = FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535
            )
            fields.append(text_field)
        
        # Create schema with dynamic fields enabled
        # Use empty description to match NeMo-Retriever client schema exactly
        schema = CollectionSchema(
            fields=fields,
            description="",  # Match NeMo-Retriever client: empty description
            enable_dynamic_field=True
        )
        
        return schema
    
    def _create_nemo_retriever_index_params(self):
        """
        Create NeMo-Retriever compatible index params matching create_nemo_retriever_index_params()
        
        Returns:
            IndexParams or dict for index creation
        """
        if MILVUS_CLIENT_AVAILABLE and IndexParams is not None and MilvusClient is not None:
            try:
                index_params = MilvusClient.prepare_index_params()
                
                # Dense vector index
                if self.local_index:
                    index_params.add_index(
                        field_name="vector",
                        index_name="dense_index",
                        index_type="FLAT",
                        metric_type="L2",
                    )
                else:
                    if self.gpu_index:
                        index_params.add_index(
                            field_name="vector",
                            index_name="dense_index",
                            index_type="GPU_CAGRA",
                            metric_type="L2",
                            params={
                                "intermediate_graph_degree": 128,
                                "graph_degree": 100,
                                "build_algo": "NN_DESCENT",
                                "cache_dataset_on_device": "true",
                                "adapt_for_cpu": "false" if self.gpu_search else "true",
                            },
                        )
                    else:
                        index_params.add_index(
                            field_name="vector",
                            index_name="dense_index",
                            index_type="HNSW",
                            metric_type="L2",
                            params={"M": 64, "efConstruction": 512},
                        )
                
                # Sparse vector index
                if self.sparse and self.local_index:
                    index_params.add_index(
                        field_name="sparse",
                        index_name="sparse_index",
                        index_type="SPARSE_INVERTED_INDEX",
                        metric_type="IP",
                        params={"drop_ratio_build": 0.2},
                    )
                elif self.sparse:
                    index_params.add_index(
                        field_name="sparse",
                        index_name="sparse_index",
                        index_type="SPARSE_INVERTED_INDEX",
                        metric_type="BM25",
                    )
                
                # Add inverted index on source["source_name"] JSON path for fast deletion
                # Milvus 2.5.10+ supports JSON path indexing with json_path parameter
                # source_name format: "bucket/key/version" - uniquely identifies each record
                # This enables O(log n) deletion instead of O(n) full scan
                try:
                    index_params.add_index(
                        field_name="source",
                        index_name="source_name_path_index",
                        index_type="INVERTED",
                        params={
                            "json_path": 'source["source_name"]',
                            "json_cast_type": "varchar"
                        }
                    )
                    logger.info("Added JSON path index on source['source_name'] for fast deletion")
                except Exception as json_idx_error:
                    logger.warning(f"Could not add JSON path index on source['source_name']: {json_idx_error}. "
                                 f"Deletion may use full scan.")
                
                return index_params
            except Exception as e:
                logger.warning(f"Failed to use MilvusClient IndexParams, falling back to dict: {e}")
        
        # Fallback: Return dict for use with collection.create_index()
        # Note: This won't support sparse indexes in older pymilvus versions
        index_params = {
            "metric_type": self.metric_type,
            "index_type": self.index_type,
        }
        
        if self.index_type.startswith("IVF"):
            index_params["params"] = {"nlist": self.nlist}
        elif self.index_type == "GPU_CAGRA":
            # GPU_CAGRA requires specific params for proper functioning
            index_params["params"] = {
                "intermediate_graph_degree": 128,
                "graph_degree": 100,
                "build_algo": "NN_DESCENT",
                "cache_dataset_on_device": "true",
                "adapt_for_cpu": "false" if self.gpu_search else "true",
            }
        elif self.index_type == "HNSW":
            index_params["params"] = {"M": 64, "efConstruction": 512}
        
        logger.warning("Using fallback index params. Sparse indexes may not be available. "
                      "Consider upgrading pymilvus or using MilvusClient API.")
        
        return index_params
    
    def create_collection(self) -> bool:
        """
        Create collection if it doesn't exist using NeMo-Retriever compatible schema
        
        Returns:
            True if collection was created or already exists, False on error
        """
        try:
            # Check if collection already exists
            if self.collection_exists():
                logger.info(f"Collection '{self.collection_name}' already exists")
                self.collection = Collection(self.collection_name, using=self.connection_alias)
                return True
            
            # Connect if not connected
            if not connections.has_connection(self.connection_alias):
                if not self.connect():
                    return False
            
            logger.info(f"Creating collection '{self.collection_name}'...")
            
            # Use NeMo-Retriever compatible schema
            logger.info("Using NeMo-Retriever compatible schema (sparse={}, gpu_index={}, gpu_search={})".format(
                self.sparse, self.gpu_index, self.gpu_search))
            schema = self._create_nemo_retriever_schema()
            
            # Create collection
            collection = Collection(
                name=self.collection_name,
                schema=schema,
                using=self.connection_alias,
                consistency_level=self.consistency_level
            )
            
            logger.info(f"Collection '{self.collection_name}' created successfully")
            
            # Create index
            self.create_index(collection)
            
            # Load collection into memory (required for queries and insertions)
            try:
                logger.info(f"Loading collection '{self.collection_name}' into memory...")
                collection.load()
                logger.info(f"Collection '{self.collection_name}' loaded successfully")
            except Exception as load_error:
                # Collection might already be loaded, or there might be another issue
                # Check if collection is already loaded
                try:
                    if collection.has_index():
                        logger.debug(f"Collection '{self.collection_name}' may already be loaded or index not ready yet")
                    else:
                        logger.warning(f"Could not load collection '{self.collection_name}': {load_error}. Collection may need to be loaded manually.")
                except Exception:
                    logger.warning(f"Could not verify collection load status: {load_error}")
            
            self.collection = collection
            return True
            
        except Exception as e:
            logger.error(f"Failed to create collection '{self.collection_name}': {e}")
            return False
    
    def create_index(self, collection: Optional[Collection] = None) -> bool:
        """
        Create index on vector field(s) using NeMo-Retriever compatible params
        
        Args:
            collection: Collection object (if None, uses self.collection)
        
        Returns:
            True if index created successfully, False otherwise
        """
        try:
            if collection is None:
                collection = self.collection
                
            if collection is None:
                collection = Collection(self.collection_name, using=self.connection_alias)
            
            # Check if index already exists
            if collection.has_index():
                logger.info(f"Index already exists on collection '{self.collection_name}'")
                return True
            
            # Use NeMo-Retriever compatible index params
            index_params = self._create_nemo_retriever_index_params()
            
            # If using MilvusClient IndexParams, we need to create indexes differently
            if hasattr(index_params, 'add_index'):
                # IndexParams object - indexes are already configured
                # For MilvusClient, indexes are created automatically when creating collection
                # But we can also create them manually if needed
                logger.info("Index params configured via MilvusClient API")
                # Try to create indexes using the collection's create_index method
                try:
                    # Get dense index config
                    if hasattr(index_params, '_indexes'):
                        indexes = getattr(index_params, '_indexes', [])
                        for idx_config in indexes:
                            if hasattr(idx_config, 'field_name'):
                                field_name = idx_config.field_name
                                index_name = getattr(idx_config, 'index_name', None)
                                index_type = getattr(idx_config, 'index_type', None)
                                metric_type = getattr(idx_config, 'metric_type', None)
                                params = getattr(idx_config, 'params', {})
                                
                                if index_type:
                                    idx_params = {"metric_type": metric_type} if metric_type else {}
                                    if params:
                                        idx_params["params"] = params
                                    
                                    create_kw = {"field_name": field_name, "index_params": {"index_type": index_type, **idx_params}}
                                    if index_name:
                                        create_kw["index_name"] = index_name
                                    collection.create_index(**create_kw)
                                    logger.info(f"Created index on field '{field_name}' with type '{index_type}' (index_name={index_name or field_name})")
                except Exception as e:
                    logger.warning(f"Could not create indexes via IndexParams object: {e}. "
                                 "Indexes may need to be created manually or will be created automatically.")
            else:
                # Dict-based index params (fallback)
                vector_field_name = "vector"
                logger.info(f"Creating index on collection '{self.collection_name}' (field: {vector_field_name}, index_name: dense_index)...")
                collection.create_index(
                    field_name=vector_field_name,
                    index_params=index_params,
                    index_name="dense_index"
                )
                
                # Create sparse index if sparse is enabled
                if self.sparse:
                    try:
                        sparse_index_params = {
                            "index_type": "SPARSE_INVERTED_INDEX",
                            "metric_type": "BM25" if not self.local_index else "IP",
                        }
                        if self.local_index:
                            sparse_index_params["params"] = {"drop_ratio_build": 0.2}
                        
                        collection.create_index(
                            field_name="sparse",
                            index_params=sparse_index_params
                        )
                        logger.info("Created sparse index on field 'sparse'")
                    except Exception as e:
                        logger.warning(f"Could not create sparse index: {e}")
                
                # Create inverted index on source["source_name"] JSON path for fast deletion
                # Milvus 2.5.10+ supports JSON path indexing
                # source_name format: "bucket/key/version" - uniquely identifies each record
                # This enables O(log n) deletion instead of O(n) full scan
                try:
                    source_path_index_params = {
                        "index_type": "INVERTED",
                        "params": {
                            "json_path": 'source["source_name"]',
                            "json_cast_type": "varchar"
                        }
                    }
                    collection.create_index(
                        field_name="source",
                        index_params=source_path_index_params,
                        index_name="source_name_path_index"
                    )
                    logger.info("Created JSON path index on source['source_name'] for fast deletion")
                except Exception as e:
                    logger.warning(f"Could not create source['source_name'] JSON path index: {e}. "
                                 f"Deletion may use full scan.")
            
            logger.info(f"Index created successfully on collection '{self.collection_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create index on collection '{self.collection_name}': {e}")
            return False
    
    def get_collection_dimension(self) -> Optional[int]:
        """
        Get the actual dimension of the vector field in the existing collection
        
        Returns:
            Dimension of the vector field, or None if collection doesn't exist or error occurs
        """
        try:
            if not self.collection:
                if not self.collection_exists():
                    return None
                self.collection = Collection(self.collection_name, using=self.connection_alias)
            
            schema = self.collection.schema
            # Find the vector field (could be "vector" or "embedding")
            for field in schema.fields:
                if field.dtype == DataType.FLOAT_VECTOR:
                    # Try to get dimension from field.dim first (direct attribute)
                    if hasattr(field, 'dim') and field.dim is not None:
                        return field.dim
                    # Fallback to field.params.get('dim')
                    elif hasattr(field, 'params') and isinstance(field.params, dict):
                        dim = field.params.get('dim')
                        if dim is not None:
                            return dim
                    # Try to get from to_dict() if available
                    if hasattr(field, 'to_dict'):
                        field_dict = field.to_dict()
                        if 'params' in field_dict and isinstance(field_dict['params'], dict):
                            dim = field_dict['params'].get('dim')
                            if dim is not None:
                                return dim
                    logger.warning(f"Could not extract dimension from vector field '{field.name}'")
            return None
        except Exception as e:
            logger.error(f"Error getting collection dimension: {e}")
            return None
    
    def ensure_collection(self) -> bool:
        """
        Ensure collection exists, create if not. Validates dimension matches if collection exists.
        Uses NeMo-Retriever compatible schema.
        
        Returns:
            True if collection exists or was created successfully, False otherwise
        """
        try:
            # Connect first
            if not connections.has_connection(self.connection_alias):
                if not self.connect():
                    return False
            
            # Check if collection exists
            if self.collection_exists():
                self.collection = Collection(self.collection_name, using=self.connection_alias)
                
                # Validate dimension matches
                actual_dimension = self.get_collection_dimension()
                if actual_dimension is not None and actual_dimension != self.dimension:
                    num_entities = self.collection.num_entities
                    logger.error(
                        f"Dimension mismatch detected! Collection '{self.collection_name}' has dimension {actual_dimension}, "
                        f"but expected {self.dimension}. This will cause embedding storage failures. "
                        f"Collection has {num_entities} entities."
                    )
                    
                    # If collection is empty, we can recreate it
                    if num_entities == 0:
                        logger.warning(
                            f"Collection '{self.collection_name}' is empty. Dropping and recreating with correct dimension {self.dimension}..."
                        )
                        try:
                            utility.drop_collection(self.collection_name, using=self.connection_alias)
                            logger.info(f"Dropped collection '{self.collection_name}'")
                            # Recreate with correct dimension
                            return self.create_collection()
                        except Exception as drop_error:
                            logger.error(f"Failed to drop and recreate collection: {drop_error}")
                            return False
                    else:
                        logger.error(
                            f"Cannot automatically fix dimension mismatch - collection has {num_entities} entities. "
                            f"Please manually drop the collection or update the dimension configuration to match {actual_dimension}."
                        )
                        return False
                elif actual_dimension is not None:
                    logger.info(
                        f"Collection '{self.collection_name}' verified (dimension: {actual_dimension}, matches expected: {self.dimension})"
                    )
                else:
                    logger.info(f"Collection '{self.collection_name}' verified (could not verify dimension)")
                
                # Ensure collection is loaded into memory
                try:
                    # Check if collection is already loaded
                    # Note: There's no direct way to check load status, so we try to load it
                    # If it's already loaded, load() will succeed without error
                    logger.debug(f"Ensuring collection '{self.collection_name}' is loaded into memory...")
                    self.collection.load()
                    logger.info(f"Collection '{self.collection_name}' is loaded and ready")
                except Exception as load_error:
                    # Collection might already be loaded, or there might be another issue
                    # Try to proceed - if collection is already loaded, operations will work
                    logger.debug(f"Collection '{self.collection_name}' load check: {load_error} (may already be loaded)")
                
                return True
            
            # Create collection
            return self.create_collection()
            
        except Exception as e:
            logger.error(f"Error ensuring collection exists: {e}")
            return False
    
    def get_collection_info(self) -> Dict[str, Any]:
        """
        Get collection information
        
        Returns:
            Dictionary with collection information
        """
        try:
            if not self.collection:
                if not self.collection_exists():
                    return {"error": "Collection does not exist"}
                self.collection = Collection(self.collection_name, using=self.connection_alias)
            
            schema = self.collection.schema
            num_entities = self.collection.num_entities
            
            consistency_level = self.consistency_level
            try:
                if hasattr(self.collection, 'consistency_level'):
                    consistency_level = self.collection.consistency_level
            except (AttributeError, Exception):
                pass
            
            # Check if collection has indexes (handle multiple indexes case)
            has_index = False
            try:
                has_index = self.collection.has_index()
            except Exception as idx_error:
                # If there are multiple indexes, has_index() may fail
                # Try to list indexes instead
                try:
                    indexes = self.collection.indexes
                    has_index = len(indexes) > 0 if indexes else False
                except Exception:
                    # If we can't determine, assume indexes exist (collection was created with indexes)
                    has_index = True
                    logger.debug(f"Could not determine index status, assuming indexes exist: {idx_error}")
            
            info = {
                "collection_name": self.collection_name,
                "num_entities": num_entities,
                "fields": [field.name for field in schema.fields],
                "has_index": has_index,
                "consistency_level": consistency_level
            }
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting collection info: {e}")
            return {"error": str(e)}
    
    def update_collection_config(self, collection_name: Optional[str] = None,
                                  dimension: Optional[int] = None,
                                  index_type: Optional[str] = None,
                                  metric_type: Optional[str] = None):
        """
        Update collection configuration (requires re-initialization)
        
        Args:
            collection_name: New collection name
            dimension: New vector dimension
            index_type: New index type
            metric_type: New metric type
        """
        if collection_name:
            self.collection_name = collection_name
        if dimension:
            self.dimension = dimension
        if index_type:
            self.index_type = index_type
        if metric_type:
            self.metric_type = metric_type
        
        logger.info("Collection configuration updated, re-initialize collection")
    
    def __enter__(self):
        """Context manager entry"""
        self.ensure_collection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
