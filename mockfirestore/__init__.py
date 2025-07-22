# by analogy with
# https://github.com/mongomock/mongomock/blob/develop/mongomock/__init__.py
# try to import gcloud exceptions
# and if gcloud is not installed, define our own
from google.api_core.exceptions import ClientError, Conflict, NotFound, AlreadyExists, InvalidArgument
from google.cloud.firestore import DELETE_FIELD, Increment, ArrayUnion, ArrayRemove


# Synchronous implementations
from mockfirestore.client import MockFirestore
from mockfirestore.document import DocumentSnapshot, DocumentReference
from mockfirestore.collection import CollectionReference, CollectionGroup
from mockfirestore.query import Query, And, Or
from mockfirestore._helpers import Timestamp
from mockfirestore.transaction import Transaction
from mockfirestore.aggregation import AggregationQuery, AggregationResult

# Asynchronous implementations
from mockfirestore.async_ import AsyncMockFirestore
from mockfirestore.async_.document import AsyncDocumentSnapshot, AsyncDocumentReference
from mockfirestore.async_.aggregation import AsyncAggregationQuery
from mockfirestore.async_.collection import AsyncCollectionReference, AsyncCollectionGroup
from mockfirestore.async_.query import AsyncQuery
from mockfirestore.async_.transaction import AsyncTransaction, AsyncBatch
