import warnings
from typing import Any, AsyncIterator, List, Optional, Dict, Tuple, Sequence, Union, Iterable, TYPE_CHECKING
import asyncio

from mockfirestore import AlreadyExists
from mockfirestore._helpers import PATH_ELEMENT_SEPARATOR, generate_random_string, Store, get_by_path, is_path_element_collection_marked, set_by_path, Timestamp, traverse_dict
from mockfirestore.async_.query import AsyncQuery
from mockfirestore.async_.document import AsyncDocumentReference, AsyncDocumentSnapshot

if TYPE_CHECKING:
    from mockfirestore.async_.aggregation import AsyncAggregationQuery


class AsyncCollectionReference:
    """Asynchronous collection reference."""

    def __init__(self, data: Store, path: List[str],
                 parent: Optional[AsyncDocumentReference] = None) -> None:
        self._data = data
        self._path = path
        self.parent = parent

    def document(self, document_id: Optional[str] = None) -> AsyncDocumentReference:
        """Get a document reference.

        Args:
            document_id: The document identifier.

        Returns:
            An AsyncDocumentReference.
        """
        # Make sure the collection exists in the data store
        try:
            collection = get_by_path(self._data, self._path)
        except KeyError:
            # Create the collection path if it doesn't exist
            collection = get_by_path(self._data, self._path, create_nested=True)
            
        if document_id is None:
            document_id = generate_random_string()
        new_path = self._path + [document_id]
        if document_id not in collection:
            set_by_path(self._data, new_path, {})
        return AsyncDocumentReference(self._data, new_path, parent=self)

    async def get(self) -> List[AsyncDocumentSnapshot]:
        """Get all documents in the collection.

        Returns:
            A list of AsyncDocumentSnapshot objects.

        Deprecated:
            Use AsyncCollectionReference.stream instead.
        """
        warnings.warn('Collection.get is deprecated, please use Collection.stream',
                      category=DeprecationWarning)
        results = []
        async for doc in self.stream():
            results.append(doc)
        return results

    async def add(self, document_data: Dict, document_id: str = None) \
            -> Tuple[Timestamp, AsyncDocumentReference]:
        """Add a document to the collection.

        Args:
            document_data: The document data.
            document_id: The document identifier. If not provided, it will be generated.

        Returns:
            A tuple containing the timestamp and the document reference.

        Raises:
            AlreadyExists: If the document already exists.
        """
        if document_id is None:
            document_id = document_data.get('id', generate_random_string())
        collection = get_by_path(self._data, self._path)
        new_path = self._path + [document_id]
        if document_id in collection:
            raise AlreadyExists('Document already exists: {}'.format(new_path))
        doc_ref = AsyncDocumentReference(self._data, new_path, parent=self)
        await doc_ref.set(document_data)
        timestamp = Timestamp.from_now()
        return timestamp, doc_ref

    def where(self, field: Optional[str] = None, op: Optional[str] = None, value: Any = None, filter=None) -> AsyncQuery:
        """Create a query with a filter.

        Args:
            field: The field to filter on.
            op: The operator to filter with.
            value: The value to compare against.
            filter: A composite filter for complex queries.

        Returns:
            An AsyncQuery with the filter applied.
        """
        query = AsyncQuery(self, field_filters=[AsyncQuery.make_field_filter(field, op, value, filter)])
        return query

    def order_by(self, field_path: str, direction: Optional[str] = None) -> AsyncQuery:
        """Create a query with an order.

        Args:
            field_path: The field to order by.
            direction: The direction to order in ('ASCENDING' or 'DESCENDING').

        Returns:
            An AsyncQuery with the order applied.
        """
        query = AsyncQuery(self, orders=[(field_path, direction)])
        return query

    def limit(self, count: int) -> AsyncQuery:
        """Create a query with a limit.

        Args:
            count: The maximum number of documents to return.

        Returns:
            An AsyncQuery with the limit applied.
        """
        query = AsyncQuery(self, limit=count)
        return query

    def offset(self, num_to_skip: int) -> AsyncQuery:
        """Create a query with an offset.

        Args:
            num_to_skip: The number of documents to skip.

        Returns:
            An AsyncQuery with the offset applied.
        """
        query = AsyncQuery(self, offset=num_to_skip)
        return query

    def start_at(self, document_fields_or_snapshot: Union[dict, AsyncDocumentSnapshot]) -> AsyncQuery:
        """Create a query with a start point.

        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.

        Returns:
            An AsyncQuery with the start point applied.
        """
        query = AsyncQuery(self, start_at=(document_fields_or_snapshot, True))
        return query

    def start_after(self, document_fields_or_snapshot: Union[dict, AsyncDocumentSnapshot]) -> AsyncQuery:
        """Create a query with a start point after a document.

        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.

        Returns:
            An AsyncQuery with the start point applied.
        """
        query = AsyncQuery(self, start_at=(document_fields_or_snapshot, False))
        return query

    def end_at(self, document_fields_or_snapshot: Union[dict, AsyncDocumentSnapshot]) -> AsyncQuery:
        """Create a query with an end point.

        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.

        Returns:
            An AsyncQuery with the end point applied.
        """
        query = AsyncQuery(self, end_at=(document_fields_or_snapshot, True))
        return query

    def end_before(self, document_fields_or_snapshot: Union[dict, AsyncDocumentSnapshot]) -> AsyncQuery:
        """Create a query with an end point before a document.

        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.

        Returns:
            An AsyncQuery with the end point applied.
        """
        query = AsyncQuery(self, end_at=(document_fields_or_snapshot, False))
        return query
    
    def select(self, field_paths: Iterable[str]):
        """Create a query with a projection.
        
        Args:
            field_paths: The fields to include in the result.

        Returns:
            An AsyncQuery with the projection applied.
        """
        query = AsyncQuery(self, projection=field_paths)
        return query
        
    def count(self, alias: Optional[str] = None) -> 'AsyncAggregationQuery':
        """Adds a count over the collection.
        
        Args:
            alias: Optional name of the field to store the result.
            
        Returns:
            An AsyncAggregationQuery with the count aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).count(alias)

    def avg(self, field_ref, alias: Optional[str] = None) -> 'AsyncAggregationQuery':
        """Adds an average over the collection.
        
        Args:
            field_ref: The field to aggregate across.
            alias: Optional name of the field to store the result.
            
        Returns:
            An AsyncAggregationQuery with the average aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).avg(field_ref, alias)

    def sum(self, field_ref, alias: Optional[str] = None) -> 'AsyncAggregationQuery':
        """Adds a sum over the collection.
        
        Args:
            field_ref: The field to aggregate across.
            alias: Optional name of the field to store the result.
            
        Returns:
            An AsyncAggregationQuery with the sum aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).sum(field_ref, alias)

    async def list_documents(self, page_size: Optional[int] = None) -> AsyncIterator[AsyncDocumentReference]:
        """List all document references in the collection.

        Args:
            page_size: The maximum number of document references to return per page.

        Returns:
            A list of AsyncDocumentReference objects.
        """
        for key in get_by_path(self._data, self._path):
            yield self.document(key)

    async def stream(self, transaction=None):
        """Stream the documents in the collection.

        Args:
            transaction: If provided, the operation will be executed within
                this transaction.

        Returns:
            An asynchronous iterator of AsyncDocumentSnapshot objects.
        """
        for key in sorted(get_by_path(self._data, self._path)):
            doc_snapshot = await self.document(key).get()
            yield doc_snapshot


class AsyncCollectionGroup:
    """Asynchronous collection group."""

    def __init__(
        self,
        data: Store,
        collection_id: str,
        projection=None,
        field_filters=(),
        orders=(),
        limit=None,
        limit_to_last=False,
        offset=None,
        start_at=None,
        end_at=None,
        all_descendants=True,
        recursive=False,
    ):
        self._data = data
        self._collection_id = collection_id
        self._projection = projection
        self._field_filters = field_filters
        self._orders = orders
        self._limit = limit
        self._limit_to_last = limit_to_last
        self._offset = offset
        self._start_at = start_at
        self._end_at = end_at
        self._all_descendants = all_descendants
        self._recursive = recursive

    def _find_collections(self) -> List[AsyncCollectionReference]:
        """
        Recursively find all subcollections matching collection_id.
        Returns list of (collection_dict, path, parent_docref)
        """
        collections: List[AsyncCollectionReference] = []

        def append_collection(key: str, current_path: str, collection_dict: Dict[str, Any]):
            if not is_path_element_collection_marked(key):
                return

            collections.append(AsyncCollectionReference(self._data, current_path.split(PATH_ELEMENT_SEPARATOR)))

        traverse_dict(self._data, append_collection)

        relevant_collections = [
            collection for collection in collections
            if collection._path[-1] == self._collection_id
        ]

        return relevant_collections

    def _copy(self, **kwargs):
        args = dict(
            data=self._data,
            collection_id=self._collection_id,
            projection=self._projection,
            field_filters=self._field_filters,
            orders=self._orders,
            limit=self._limit,
            limit_to_last=self._limit_to_last,
            offset=self._offset,
            start_at=self._start_at,
            end_at=self._end_at,
            all_descendants=self._all_descendants,
            recursive=self._recursive,
        )
        args.update(kwargs)
        return AsyncCollectionGroup(**args)

    # ---- Query/Chaining methods ----
    def where(self, field=None, op=None, value=None, filter=None):
        """Create a query with a filter.

        Args:
            field: The field to filter on.
            op: The operator to filter with.
            value: The value to compare against.
            filter: A composite filter for complex queries.

        Returns:
            An AsyncCollectionGroup with the filter applied.
        """
        new_filters = self._field_filters + (AsyncQuery.make_field_filter(field, op, value, filter),)
        return self._copy(field_filters=new_filters)

    def order_by(self, field_path: str, direction: Optional[str] = None):
        """Create a query with an order.

        Args:
            field_path: The field to order by.
            direction: The direction to order in ('ASCENDING' or 'DESCENDING').

        Returns:
            An AsyncCollectionGroup with the order applied.
        """
        new_orders = self._orders + ((field_path, direction),)
        return self._copy(orders=new_orders)

    def limit(self, count: int):
        """Create a query with a limit.

        Args:
            count: The maximum number of documents to return.

        Returns:
            An AsyncCollectionGroup with the limit applied.
        """
        return self._copy(limit=count)

    def limit_to_last(self, count: int):
        """Create a query with a limit, returning the last matching documents.

        Args:
            count: The maximum number of documents to return.

        Returns:
            An AsyncCollectionGroup with the limit applied.
        """
        return self._copy(limit=count, limit_to_last=True)

    def offset(self, num_to_skip: int):
        """Create a query with an offset.

        Args:
            num_to_skip: The number of documents to skip.

        Returns:
            An AsyncCollectionGroup with the offset applied.
        """
        return self._copy(offset=num_to_skip)

    def start_at(self, document_fields_or_snapshot):
        """Create a query with a start point.

        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.

        Returns:
            An AsyncCollectionGroup with the start point applied.
        """
        return self._copy(start_at=(document_fields_or_snapshot, True))

    def start_after(self, document_fields_or_snapshot):
        """Create a query with a start point after a document.

        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.

        Returns:
            An AsyncCollectionGroup with the start point applied.
        """
        return self._copy(start_at=(document_fields_or_snapshot, False))

    def end_at(self, document_fields_or_snapshot):
        """Create a query with an end point.

        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.

        Returns:
            An AsyncCollectionGroup with the end point applied.
        """
        return self._copy(end_at=(document_fields_or_snapshot, True))

    def end_before(self, document_fields_or_snapshot):
        """Create a query with an end point before a document.

        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.

        Returns:
            An AsyncCollectionGroup with the end point applied.
        """
        return self._copy(end_at=(document_fields_or_snapshot, False))
    
    def select(self, field_paths: Iterable[str]):
        """Create a query with a projection.
        
        Args:
            field_paths: The fields to include in the result.
            
        Returns:
            A new AsyncQuery with the projection applied.
        """
        return self._copy(projection=field_paths)

    # ---- Aggregations ----
    def count(self, alias=None):
        """Count documents in the collection group.

        Args:
            alias: An alias for the count.

        Returns:
            An AsyncAggregationQuery with the count aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).count(alias)

    def avg(self, field_ref, alias=None):
        """Calculate the average of a field.

        Args:
            field_ref: The field to average.
            alias: An alias for the average.

        Returns:
            An AsyncAggregationQuery with the average aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).avg(field_ref, alias)

    def sum(self, field_ref, alias=None):
        """Calculate the sum of a field.

        Args:
            field_ref: The field to sum.
            alias: An alias for the sum.

        Returns:
            An AsyncAggregationQuery with the sum aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).sum(field_ref, alias)

    def find_nearest(
        self,
        vector_field,
        query_vector,
        limit,
        distance_measure,
        *,
        distance_result_field=None,
        distance_threshold=None
    ):
        """Find nearest vectors (stub).

        Args:
            vector_field: The field containing vectors.
            query_vector: The query vector.
            limit: The maximum number of results.
            distance_measure: The distance measure to use.
            distance_result_field: The field to store distance results.
            distance_threshold: The distance threshold for inclusion.

        Returns:
            The collection group.
        """
        return self

    # ---- Streaming/get ----
    async def stream(self, transaction=None, retry=None, timeout=None, *, explain_options=None) -> AsyncIterator[AsyncDocumentSnapshot]:
        """Stream the documents in the collection group.

        Args:
            transaction: If provided, the operation will be executed within this transaction.
            retry: Retry options for the operation.
            timeout: Timeout for the operation.
            explain_options: Query explanation options.

        Returns:
            An asynchronous iterator of AsyncDocumentSnapshot objects.
        """
        async for doc in self._iter_documents():
            yield doc.get(transaction=transaction, retry=retry, timeout=timeout)

    async def get(self, transaction=None, retry=None, timeout=None, *, explain_options=None) -> List[AsyncDocumentSnapshot]:
        """Get all documents in the collection group.

        Args:
            transaction: If provided, the operation will be executed within this transaction.
            retry: Retry options for the operation.
            timeout: Timeout for the operation.
            explain_options: Query explanation options.

        Returns:
            A list of AsyncDocumentSnapshot objects.
        """
        results = []
        async for doc in self.stream(transaction=transaction, retry=retry, timeout=timeout, explain_options=explain_options):
            results.append(await doc)
        return results

    async def list_documents(self, page_size: Optional[int] = None) -> AsyncIterator[AsyncDocumentReference]:
        async for doc in self._iter_documents():
            yield doc

    def on_snapshot(self, callback):
        """Register a callback for snapshot updates.

        Args:
            callback: The callback to register.

        Raises:
            NotImplementedError: on_snapshot is not supported in the mock.
        """
        raise NotImplementedError("on_snapshot is not supported in mock.")

    # ---- Internal: yield DocumentSnapshot objects, filtered ----
    async def _iter_documents(self) -> AsyncIterator[AsyncDocumentReference]:
        for collection_reference in self._find_collections():
            async for document in collection_reference.list_documents():
                yield document

    def __repr__(self):
        return f"<AsyncCollectionGroup '{self._collection_id}'>"
