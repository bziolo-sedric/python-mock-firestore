import warnings
from typing import Any, AsyncIterator, List, Literal, Optional, Dict, Tuple, Sequence, Union, Iterable, TYPE_CHECKING
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

    async def stream(self, transaction=None) -> AsyncIterator[AsyncDocumentSnapshot]:
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


class AsyncCollectionGroup(AsyncQuery):
    """Asynchronous collection group."""

    def __init__(
        self,
        parent: AsyncCollectionReference,
        projection=None,
        field_filters: Tuple[Tuple[str, str, Any], ...] = tuple(), 
        orders: Tuple[Tuple[str, Union[Literal['DESCENDING'], Literal['ASCENDING']]], ...] = tuple(), 
        limit: Optional[int] = None, 
        limit_to_last: Optional[int] = None, 
        offset: Optional[int] = None,
        start_at: Optional[Any] = None, 
        end_at: Optional[Any] = None, 
        all_descendants: bool = False,
        recursive: bool = False
    ):
        super().__init__(
            parent=parent,
            projection=projection,
            field_filters=field_filters,
            orders=orders,
            limit=limit,
            limit_to_last=limit_to_last,
            offset=offset,
            start_at=start_at,
            end_at=end_at,
            all_descendants=all_descendants
        )

        self._recursive = recursive

    def _copy(self, **kwargs):
        """Create a copy of this query with the specified changes.
        
        Args:
            **kwargs: The changes to apply to the query.
            
        Returns:
            A new AsyncQuery with the changes applied.
        """
        args = dict(
            parent=self._parent,
            field_filters=self._field_filters,
            orders=self._orders,
            limit=self._limit,
            limit_to_last=self._limit_to_last,
            offset=self._offset,
            start_at=self._start_at,
            end_at=self._end_at,
            all_descendants=self._all_descendants,
            projection=self._projection,
            recursive=self._recursive
        )
        args.update(kwargs)
        return AsyncCollectionGroup(**args)

    def _get_collection_id(self) -> str:
        """Get the collection ID from the parent path."""
        parent: AsyncCollectionReference = self._parent

        if not parent._path:
            raise ValueError("CollectionGroup must have a parent with a valid path.")

        return parent._path[-1]

    def _find_collections(self) -> List[AsyncCollectionReference]:
        """
        Recursively find all subcollections matching collection_id.
        Returns list of (collection_dict, path, parent_docref)
        """
        collections: List[AsyncCollectionReference] = []

        parent: AsyncCollectionReference = self._parent

        def append_collection(key: str, current_path: str, collection_dict: Dict[str, Any]):
            if not is_path_element_collection_marked(key):
                return

            collections.append(AsyncCollectionReference(parent._data, current_path.split(PATH_ELEMENT_SEPARATOR)))

        traverse_dict(parent._data, append_collection)

        relevant_collections = [
            collection for collection in collections
            if collection._path[-1] == self._get_collection_id()
            and collection != self._parent  # Exclude the parent collection itself
        ]

        return relevant_collections

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

    def on_snapshot(self, callback):
        """Register a callback for snapshot updates.

        Args:
            callback: The callback to register.

        Raises:
            NotImplementedError: on_snapshot is not supported in the mock.
        """
        raise NotImplementedError("on_snapshot is not supported in mock.")

    def __repr__(self):
        return f"<AsyncCollectionGroup '{self._collection_id}'>"
