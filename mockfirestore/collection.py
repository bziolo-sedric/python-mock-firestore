import warnings
from typing import Any, Callable, Iterator, List, Optional, Iterable, Dict, Tuple, Sequence, Union, TYPE_CHECKING

from mockfirestore import AlreadyExists
from mockfirestore._helpers import PATH_ELEMENT_SEPARATOR, generate_random_string, Store, get_by_path, is_path_element_collection_marked, set_by_path, Timestamp, traverse_dict
from mockfirestore.query import Query
from mockfirestore.document import DocumentReference, DocumentSnapshot

if TYPE_CHECKING:
    from mockfirestore.aggregation import AggregationQuery


class CollectionReference:
    def __init__(self, data: Store, path: List[str],
                 parent: Optional[DocumentReference] = None) -> None:
        self._data = data
        self._path = path
        self.parent = parent

    def document(self, document_id: Optional[str] = None) -> DocumentReference:
        collection = get_by_path(self._data, self._path)
        if document_id is None:
            document_id = generate_random_string()
        new_path = self._path + [document_id]
        if document_id not in collection:
            set_by_path(self._data, new_path, {})
        return DocumentReference(self._data, new_path, parent=self)

    def get(self) -> List[DocumentSnapshot]:
        warnings.warn('Collection.get is deprecated, please use Collection.stream',
                      category=DeprecationWarning)
        return list(self.stream())

    def add(self, document_data: Dict, document_id: str = None) \
            -> Tuple[Timestamp, DocumentReference]:
        if document_id is None:
            document_id = document_data.get('id', generate_random_string())
        collection = get_by_path(self._data, self._path)
        new_path = self._path + [document_id]
        if document_id in collection:
            raise AlreadyExists('Document already exists: {}'.format(new_path))
        doc_ref = DocumentReference(self._data, new_path, parent=self)
        doc_ref.set(document_data)
        timestamp = Timestamp.from_now()
        return timestamp, doc_ref

    def where(self, field: Optional[str] = None, op: Optional[str] = None, value: Any = None, filter=None) -> Query:
        query = Query(self, field_filters=[Query.make_field_filter(field, op, value, filter)])
        return query

    def order_by(self, field_path: str, direction: Optional[str] = None) -> Query:
        query = Query(self, orders=[(field_path, direction)])
        return query

    def limit(self, count: int) -> Query:
        query = Query(self, limit=count)
        return query

    def offset(self, num_to_skip: int) -> Query:
        query = Query(self, offset=num_to_skip)
        return query

    def start_at(self, document_fields_or_snapshot: Union[dict, DocumentSnapshot]) -> Query:
        query = Query(self, start_at=(document_fields_or_snapshot, True))
        return query

    def start_after(self, document_fields_or_snapshot: Union[dict, DocumentSnapshot]) -> Query:
        query = Query(self, start_at=(document_fields_or_snapshot, False))
        return query

    def end_at(self, document_fields_or_snapshot: Union[dict, DocumentSnapshot]) -> Query:
        query = Query(self, end_at=(document_fields_or_snapshot, True))
        return query

    def end_before(self, document_fields_or_snapshot: Union[dict, DocumentSnapshot]) -> Query:
        query = Query(self, end_at=(document_fields_or_snapshot, False))
        return query
        
    def select(self, field_paths: Iterable[str]) -> Query:
        query = Query(self, projection=field_paths)
        return query
        
    def count(self, alias: Optional[str] = None) -> 'AggregationQuery':
        """Adds a count over the collection.
        
        Args:
            alias: Optional name of the field to store the result.
            
        Returns:
            An AggregationQuery with the count aggregation.
        """
        from mockfirestore.aggregation import AggregationQuery
        return AggregationQuery(self, alias).count(alias)
        
    def avg(self, field_ref, alias: Optional[str] = None) -> 'AggregationQuery':
        """Adds an average over the collection.
        
        Args:
            field_ref: The field to aggregate across.
            alias: Optional name of the field to store the result.
            
        Returns:
            An AggregationQuery with the average aggregation.
        """
        from mockfirestore.aggregation import AggregationQuery
        return AggregationQuery(self, alias).avg(field_ref, alias)
        
    def sum(self, field_ref, alias: Optional[str] = None) -> 'AggregationQuery':
        """Adds a sum over the collection.
        
        Args:
            field_ref: The field to aggregate across.
            alias: Optional name of the field to store the result.
            
        Returns:
            An AggregationQuery with the sum aggregation.
        """
        from mockfirestore.aggregation import AggregationQuery
        return AggregationQuery(self, alias).sum(field_ref, alias)

    def list_documents(self, page_size: Optional[int] = None) -> Sequence[DocumentReference]:
        docs = []
        for key in get_by_path(self._data, self._path):
            docs.append(self.document(key))
        return docs

    def stream(self, transaction=None) -> Iterable[DocumentSnapshot]:
        for key in sorted(get_by_path(self._data, self._path)):
            doc_snapshot = self.document(key).get(transaction=transaction)
            yield doc_snapshot

class CollectionGroup:
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

    def _find_collections(self) -> List[CollectionReference]:
        """
        Recursively find all subcollections matching collection_id.
        Returns list of (collection_dict, path, parent_docref)
        """
        collections: List[CollectionReference] = []

        def append_collection(key: str, current_path: str, collection_dict: Dict[str, Any]):
            if not is_path_element_collection_marked(key):
                return

            collections.append(CollectionReference(self._data, current_path.split(PATH_ELEMENT_SEPARATOR)))

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
        return CollectionGroup(**args)

    # ---- Query/Chaining methods ----
    def where(self, field=None, op=None, value=None, filter=None):
        new_filters = self._field_filters + (Query.make_field_filter(field, op, value, filter),)
        return self._copy(field_filters=new_filters)

    def order_by(self, field_path: str, direction: Optional[str] = None):
        new_orders = self._orders + ((field_path, direction),)
        return self._copy(orders=new_orders)

    def limit(self, count: int):
        return self._copy(limit=count)

    def limit_to_last(self, count: int):
        return self._copy(limit=count, limit_to_last=True)

    def offset(self, num_to_skip: int):
        return self._copy(offset=num_to_skip)

    def start_at(self, document_fields_or_snapshot):
        return self._copy(start_at=(document_fields_or_snapshot, True))

    def start_after(self, document_fields_or_snapshot):
        return self._copy(start_at=(document_fields_or_snapshot, False))

    def end_at(self, document_fields_or_snapshot):
        return self._copy(end_at=(document_fields_or_snapshot, True))

    def end_before(self, document_fields_or_snapshot):
        return self._copy(end_at=(document_fields_or_snapshot, False))
    
    def select(self, field_paths: Iterable[str]):
        return self._copy(projection=field_paths)

    # ---- Aggregations ----
    def count(self, alias=None):
        """Adds a count over the collection group.
        
        Args:
            alias: Optional name of the field to store the result.
            
        Returns:
            An AggregationQuery with the count aggregation.
        """
        from mockfirestore.aggregation import AggregationQuery
        return AggregationQuery(self, alias).count(alias)

    def avg(self, field_ref, alias=None):
        """Adds an average over the collection group.
        
        Args:
            field_ref: The field to aggregate across.
            alias: Optional name of the field to store the result.
            
        Returns:
            An AggregationQuery with the average aggregation.
        """
        from mockfirestore.aggregation import AggregationQuery
        return AggregationQuery(self, alias).avg(field_ref, alias)

    def sum(self, field_ref, alias=None):
        """Adds a sum over the collection group.
        
        Args:
            field_ref: The field to aggregate across.
            alias: Optional name of the field to store the result.
            
        Returns:
            An AggregationQuery with the sum aggregation.
        """
        from mockfirestore.aggregation import AggregationQuery
        return AggregationQuery(self, alias).sum(field_ref, alias)

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
        return self

    # ---- Streaming/get ----
    def stream(self, transaction=None, retry=None, timeout=None, *, explain_options=None) -> Iterator[DocumentSnapshot]:
        for doc in self._iter_documents():
            yield doc.get(transaction=transaction, retry=retry, timeout=timeout)

    def get(self, transaction=None, retry=None, timeout=None, *, explain_options=None) -> List[DocumentSnapshot]:
        return list(self.stream(transaction=transaction, retry=retry, timeout=timeout, explain_options=explain_options))

    def list_documents(self, page_size: Optional[int] = None) -> Iterator[DocumentReference]:
        yield from self._iter_documents()

    def on_snapshot(self, callback: Callable) -> None:
        raise NotImplementedError("on_snapshot is not supported in mock.")

    # ---- Internal: yield DocumentSnapshot objects, filtered ----
    def _iter_documents(self) -> Iterator[DocumentReference]:
        for collection_reference in self._find_collections():
            yield from collection_reference.list_documents()

    def __repr__(self):
        return f"<CollectionGroup '{self._collection_id}'>"
