import warnings
from typing import Any, Callable, Iterator, List, Literal, Optional, Iterable, Dict, Tuple, Sequence, Union, TYPE_CHECKING

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

class CollectionGroup(Query):
    def __init__(
        self,
        parent: CollectionReference,
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
            orders=self.orders,
            limit=self._limit,
            limit_to_last=self._limit_to_last,
            offset=self._offset,
            start_at=self._start_at,
            end_at=self._end_at,
            all_descendants=self.all_descendants,
            projection=self._projection,
            recursive=self._recursive
        )
        args.update(kwargs)
        return CollectionGroup(**args)

    def _get_collection_id(self) -> str:
        """Get the collection ID from the parent path."""
        parent: CollectionReference = self._parent

        if not parent._path:
            raise ValueError("CollectionGroup must have a parent with a valid path.")

        return parent._path[-1]

    def _find_collections(self) -> List[CollectionReference]:
        """
        Recursively find all subcollections matching collection_id.
        Returns list of (collection_dict, path, parent_docref)
        """
        collections: List[CollectionReference] = []

        parent: CollectionReference = self._parent

        def append_collection(key: str, current_path: str, collection_dict: Dict[str, Any]):
            if not is_path_element_collection_marked(key):
                return

            collections.append(CollectionReference(parent._data, current_path.split(PATH_ELEMENT_SEPARATOR)))

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
        return self

    def on_snapshot(self, callback: Callable) -> None:
        raise NotImplementedError("on_snapshot is not supported in mock.")

    def __repr__(self):
        return f"<CollectionGroup '{self._get_collection_id()}'>"
