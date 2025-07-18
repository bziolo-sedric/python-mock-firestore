import warnings
from typing import Any, List, Optional, Iterable, Dict, Tuple, Sequence, Union, TYPE_CHECKING

from mockfirestore import AlreadyExists
from mockfirestore._helpers import generate_random_string, Store, get_by_path, set_by_path, Timestamp
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
            doc_snapshot = self.document(key).get()
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

    def _find_collections(self, node, path, parent_docref=None):
        """
        Recursively find all subcollections matching collection_id.
        Returns list of (collection_dict, path, parent_docref)
        """
        found = []
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, dict) and not key.startswith("_"):
                    if key == self._collection_id:
                        found.append((value, path + [key], parent_docref))
                    for doc_key, doc_value in value.items():
                        if isinstance(doc_value, dict):
                            # Prepare the DocumentReference for this document as parent
                            doc_path = path + [key, doc_key]
                            docref_parent = CollectionReference(self._data, path + [key], parent=parent_docref)
                            docref = DocumentReference(self._data, doc_path, parent=docref_parent)
                            found += self._find_collections(doc_value, doc_path, parent_docref=docref)
        return found

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
    def stream(self, transaction=None, retry=None, timeout=None, *, explain_options=None):
        docs = list(self._iter_documents())
        for doc in docs:
            yield doc

    def get(self, transaction=None, retry=None, timeout=None, *, explain_options=None) -> List[DocumentSnapshot]:
        return list(self.stream(transaction=transaction, retry=retry, timeout=timeout, explain_options=explain_options))

    def list_documents(self, page_size=None):
        docs = []
        collections = self._find_collections(self._data, [], parent_docref=None)
        for collection, path, parent_docref in collections:
            collection_ref = CollectionReference(self._data, path, parent=parent_docref)
            for doc_id in collection:
                docs.append(DocumentReference(self._data, path + [doc_id], parent=collection_ref))
        return docs

    def on_snapshot(self, callback):
        raise NotImplementedError("on_snapshot is not supported in mock.")

    # ---- Internal: yield DocumentSnapshot objects, filtered ----
    def _iter_documents(self):
        collections = self._find_collections(self._data, [], parent_docref=None)
        docs = []
        for collection, path, parent_docref in collections:
            collection_ref = CollectionReference(self._data, path, parent=parent_docref)
            for doc_id in collection:
                doc_ref = DocumentReference(self._data, path + [doc_id], parent=collection_ref)
                docs.append(doc_ref.get())
        # Filtering, ordering, etc would go here.
        return docs

    def __repr__(self):
        return f"<CollectionGroup '{self._collection_id}'>"
