from functools import partial
from typing import Iterable, Callable, List

from mockfirestore._helpers import generate_random_string, FIRESTORE_DOCUMENT_SIZE_LIMIT, calculate_document_size
from mockfirestore import InvalidArgument
from mockfirestore.document import DocumentReference, DocumentSnapshot
from mockfirestore.query import Query
from mockfirestore.write_result import WriteResult

MAX_ATTEMPTS = 5
_MISSING_ID_TEMPLATE = "The transaction has no transaction ID, so it cannot be {}."
_CANT_BEGIN = "The transaction has already begun. Current transaction ID: {!r}."
_CANT_ROLLBACK = _MISSING_ID_TEMPLATE.format("rolled back")
_CANT_COMMIT = _MISSING_ID_TEMPLATE.format("committed")


class Transaction:
    """
    This mostly follows the model from
    https://googleapis.dev/python/firestore/latest/transaction.html
    """

    def __init__(self, client,
                 max_attempts=MAX_ATTEMPTS, read_only=False):
        self._client = client
        self._max_attempts = max_attempts
        self._read_only = read_only
        self._id = None
        self._write_ops = []
        self.write_results = None

    @property
    def in_progress(self):
        return self._id is not None

    @property
    def id(self):
        return self._id

    def _begin(self, retry_id=None):
        # generate a random ID to set the transaction as in_progress
        self._id = generate_random_string()

    def _clean_up(self):
        self._write_ops.clear()
        self._id = None

    def _rollback(self):
        if not self.in_progress:
            raise ValueError(_CANT_ROLLBACK)

        self._clean_up()

    def _commit(self) -> List[WriteResult]:
        if not self.in_progress:
            raise ValueError(_CANT_COMMIT)

        results = []
        for write_op in self._write_ops:
            write_op()
            results.append(WriteResult())
        self.write_results = results
        self._clean_up()
        return results

    def get_all(self,
                references: Iterable[DocumentReference]) -> Iterable[DocumentSnapshot]:
        return self._client.get_all(references)

    def get(self, ref_or_query) -> Iterable[DocumentSnapshot]:
        if isinstance(ref_or_query, DocumentReference):
            return self._client.get_all([ref_or_query])
        elif isinstance(ref_or_query, Query):
            return ref_or_query.stream()
        else:
            raise ValueError(
                'Value for argument "ref_or_query" must be a DocumentReference or a Query.'
            )

    # methods from
    # https://googleapis.dev/python/firestore/latest/batch.html#google.cloud.firestore_v1.batch.WriteBatch

    def _add_write_op(self, write_op: Callable):
        if self._read_only:
            raise ValueError(
                "Cannot perform write operation in read-only transaction."
            )
        self._write_ops.append(write_op)

    def create(self, reference: DocumentReference, document_data):
        """Create a document.
        
        Args:
            reference: The document reference.
            document_data: The document data.
            
        Raises:
            InvalidArgument: If the document exceeds the 1MB size limit.
        """
        # Check document size before creating
        doc_size = calculate_document_size(document_data)
        if doc_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
            raise InvalidArgument(
                f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes. Current size: {doc_size} bytes."
            )
        # this is a no-op, because if we have a DocumentReference
        # it's already in the MockFirestore
        ...

    def set(self, reference: DocumentReference, document_data: dict,
            merge=False):
        """Set a document.
        
        Args:
            reference: The document reference.
            document_data: The document data.
            merge: If True, fields omitted will remain unchanged.
            
        Raises:
            InvalidArgument: If the document exceeds the 1MB size limit.
        """
        # Check document size before setting
        doc_size = calculate_document_size(document_data)
        if doc_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
            raise InvalidArgument(
                f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes. Current size: {doc_size} bytes."
            )
            
        write_op = partial(reference.set, document_data, merge=merge)
        self._add_write_op(write_op)

    def update(
        self, 
        reference: DocumentReference,
        field_updates: dict, 
        option=None
    ):
        """Update a document.
        
        Args:
            reference: The document reference.
            field_updates: The fields to update and their values.
            option: If provided, restricts the update to certain field paths.
            
        Raises:
            InvalidArgument: If the document exceeds the 1MB size limit after update.
        """
        # For update operations, the size check is done inside the DocumentReference.update method
        # which is called when the transaction is committed
        write_op = partial(reference.update, field_updates)
        self._add_write_op(write_op)

    def delete(
        self, 
        reference: DocumentReference, 
        option=None
    ):
        """Delete a document.
        
        Args:
            reference: The document reference.
            option: If provided, restricts the delete to certain field paths.
            
        Returns:
            Timestamp: The time that the delete request was received by the server.
        """
        write_op = reference.delete
        self._add_write_op(write_op)

    def commit(self) -> List[WriteResult]:
        """Commit the transaction.
        
        Returns:
            List[WriteResult]: A list of write results for each write operation.
        """
        return self._commit()

    def __enter__(self):
        """Start a transaction.
        
        Returns:
            Transaction: The transaction instance.
        """
        self._begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End a transaction.
        
        If no exception was raised, commits the transaction,
        otherwise does nothing.
        """
        if exc_type is None:
            self.commit()


class Batch(Transaction):
    def commit(self):
        """Commit the batch.
        
        Returns:
            List[WriteResult]: A list of write results for each write operation.
        """
        self._begin()  # batch can call commit many times
        return super(Batch, self).commit()
