from typing import Dict, Any, Optional, Union, List, Type
from mockfirestore.collection import CollectionGroup, CollectionReference
from mockfirestore.document import DocumentSnapshot
from mockfirestore.query import Query


class AggregationQuery:
    """Mock implementation of Firestore AggregationQuery.

    This class implements aggregation operations over a Firestore query.
    """

    def __init__(self, parent_query: Union[Query, CollectionReference, CollectionGroup], alias=None):
        """Initialize the AggregationQuery.
        
        Args:
            parent_query: The parent query to perform aggregations on.
            alias: Optional alias for the aggregation result.
        """
        self._parent_query = parent_query
        self._alias = alias
        self._aggregations = []

    def add_aggregation(self, aggregation_type, field_ref=None, alias=None):
        """Add an aggregation to this query.
        
        Args:
            aggregation_type: Type of aggregation (count, sum, avg).
            field_ref: Field to aggregate over (not used for count).
            alias: Optional alias for the aggregation result.
            
        Returns:
            Self for chaining.
        """
        self._aggregations.append({
            'type': aggregation_type,
            'field_ref': field_ref,
            'alias': alias
        })
        return self

    def count(self, alias=None):
        """Add a count aggregation.
        
        Args:
            alias: Optional name of the field to store the result.
            
        Returns:
            Self for chaining.
        """
        return self.add_aggregation('count', alias=alias)

    def sum(self, field_ref, alias=None):
        """Add a sum aggregation.
        
        Args:
            field_ref: The field to sum.
            alias: Optional name of the field to store the result.
            
        Returns:
            Self for chaining.
        """
        return self.add_aggregation('sum', field_ref, alias)

    def avg(self, field_ref, alias=None):
        """Add an average aggregation.
        
        Args:
            field_ref: The field to average.
            alias: Optional name of the field to store the result.
            
        Returns:
            Self for chaining.
        """
        return self.add_aggregation('avg', field_ref, alias)

    def _compute_aggregate_count(self, docs):
        """Compute a count aggregation on the query results.
        
        Args:
            docs: The documents to count.
            
        Returns:
            The count of documents.
        """
        return len(docs)
    
    def _compute_aggregate_sum(self, docs, field_ref):
        """Compute a sum aggregation on the query results.
        
        Args:
            docs: The documents to sum over.
            field_ref: The field to sum.
            
        Returns:
            The sum of the field values.
        """
        total = 0
        for doc in docs:
            doc_dict = doc.to_dict()
            if field_ref in doc_dict and isinstance(doc_dict[field_ref], (int, float)):
                total += doc_dict[field_ref]
        return total
    
    def _compute_aggregate_avg(self, docs, field_ref):
        """Compute an average aggregation on the query results.
        
        Args:
            docs: The documents to average over.
            field_ref: The field to average.
            
        Returns:
            The average of the field values.
        """
        total = 0
        count = 0
        for doc in docs:
            doc_dict = doc.to_dict()
            if field_ref in doc_dict and isinstance(doc_dict[field_ref], (int, float)):
                total += doc_dict[field_ref]
                count += 1
        return total / count if count > 0 else 0
    
    def get(self, transaction=None, retry=None, timeout=None):
        """Execute the aggregation query and get the results.
        
        Args:
            transaction: Optional transaction to execute the query within.
            retry: Optional retry configuration.
            timeout: Optional timeout.
            
        Returns:
            A dictionary of aggregation results.
        """
        # Get all documents from the parent query
        docs = list(self._parent_query.stream(transaction=transaction))
        
        # Compute all requested aggregations
        results = {}
        for i, agg in enumerate(self._aggregations):
            agg_type = agg['type']
            alias = agg['alias'] or f'field_{i}'
            
            if agg_type == 'count':
                results[alias] = self._compute_aggregate_count(docs)
            elif agg_type == 'sum':
                results[alias] = self._compute_aggregate_sum(docs, agg['field_ref'])
            elif agg_type == 'avg':
                results[alias] = self._compute_aggregate_avg(docs, agg['field_ref'])
        
        return AggregationResult(results)


class AggregationResult:
    """Result of an aggregation query."""
    
    def __init__(self, data):
        """Initialize the aggregation result.
        
        Args:
            data: Dictionary of aggregation results.
        """
        self._data = data
    
    def __getitem__(self, key):
        """Get a specific aggregation result by key.
        
        Args:
            key: The aggregation alias.
            
        Returns:
            The aggregation value.
        """
        return self._data.get(key)
    
    def __contains__(self, key):
        """Check if an aggregation alias exists.
        
        Args:
            key: The aggregation alias.
            
        Returns:
            True if the alias exists.
        """
        return key in self._data
    
    def to_dict(self):
        """Get all aggregation results as a dictionary.
        
        Returns:
            Dictionary of all aggregation results.
        """
        return dict(self._data)
