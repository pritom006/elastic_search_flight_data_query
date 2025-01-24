import json
import logging
from elasticsearch import Elasticsearch

class ElasticsearchQueryBuilder:
    def __init__(self, es_client, index_name):
        """
        Initialize the query builder with Elasticsearch client and index name
        
        :param es_client: Elasticsearch client
        :param index_name: Name of the index to query
        """
        self.es = es_client
        self.index_name = index_name
        self.logger = logging.getLogger(__name__)

    def build_query(self, query_type, params=None):
        """
        Build and execute Elasticsearch queries dynamically
        
        :param query_type: Type of query to build
        :param params: Dictionary of parameters for the query
        :return: Formatted query results
        """
        query_strategies = {
            # Match All Query
            "match_all": lambda size=10: {
                "size": size,
                "query": {"match_all": {}}
            },
            
            # Term Query (Exact Match)
            "term": lambda field, value, size=10: {
                "size": size,
                "query": {
                    "term": {
                        field: value
                    }
                }
            },
            
            # Match Query (Text/Partial Match)
            "match": lambda field, value, size=10: {
                "size": size,
                "query": {
                    "match": {
                        field: value
                    }
                }
            },
            
            # Range Query
            "range": lambda field, gte=None, lte=None, size=10: {
                "size": size,
                "query": {
                    "range": {
                        field: {
                            k: v for k, v in {
                                "gte": gte,
                                "lte": lte
                            }.items() if v is not None
                        }
                    }
                }
            },
            
            # Bool Query (Complex Boolean Logic)
            "bool": lambda must=None, must_not=None, should=None, filter=None, size=10: {
                "size": size,
                "query": {
                    "bool": {
                        key: value for key, value in {
                            "must": must,
                            "must_not": must_not,
                            "should": should,
                            "filter": filter
                        }.items() if value is not None
                    }
                }
            },
            
            # Aggregation Queries
            "aggs": lambda aggs_type, field, size=0: {
                "size": size,
                "aggs": {
                    f"{aggs_type}_on_{field}": {
                        "terms": {"field": field} if aggs_type == "terms" else {},
                        "aggs": {
                            "avg_value": {"avg": {"field": field}} if aggs_type == "avg" else {}
                        }
                    }
                }
            },
            
            # Sorting Query
            "sort": lambda field, order="desc", size=10: {
                "size": size,
                "sort": [
                    {field: {"order": order}}
                ]
            }
        }
        
        # Validate query type
        if query_type not in query_strategies:
            raise ValueError(f"Unsupported query type: {query_type}")
        
        # Build query based on strategy
        try:
            if params:
                # Extract size, defaulting to 10 if not provided
                size = params.pop('size', 10)
                params['size'] = size
                query = query_strategies[query_type](**params)
            else:
                query = query_strategies[query_type]()
            
            # Execute query
            response = self.es.search(index=self.index_name, body=query)
            return self._format_response(response)
        except Exception as e:
            self.logger.error(f"Query execution error: {e}")
            return {"error": str(e)}
    
    def _format_response(self, response):
        """
        Format Elasticsearch response
        
        :param response: Raw Elasticsearch response
        :return: Formatted response dictionary
        """
        response_dict = response.body
        
        formatted_results = {
            "total_hits": response_dict["hits"]["total"]["value"],
            "took_ms": response_dict["took"],
            "hits": []
        }
        
        if "hits" in response_dict["hits"]:
            formatted_results["hits"] = [hit["_source"] for hit in response_dict["hits"]["hits"]]
        
        if "aggregations" in response_dict:
            formatted_results["aggregations"] = response_dict["aggregations"]
        
        return formatted_results