import json
import time
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os
import requests
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElasticsearchSetup:
    def __init__(self):
        load_dotenv()
        
        # Elasticsearch configuration
        self.es_url = os.getenv('ES_LOCAL_URL', 'http://localhost:9200')
        self.es_password = os.getenv('ES_LOCAL_PASSWORD')
        self.index_name = "kibana_sample_data_flights"
        
        # Initialize Elasticsearch client
        self.es = Elasticsearch(
            self.es_url,
            basic_auth=("elastic", self.es_password),
            verify_certs=False
        )
        
        # Wait for Elasticsearch to be ready
        self._wait_for_elasticsearch()

    def _wait_for_elasticsearch(self, timeout=300):
        """Wait for Elasticsearch to become available"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                if self.es.ping():
                    logger.info("Successfully connected to Elasticsearch")
                    return True
                logger.info("Waiting for Elasticsearch...")
                time.sleep(5)
            except Exception as e:
                logger.warning(f"Connection failed: {e}")
                time.sleep(5)
        raise TimeoutError("Elasticsearch did not become available in time")

    def create_index(self):
        """Create the flights index with appropriate mappings"""
        mappings = {
            "mappings": {
                "properties": {
                    "FlightNum": {"type": "keyword"},
                    "DestCountry": {"type": "keyword"},
                    "OriginWeather": {"type": "keyword"},
                    "OriginCityName": {"type": "keyword"},
                    "AvgTicketPrice": {"type": "float"},
                    "DistanceMiles": {"type": "float"},
                    "FlightDelay": {"type": "boolean"},
                    "DestWeather": {"type": "keyword"},
                    "Dest": {"type": "keyword"},
                    "FlightDelayType": {"type": "keyword"},
                    "OriginCountry": {"type": "keyword"},
                    "dayOfWeek": {"type": "integer"},
                    "DistanceKilometers": {"type": "float"},
                    "timestamp": {"type": "date"},
                    "DestLocation": {"type": "geo_point"},
                    "DestAirportID": {"type": "keyword"},
                    "Carrier": {"type": "keyword"},
                    "Cancelled": {"type": "boolean"},
                    "FlightTimeMin": {"type": "float"},
                    "Origin": {"type": "keyword"},
                    "OriginLocation": {"type": "geo_point"},
                    "DestRegion": {"type": "keyword"},
                    "OriginAirportID": {"type": "keyword"},
                    "OriginRegion": {"type": "keyword"},
                    "DestCityName": {"type": "keyword"},
                    "FlightTimeHour": {"type": "float"},
                    "FlightDelayMin": {"type": "integer"}
                }
            }
        }
        
        try:
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name, body=mappings)
                logger.info(f"Created index: {self.index_name}")
            else:
                logger.info(f"Index {self.index_name} already exists")
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            raise

class FlightDataQueries:
    def __init__(self, es_client):
        self.es = es_client
        self.index_name = "kibana_sample_data_flights"

    def format_response(self, response):
        """Format Elasticsearch response for display"""
        if response is None:
            return {"error": "No response received"}
        
        # Convert response to dict
        response_dict = response.body
        
        # Format the results
        formatted_results = {
            "total_hits": response_dict["hits"]["total"]["value"],
            "took_ms": response_dict["took"],
            "hits": []
        }
        
        # Format regular search results
        if "hits" in response_dict and "hits" in response_dict["hits"]:
            formatted_results["hits"] = [hit["_source"] for hit in response_dict["hits"]["hits"]]
        
        # Format aggregations if present
        if "aggregations" in response_dict:
            formatted_results["aggregations"] = response_dict["aggregations"]
        
        return formatted_results



    def get_all_flights(self):
        query = {
            "query" : {
                "match_all": {}
            }
        }
    
    def filter_by_carrier(self):
        query = {
            "query": {
                "term": {
                    "Carrier": carrier
                }
            }
        }
    
    def search_origin_city(self):
        query = {
            "query": {
                "match": {
                    "OriginCityName": city
                }
            }
        }

    
    def filter_by_price_range(self):
        query = {
            "query": {
                "range": {
                    "AvgTicketPrice": {
                        "gte": min_price,
                        "lte": max_price
                    }
                }
            }
        }

    def avg_price_per_carrier(self):
        query = {
            "query": {
                "aggs": {
                    "avg_price_carrier": {
                        "terms": {
                            "field": "Carrier"
                        },
                        "aggs": {
                            "avg_price": {
                                "avg": {
                                    "field": "AvgTicketPrice"
                                }
                            }
                        }
                    }
                }
            }
        }

    def find_long_distance(self):
        query = {
            "query": {
                "range": {
                    "DistanceKilometers": {
                        "gt": 5000
                    }
                }
            }
        }
    
    def flight_on_specific_date(self):
        query = {
            "query": {
                "term": {
                    "timestamp": "2025-01-06T00:00:00"
                }
            }
        }
    
    def flight_with_date_range(self):
        query = {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": "2025-01-06T00:00:00",
                        "lte": "2025-02-06T00:00:00"
                    }
                }
            }
        }