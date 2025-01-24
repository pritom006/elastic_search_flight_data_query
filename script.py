import json
import time
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os
import requests
import logging
from datetime import datetime
from elasticsearch_queries import ElasticsearchQueryBuilder

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

        # Initialize the ElasticsearchQueryBuilder
        self.query_builder = ElasticsearchQueryBuilder(self.es, self.index_name)

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

# class FlightDataQueries:
#     def __init__(self, es_client):
#         self.es = es_client
#         self.index_name = "kibana_sample_data_flights"

#     def format_response(self, response):
#         """Format Elasticsearch response for display"""
#         if response is None:
#             return {"error": "No response received"}
        
#         # Convert response to dict
#         response_dict = response.body
        
#         # Format the results
#         formatted_results = {
#             "total_hits": response_dict["hits"]["total"]["value"],
#             "took_ms": response_dict["took"],
#             "hits": []
#         }
        
#         # Format regular search results
#         if "hits" in response_dict and "hits" in response_dict["hits"]:
#             formatted_results["hits"] = [hit["_source"] for hit in response_dict["hits"]["hits"]]
        
#         # Format aggregations if present
#         if "aggregations" in response_dict:
#             formatted_results["aggregations"] = response_dict["aggregations"]
        
#         return formatted_results

#     def get_all_flights(self):
#         """1. Retrieve all documents"""
#         query = {
#             "query": {
#                 "match_all": {}
#             }
#         }
#         return self._execute_query(query)

#     def filter_by_carrier(self, carrier, size=10):
#         """2. Filter by carrier"""
#         query = {
#             "size": size,
#             "query": {
#                 "term": {
#                     "Carrier": carrier
#                 }
#             }
#         }
#         return self._execute_query(query)

#     def search_by_origin_city(self, city, size=10):
#         """3. Search by origin city"""
#         query = {
#             "size": size,
#             "query": {
#                 "match": {
#                     "OriginCityName": city
#                 }
#             }
#         }
#         return self._execute_query(query)

#     def filter_by_price_range(self, min_price, max_price, size=10):
#         """4. Filter by price range"""
#         query = {
#             "size": size,
#             "query": {
#                 "range": {
#                     "AvgTicketPrice": {
#                         "gte": min_price,
#                         "lte": max_price
#                     }
#                 }
#             }
#         }
#         return self._execute_query(query)

#     def avg_price_per_carrier(self):
#         """Average ticket price per carrier"""
#         query = {
#             "size": 0,
#             "aggs": {
#                 "avg_price_per_carrier": {
#                     "terms": {
#                         "field": "Carrier"
#                     },
#                     "aggs": {
#                         "avg_price": {
#                             "avg": {
#                                 "field": "AvgTicketPrice"
#                             }
#                         }
#                     }
#                 }
#             }
#         }
#         return self._execute_query(query)

#     def find_long_distance_flight(self, distance_len):
#         """Find Long-Distance Flights (>5000 km)"""
#         query = {
#             "query": {
#                 "range": {
#                     "DistanceKilometers": {
#                         "gt": distance_len
#                     }
#                 }
#             }
#         }
#         return self._execute_query(query)

#     def flight_on_specific_date(self):
#         "flight on a specific date"
#         query = {
#             "query": {
#                 "term": {
#                     "timestamp": "2025-01-06T00:00:00"                   
#                 }
#             }
#         }
#         return self._execute_query(query)

#     def flight_within_date_range(self):
#         "# Flights within a Date Range (e.g., '2022-01-01' to '2022-01-31')"
#         query = {
#             "query": {
#                 "range": {
#                     "timestamp": {
#                         "gte": "2025-01-06T00:00:00",
#                         "lte": "2025-02-06T00:00:00"
#                     }
#                 }
#             }
#         }
#         return self._execute_query(query)

#     def avg_tckt_price_by_carrier(self):
#         query = {
#             "query":{
#                 "aggs": {
#                     "avg_tckt_price_carrier": {
#                         "terms": {
#                             "field": "Carrier"
#                         },
#                         "aggs": {
#                             "avg_price": {
#                                 "avg": {
#                                     "field": "AvgTicketPrice"
#                                 }
#                             }
#                         }
#                     }
#                 }
#             }
#         }
#         return self._execute_query(query)

    
#     def total_flight_per_destinition(self):
#         query = {
#             "size": 0,
#             "aggs": {
#                 "total_flight_per_destination": {
#                     "terms": {
#                         "field": "DestCountry"
#                     }
#                 }
#             }
#         }
#         return self._execute_query(query)

    
#     def sort_by_tckt_price(self):
#         """sort by tckt price (Descending Order)"""
#         query = {
#             "sort": [
#                 {
#                     "AvgTicketPrice": {
#                         "order": "desc"
#                     }
#                 }
#             ]
#         }
#         return self._execute_query(query)
    
#     def first_ten_flight_short_duration(self):
#         """Paginate results (First ten flight with shortest duration)"""
#         query = {
#             "size": 10,
#             "sort": [
#                 {
#                     "FlightTimeMin": {
#                         "order": "asc"
#                     }
#                 }
#             ]
#         }
#         return self._execute_query(query)
    
#     def filter_by_multiple_criteria(self, carrier, origincityname, destcityname):
#         """Filter by multiple criteria (e.g.. "ES-Air", flights from "Adelaide" to " Tokoname")"""
#         query = {
#             "query": {
#                 "bool": {
#                     "must": [
#                         {"term": {"Carrier": carrier}},
#                         {"match": {"OriginCityName": origincityname}},
#                         {"match": {"DestCityName": destcityname}}
#                     ]
#                 }
#             }
#         }
#         return self._execute_query(query)

#     def exclude_cancel_flight_with_delay(self, delay_minitues):
#         "canceled flight with delay>30 minutes"
#         query = {
#             "query": {
#                 "bool": {
#                     "must_not": [
#                         {"term": {"Cancelled": True}}
#                     ],
#                     "filter": [
#                         {
#                             "range": {
#                                 "FlightDelayMin": {"gt": delay_minitues}
#                             }
#                         }
#                     ]
#                 }
#             }
#         }
#         return self._execute_query(query)
    
#     def flight_with_weather_conditon(self, origin_weather_cond, dest_weather_cond):
#         query = {
#             "query": {
#                 "bool": {
#                     "should": [
#                         {"match": {"OriginWeather": origin_weather_cond}},
#                         {"match": {"DestWeather": dest_weather_cond}}
#                     ]
#                 }
#             }
#         }
#         return self._execute_query(query)
    
#     def delay_flight_analysis(self, min_delay):
#         """avg flight delay >60min with Avg Delay"""
#         query = {
#             "size": 0,
#             "aggs": {
#                 "avg_delay": {
#                     "avg": {
#                         "field": "FlightDelayMin"
#                     }
#                 }
#             },
#             "query": {
#                 "range": {
#                     "FlightDelayMin": {
#                         "gt": min_delay
#                     }
#                 }
#             }
#         }
#         return self._execute_query(query)
    

#     def _execute_query(self, query):
#         """Execute a query and return formatted results"""
#         try:
#             response = self.es.search(index=self.index_name, body=query)
#             return self.format_response(response)
#         except Exception as e:
#             logger.error(f"Error executing query: {e}")
#             return {"error": str(e)}


class FlightDataQueries:
    def __init__(self, es_client, index_name):
        self.es_queries = ElasticsearchQueryBuilder(es_client, index_name)
        self.index_name = index_name

    def get_all_flights(self):
        return self.es_queries.build_query("match_all")

    def filter_by_carrier(self, carrier, size=10):
        return self.es_queries.build_query("term", {"field": "Carrier", "value": carrier, "size": size})

    def search_by_origin_city(self, city, size=10):
        return self.es_queries.build_query("match", {"field": "OriginCityName", "value": city, "size": size})

    def filter_by_price_range(self, min_price, max_price, size=10):
        return self.es_queries.build_query("range", {"field": "AvgTicketPrice", "gte": min_price, "lte": max_price, "size": size})

    def avg_price_per_carrier(self):
        return self.es_queries.build_query("aggs", {"aggs_type": "terms", "field": "Carrier"})

    def find_long_distance_flight(self, distance_len):
        return self.es_queries.build_query("range", {"field": "DistanceKilometers", "gte": distance_len})

    def flight_on_specific_date(self, date):
        return self.es_queries.build_query("term", {"field": "timestamp", "value": date})

    def flight_within_date_range(self, start_date, end_date):
        return self.es_queries.build_query("range", {"field": "timestamp", "gte": start_date, "lte": end_date})

    def avg_tckt_price_by_carrier(self):
        return self.es_queries.build_query("aggs", {"aggs_type": "terms", "field": "Carrier"})

    def total_flight_per_destination(self):
        return self.es_queries.build_query("aggs", {"aggs_type": "terms", "field": "DestCountry"})

    def sort_by_ticket_price(self, order="desc"):
        return self.es_queries.build_query("sort", {"field": "AvgTicketPrice", "order": order})

    def first_ten_flights_by_duration(self):
        return self.es_queries.build_query("sort", {"field": "FlightTimeMin", "order": "asc"})

    def filter_by_multiple_criteria(self, carrier, origin_city, dest_city):
        return self.es_queries.build_query("bool", {
            "must": [
                {"term": {"Carrier": carrier}},
                {"match": {"OriginCityName": origin_city}},
                {"match": {"DestCityName": dest_city}}
            ]
        })

    def exclude_canceled_flights_with_delay(self, delay_minutes):
        return self.es_queries.build_query("bool", {
            "must_not": [{"term": {"Cancelled": True}}],
            "filter": [{"range": {"FlightDelayMin": {"gt": delay_minutes}}}]
        })

    def flights_with_weather_conditions(self, origin_weather, dest_weather):
        return self.es_queries.build_query("bool", {
            "should": [
                {"match": {"OriginWeather": origin_weather}},
                {"match": {"DestWeather": dest_weather}}
            ]
        })

    def delayed_flight_analysis(self, min_delay):
        return self.es_queries.build_query("range", {
            "field": "FlightDelayMin", 
            "gte": min_delay, 
            "size": 0, 
            "aggs": True
        })

def display_results(title, results):
    """Helper function to display results in a readable format"""
    print(f"\n{title}")
    print("-" * 80)
    print(json.dumps(results, indent=2))
    print("-" * 80)

def main():
    try:
        # Initialize and setup Elasticsearch
        setup = ElasticsearchSetup()
        setup.create_index()
        
        # Initialize queries
        queries = FlightDataQueries(setup.es, setup.index_name)
        
        print("\nExecuting example queries...")
        
        # # Example 1: All flights
        # results = queries.get_all_flights()
        # display_results("1. Getting all flights (first 5):", results)
        
        # # Example 2: Flights by carrier
        # results = queries.filter_by_carrier("ES-Air", size=5)
        # display_results("2. Flights by carrier 'ES-Air' (first 5):", results)
        
        # # Example 3: Flights from specific city
        results = queries.search_by_origin_city("Adelaide", size=5)
        display_results("3. Flights from Adelaide (first 5):", results)
        
        # # Example 4: Flights in price range
        # results = queries.filter_by_price_range(200, 400, size=5)
        #display_results("4. Flights between $200-$400 (first 5):", results)
        
        # Example 5: Average price by carrier
        # results = queries.avg_price_per_carrier()
        # display_results("5. Average price per carrier:", results)

        # Example 6: Find long distance flight (>5000km)
        # long_distance = int(input())
        # results = queries.find_long_distance_flight(long_distance)
        # display_results("Find long distance flight which have greater than 4000 km", results)

        # Example 7: Flight on specific date
        #results = queries.flight_on_specific_date()
        #display_results("Fligh on specific date", results)

        # Example 8: Flight within date range
        #results = queries.flight_within_date_range()
        #display_results("Fligh on specific date", results)

        # Example 9: Total numbers of flights per destination country
        #results = queries.total_flight_per_destinition()
        #display_results("Total num of flights per Destination country", results)

        # Example 10: Sort by Ticket Price (Descending Order)
        #result = queries.sort_by_tckt_price()
        #display_results("Sort by tckt price in descending order", result)

        # Example 11: First 10 flight with shortest durations (Paginate result)
        #result = queries.first_ten_flight_short_duration()
        #display_results("First ten flight with shortest durations", result)

        # Example 12: Filter by multiple criteria
        # carrier = input()
        # origincityname = input()
        # destcityname = input()
        # result = queries.filter_by_multiple_criteria(carrier, origincityname, destcityname)
        # display_results("By ES-Air flight goes to Adelaide to Tokoname", result)

        # Example 13: Exclude Canceled Flights and Find Delays >30 Minutes
        #result = queries.exclude_cancel_flight_with_delay(40)
        #display_results("excluded canceld flighs which has >30 minutes delays", result)

        # Example 14: Flights with weather Conditions (e.g. "Sunny")
        # origin_weather_cond = input()
        # dest_weather_cond = input()
        # result = queries.flight_with_weather_conditon(origin_weather_cond, dest_weather_cond)
        # display_results("FLight with weather conditon with Origin and Destination", result)

        # Example 15; Delayed flight analysis (>60 Minutes, calculate average delay)
        #result = queries.delay_flight_analysis(40)
        #display_results("Which flight has minutes delay and calculate avg delay", result)


    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()