from flask import Flask, request, jsonify
from script import FlightDataQueries, ElasticsearchSetup

app = Flask(__name__)

# Initialize Elasticsearch setup
setup = ElasticsearchSetup()
flight_queries = FlightDataQueries(setup.es)

@app.route('/flights/delay-analysis', methods=['GET'])
def flight_delay_analysis():
    try:
        # Get query parameters with default values
        min_delay = int(request.args.get('min_delay', 40))
        
        # Call the existing delay_flight_analysis() method
        results = flight_queries.delay_flight_analysis(min_delay)
        
        return jsonify(results)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
