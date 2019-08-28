"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"
KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KEY = 'station_id',
    VALUE_FORMAT = 'avro',
    KAFKA_TOPIC = 'cta.station.turnstile'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='JSON') AS
    SELECT 
        station_id,COUNT(station_id) AS count 
    FROM 
        turnstile
    GROUP BY  
        station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")
    url_str = "http://localhost:8088/ksql"
    # url_str = "http://ksql:8088/ksql"


    resp = requests.post(url_str,
                         headers={
                             "Content-Type": "application/vnd.ksql.v1+json"},
                         data=json.dumps(
                             {
                                 "ksql": KSQL_STATEMENT,
                                 "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
                             }
                         ),
                         )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()

