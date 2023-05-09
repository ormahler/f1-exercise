import json

import requests


class KsqlDbClient:

    def __init__(self, base_url):
        self.server = base_url
        self.statement_url = f"{base_url}/ksql"

    def execute_statement(self, statement):
        data = {
            "ksql": statement,
            "streamsProperties": {
                "ksql.streams.auto.offset.reset": "earliest"
            }
        }

        return requests.post(self.statement_url, json.dumps(data))
