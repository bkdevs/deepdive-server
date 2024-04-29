import json
import traceback
from abc import ABC, abstractmethod
from typing import List

from deepdive.gpt.open_ai import complete_prompt
from deepdive.gpt.prompts.writer import write_db
from deepdive.schema import DatabaseSchema, ForeignKey

NUM_QUESTIONS = 4


class GptClient(ABC):
    @abstractmethod
    def construct_query(self, question: str, example_queries: str = "") -> str:
        pass

    def generate_questions(self, schema: DatabaseSchema) -> List[str]:
        prompt = write_db(schema)
        prompt += "I want you to act as a data analyst and product questions."
        prompt += f"For the above table, return {NUM_QUESTIONS} questions to construct useful visuals to be monitored."
        prompt += "Return questions only in JSON format."
        questions = complete_prompt(prompt, model=self.model, temperature=0)
        return json.loads(questions)["questions"]

    def generate_foreign_keys(self, schema: DatabaseSchema) -> List[ForeignKey]:
        prompt = "For the given SQL database schema, identify foreign keys. Return foreign keys as JSON only\n"
        prompt += """Database schema:
Table CUSTOMER, columns = [*,C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT]
Table LINEITEM, columns = [*,L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT]
Table ORDERS, columns = [*,O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT]
foreign_keys:
{
  "CUSTOMER.C_CUSTKEY": "ORDERS.O_CUSTKEY",
  "LINEITEM.L_ORDERKEY": "ORDERS.O_ORDERKEY"
}
"""
        prompt += "\n"
        prompt += """Database schema:
Table bikeshare_stations, columns = [*,station_id,name,status,address,alternate_name,city_asset_number,property_type,number_of_docks,power_type,footprint_length,footprint_width,notes,council_district,modified_date]
Table bikeshare_trips, columns = [*,trip_id,subscriber_type,bike_id,bike_type,start_time,start_station_id,start_station_name,end_station_id,end_station_name,duration_minutes]
foreign_keys:
{
  "bikeshare_trips.start_station_id": "bikeshare_stations.station_id",
  "bikeshare_trips.end_station_id": "bikeshare_stations.staion_id"
}
"""
        prompt += "\n"
        prompt += "Database schema:\n"
        prompt += write_db(schema)
        prompt += "foreign_keys: "

        print(prompt)
        foreign_keys_json = complete_prompt(prompt, model=self.model, temperature=0)
        print("Attempted to generate foreign keys: ")
        print(foreign_keys_json)

        try:
            foreign_keys = []
            for k, v in json.loads(foreign_keys_json).items():
                if (
                    k != v
                    and self._key_is_valid(schema, k)
                    and self._key_is_valid(schema, v)
                ):
                    foreign_keys.append(ForeignKey(primary=k, reference=v))

            return self._remove_duplicates(foreign_keys)
        except Exception as e:
            print("Failed to parse foreign keys: ")
            traceback.print_exc()
            return []

    def _key_is_valid(self, schema: DatabaseSchema, key: str):
        if "." not in key:
            return False

        table_name, column_name = key.split(".")
        table = schema.get_table(table_name)
        return table and table.get_column(column_name)

    def _remove_duplicates(self, foreign_keys: List[ForeignKey]) -> List[ForeignKey]:
        foreign_key_strings = [
            ":".join(sorted([key.primary, key.reference])) for key in foreign_keys
        ]

        cleaned_foreign_keys = []
        for foreign_key_string in sorted(set(foreign_key_strings)):
            primary, reference = foreign_key_string.split(":")
            cleaned_foreign_keys.append(
                ForeignKey(primary=primary, reference=reference)
            )

        return cleaned_foreign_keys
