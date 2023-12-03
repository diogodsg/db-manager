from tabulate import tabulate
import os
import pandas as pd
import json
import operator
from modules.query_builder import QueryBuilder


class QueryExecutor:
    def __init__(self, directory: str, query):
        self.query_builder = QueryBuilder(query)
        self.directory = directory
        self.data = []

    def execute(self):
        if self.query_builder.query_type == "QUERY":
            self._handle_from()
            self.data = self._handle_where()
            self._handle_select()
            self._handle_order_by()
            self._handle_limit()
        if self.query_builder.query_type == "INSERT":
            self._handle_insert()
        if self.query_builder.query_type == "DELETE":
            self._handle_delete()
        if self.query_builder.query_type == "UPDATE":
            self._handle_update()
        self.print_table()

    def print_table(self, data=None):
        if not data:
            data = self.data
        table = tabulate(data, headers="keys", tablefmt="psql")
        print(table)

    def _handle_select(self):
        spec = self.query_builder.select_spec
        if not spec["all"]:
            self.data = [
                {col: entry[col] for col in spec["cols"]} for entry in self.data
            ]

    def _get_file_map(self):
        return {file.lower().split(".")[0]: file for file in os.listdir(self.directory)}

    def _handle_insert(self):
        file_map = self._get_file_map()
        spec = self.query_builder.insert_update_spec
        table_data = self.load_table(file_map, spec["table"])
        table_data.append(spec["row"])
        self.save_table(file_map, spec["table"], table_data)

    def _handle_update(self):
        file_map = self._get_file_map()
        spec = self.query_builder.insert_update_spec
        table_data = self.load_table(file_map, spec["table"])
        to_update = self._handle_where(
            spec=self.query_builder.where_spec, data=table_data, reverse=False
        )

        for row in table_data:
            for update_row in to_update:
                if update_row == row:
                    row.update(spec["row"])

        self.save_table(file_map, spec["table"], table_data)

    def _handle_delete(self):
        file_map = {
            file.lower().split(".")[0]: file for file in os.listdir(self.directory)
        }
        spec = self.query_builder.delete_spec
        original_table = self.load_table(file_map, spec["table"])
        self.data = self._handle_where(
            spec=spec["query"], data=original_table, reverse=True
        )

        self.save_table(file_map, spec["table"], self.data)

    def _handle_from(self):
        file_map = {
            file.lower().split(".")[0]: file for file in os.listdir(self.directory)
        }
        spec = self.query_builder.from_spec
        self.data = self.load_table(file_map, spec["from"])

        for join in spec["joins"]:
            join_table = self.load_table(file_map, join["table"])
            lookup_table = {item[join["join_col"]]: item for item in join_table}
            self.data = [
                {**item1, **lookup_table[item1[join["base_col"]]]}
                for item1 in self.data
                if item1[join["base_col"]] in lookup_table
            ]

    def _handle_where(self, spec=None, data=None, reverse=False):
        if not spec:
            spec = self.query_builder.where_spec
        if not data:
            data = self.data

        def handle_condition(row, condition):
            left_side = row[condition["left_side"]]
            value = condition["value"]
            if isinstance(left_side, float) or isinstance(left_side, int):
                left_side = int(left_side)
                value = int(value)
            if isinstance(left_side, str):
                left_side = left_side.lower()

            comparison_operator = condition["operator"]

            operators = {
                "=": operator.eq,
                ">": operator.gt,
                ">=": operator.ge,
                "<": operator.lt,
                "<=": operator.le,
            }

            if comparison_operator in operators:
                return operators[comparison_operator](left_side, value)

            return True

        def met_conditions(row):
            for and_condition in spec["$and"]:
                condition_met = False if len(and_condition["$or"]) else True
                for or_condition in and_condition["$or"]:
                    if handle_condition(row, or_condition):
                        condition_met = True

                if not condition_met:
                    return False
            return True

        if reverse:
            return [row for row in data if not met_conditions(row)]
        return [row for row in data if met_conditions(row)]

    def _handle_order_by(self):
        spec = self.query_builder.order_spec
        for item in spec[::-1]:
            self.data = sorted(
                self.data,
                key=lambda x: x[item["column"]],
                reverse=item["direction"] == "asc",
            )

    def _handle_limit(self):
        self.data = self.data[: self.query_builder.limit_spec]

    def load_table(self, file_map: str, table: str):
        df = pd.read_csv(os.path.join(self.directory, file_map[table]))
        df.columns = df.columns.str.lower().map(lambda x: f"{table}.{x}")
        return json.loads(df.to_json(orient="records"))

    def save_table(self, file_map: str, table: str, data: list):
        df = pd.DataFrame.from_records(data)
        df.columns = df.columns.str.lower().map(lambda x: x.replace(f"{table}.", ""))
        df.to_csv(os.path.join(self.directory, file_map[table]), index=False)


# QueryExecutor(
#     "./database",
#     """
#     selecione student.id, takes.year, student.name, takes.grade de student mescle takes em student.id=takes.id onde takes.year=2017 ou takes.year=2018 ordene por takes.year limite 5
# """,
# ).execute()
