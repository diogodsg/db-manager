from tabulate import tabulate
import os
import pandas as pd
import json
import re
import operator


class QueryBuilder:
    def __init__(self, query: str):
        self.query = query.strip().lower()
        self.start_clauses()
        self.extract_clauses()

        # generate easier to handle dicts
        self.handle_select()
        self.handle_from()
        self.handle_where()
        self.handle_order_by()
        self.handle_limit()

    def start_clauses(self):
        self.select_clause = ""
        self.from_clause = ""
        self.where_clause = ""
        self.group_by_clause = ""
        self.having_clause = ""
        self.order_by_clause = ""
        self.limit_clause = ""

    def extract_clauses(self):
        self.query = re.sub(r"\s+", " ", self.query).strip()
        select_regex = re.compile(
            r"\bSELECIONE\b(.*?)\bDE\b", re.IGNORECASE | re.DOTALL
        )
        from_regex = re.compile(
            r"\bDE\b(.*?)\b(?:ONDE\b|ORDENE\s+POR\b|LIMITE\s+EM\b|$)",
            re.IGNORECASE | re.DOTALL,
        )
        where_regex = re.compile(
            r"\bONDE\b(.*?)\s*(?:ORDENE\s+POR|LIMITE\s+EM|$)",
            re.IGNORECASE | re.DOTALL,
        )

        order_by_regex = re.compile(
            r"\bORDER BY\b(.*?)\b(?:LIMIT\b|$)", re.IGNORECASE | re.DOTALL
        )
        limit_regex = re.compile(r"\bLIMIT\b(.*$)", re.IGNORECASE | re.DOTALL)

        select_match = select_regex.search(self.query)
        from_match = from_regex.search(self.query)
        where_match = where_regex.search(self.query)
        order_by_match = order_by_regex.search(self.query)
        limit_match = limit_regex.search(self.query)

        if select_match:
            self.select_clause = select_match.group(1).strip()
        if from_match:
            self.from_clause = from_match.group(1).strip()
        if where_match:
            self.where_clause = where_match.group(1).strip()

        if order_by_match:
            self.order_by_clause = order_by_match.group(1).strip()
        if limit_match:
            self.limit_clause = limit_match.group(1).strip()

    def handle_select(self):
        if self.select_clause == "*":
            self.select_spec = {"all": True, "cols": []}
            return

        self.select_spec = {
            "all": False,
            "cols": [col.strip() for col in self.select_clause.split(",")],
        }

    def handle_from(self):
        pattern = r"MESCLE\s+(\w+)\s+EM\s+(\w+)\.(\w+)=(\w+)\.(\w+)"

        matches = re.findall(pattern, self.from_clause, re.IGNORECASE)

        output = {
            "from": self.from_clause.split(" ")[0],
            "joins": [],
        }

        for match in matches:
            if match[0] == match[1]:
                join_info = {
                    "join_col": f"{match[1]}.{match[2]}",
                    "base_col": f"{match[3]}.{match[4]}",
                }
                join_info["table"] = match[1]
            else:
                join_info = {
                    "join_col": f"{match[3]}.{match[4]}",
                    "base_col": f"{match[1]}.{match[2]}",
                }
                join_info["table"] = match[3]

            output["joins"].append(join_info)

        self.from_spec = output

    def handle_where(self):
        sql_where = self.where_clause.replace("ONDE ", "").split(" e ")
        clauses = {"$and": []}
        for and_clause in sql_where:
            and_condition = {}
            or_clause = and_clause.split(" ou ")
            and_condition["$or"] = []
            for clause in or_clause:
                clause = clause.strip()
                pattern = r"([.\w]+)\s*(=|>|>=|<|<=)\s*([\w\s.]+)"
                match = re.match(pattern, clause)
                if match:
                    left_side = match.group(1)
                    operator = match.group(2)
                    value = match.group(3).strip("'")
                    and_condition["$or"].append(
                        {"operator": operator, "left_side": left_side, "value": value}
                    )
            clauses["$and"].append(and_condition)
        self.where_spec = clauses

    def handle_order_by(self):
        if not self.order_by_clause:
            self.order_spec = []
            return
        orders = self.order_by_clause.split(",")
        order_arr = []
        for order in orders:
            order = order.strip().split(" ")
            order_spec = {"column": order[0]}
            order_spec["direction"] = "asc"

            if len(order) > 1 and order[1] == "desc":
                order_spec["direction"] = "desc"

            order_arr.append(order_spec)

        self.order_spec = order_arr

    def handle_limit(self):
        self.limit_spec = int(self.limit_clause) if self.limit_clause else None


class QueryExecutor:
    def __init__(self, directory: str, query):
        self.query_builder = QueryBuilder(query)
        self.directory = directory
        self.data = []

    def execute(self):
        self._handle_from()
        self._handle_where()
        self._handle_select()
        self._handle_order_by()
        self._handle_limit()
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

    def _handle_where(self):
        spec = self.query_builder.where_spec

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

        self.data = [row for row in self.data if met_conditions(row)]

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
