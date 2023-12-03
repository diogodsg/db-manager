from enum import Enum
import re


class OrderDirection(Enum):
    ASC = "asc"
    DESC = "desc"


class QueryBuilder:
    def __init__(self, query: str):
        self.query = query.strip().lower()
        self.query = re.sub(r"\s+", " ", self.query).strip()
        self.query_type = "QUERY"
        if self.query.startswith("insira"):
            self.query_type = "INSERT"

        if self.query.startswith("delete"):
            self.query_type = "DELETE"

        if self.query.startswith("atualize"):
            self.query_type = "UPDATE"

        self.start_clauses()
        # generate easier to handle dicts
        if self.query_type == "QUERY":
            self.extract_clauses_query()
            self.handle_select()
            self.handle_from()
            self.handle_where()
            self.handle_order_by()
            self.handle_limit()

        if self.query_type == "INSERT":
            self.extract_clauses_insert()
            self.handle_insert_update()

        if self.query_type == "DELETE":
            self.extract_clauses_delete()
            self.handle_delete_where()

        if self.query_type == "UPDATE":
            self.extract_clauses_update()
            self.handle_insert_update()
            self.handle_where(table=self.into_clause)

    def start_clauses(self):
        self.select_clause = ""
        self.from_clause = ""
        self.where_clause = ""
        self.group_by_clause = ""
        self.having_clause = ""
        self.order_by_clause = ""
        self.limit_clause = ""
        self.insert_update_clause = ""
        self.into_clause = ""
        self.delete_clause = ""

    def extract_clauses_query(self):
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
            r"\bORDENE\s+POR\b(.*?)\b(?:LIMITE\b|$)", re.IGNORECASE | re.DOTALL
        )
        limit_regex = re.compile(r"\bLIMITE\b(.*$)", re.IGNORECASE | re.DOTALL)

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

    def extract_clauses_insert(self):
        insert_regex = re.compile(r"\bINSIRA\b(.*?)\bEM\b", re.IGNORECASE | re.DOTALL)
        into_regex = re.compile(r"\bEM\b(.*$)", re.IGNORECASE | re.DOTALL)

        insert_match = insert_regex.search(self.query)
        into_match = into_regex.search(self.query)
        if insert_match:
            self.insert_update_clause = insert_match.group(1).strip()
        if into_match:
            self.into_clause = into_match.group(1).strip()

    def extract_clauses_delete(self):
        delete_regex = re.compile(
            r"\bDELETE\s+DE\b(.*?)\bONDE\b", re.IGNORECASE | re.DOTALL
        )
        delete_where_regex = re.compile(r"\bONDE\b(.*$)", re.IGNORECASE | re.DOTALL)

        delete_match = delete_regex.search(self.query)
        delete_where_match = delete_where_regex.search(self.query)
        if delete_match:
            self.delete_clause = delete_match.group(1).strip()
        if delete_where_match:
            self.where_clause = delete_where_match.group(1).strip()

    def extract_clauses_update(self):
        update_regex = re.compile(r"\bATUALIZE\b(.*?)\bDE\b", re.IGNORECASE | re.DOTALL)
        update_table_regex = re.compile(
            r"\bDE\b\s+(\w+)\s+ONDE\b", re.IGNORECASE | re.DOTALL
        )
        update_where_regex = re.compile(r"\bONDE\b(.*$)", re.IGNORECASE | re.DOTALL)

        update_match = update_regex.search(self.query)
        update_table_match = update_table_regex.search(self.query)
        update_where_match = update_where_regex.search(self.query)

        if update_match:
            self.insert_update_clause = update_match.group(1).strip()
        if update_table_match:
            self.into_clause = update_table_match.group(1).strip()
        if update_where_match:
            self.where_clause = update_where_match.group(1).strip()

    def handle_insert_update(self):
        columns = [col.strip() for col in self.insert_update_clause.split(",")]
        row = {}
        for col in columns:
            match = re.match(r"(\w+)=(\w+)", col)
            if match:
                column = f"{self.into_clause}.{match.group(1)}"
                value = match.group(2)
                row[column] = value
        self.insert_update_spec = {"row": row, "table": self.into_clause}

    def handle_delete_where(self):
        self.handle_where(self.delete_clause)
        self.delete_spec = {"query": self.where_spec, "table": self.delete_clause}

    def handle_select(self):
        if self.select_clause == "*":
            self.select_spec = {"all": True, "cols": []}
        else:
            self.select_spec = {
                "all": False,
                "cols": [col.strip() for col in self.select_clause.split(",")],
            }

    def handle_from(self):
        pattern = r"MESCLE\s+(\w+)\s+EM\s+(\w+)\.(\w+)=(\w+)\.(\w+)"
        matches = re.findall(pattern, self.from_clause, re.IGNORECASE)

        output = {"from": self.from_clause.split(" ")[0], "joins": []}

        for match in matches:
            join_info = self._get_join_info(match)
            output["joins"].append(join_info)

        self.from_spec = output

    def _get_join_info(self, match):
        if match[0] == match[1]:
            join_col, base_col, table = (
                f"{match[1]}.{match[2]}",
                f"{match[3]}.{match[4]}",
                match[1],
            )
        else:
            join_col, base_col, table = (
                f"{match[3]}.{match[4]}",
                f"{match[1]}.{match[2]}",
                match[3],
            )

        return {"join_col": join_col, "base_col": base_col, "table": table}

    def handle_where(self, table: str = ""):
        sql_where = self.where_clause.replace("ONDE ", "").split(" e ")
        clauses = {"$and": []}
        for and_clause in sql_where:
            and_condition = {"$or": []}
            for clause in and_clause.split(" ou "):
                clause = clause.strip()
                match = re.match(r"([.\w]+)\s*(=|>|>=|<|<=)\s*([\w\s.]+)", clause)
                if match:
                    left_side, operator, value = match.groups()
                    left_side = f"{table}.{left_side}" if table else left_side
                    and_condition["$or"].append(
                        {
                            "operator": operator,
                            "left_side": left_side,
                            "value": value.strip("'"),
                        }
                    )
            clauses["$and"].append(and_condition)
        self.where_spec = clauses

    def handle_order_by(self):
        if not self.order_by_clause:
            self.order_spec = []
            return

        order_arr = [
            {
                "column": order[0],
                "direction": OrderDirection(
                    order[1]
                    if len(order) > 1 and order[1] == "desc"
                    else OrderDirection.ASC
                ),
            }
            for order in (
                order.strip().split(" ") for order in self.order_by_clause.split(",")
            )
        ]
        self.order_spec = order_arr

    def handle_limit(self):
        self.limit_spec = int(self.limit_clause) if self.limit_clause else None
