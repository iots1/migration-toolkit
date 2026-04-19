"""
SQL Query Builder — mirrors TypeOrmQueryBuilder concept.

Transforms REST API query parameters (filter, s, sort, fields, exclude_ids)
into Python in-memory filtering/sorting/pagination operations.

Operators:
  Filter: $eq, $ne, $gt, $lt, $gte, $lte, $cont, $starts, $ends, $in, $notin,
          $isnull, $notnull, $between
  Search: $eq, $ne, $gt, $gte, $lt, $lte, $like, $in, $between, !=, >, >=, <, <=
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from api.base.query_params import QueryParams


@dataclass
class ParsedFilter:
    field: str
    operator: str
    value: str


FILTER_OPERATORS = {
    "$eq",
    "$ne",
    "$gt",
    "$lt",
    "$gte",
    "$lte",
    "$cont",
    "$starts",
    "$ends",
    "$in",
    "$notin",
    "$isnull",
    "$notnull",
    "$between",
}

SEARCH_OPERATORS = {
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$like",
    "$contains",
    "$in",
    "$between",
    ">",
    ">=",
    "<",
    "<=",
    "!=",
}


class SqlQueryBuilder:
    @staticmethod
    def build(params: QueryParams) -> SqlQueryBuilder:
        return SqlQueryBuilder(params)

    def __init__(self, params: QueryParams):
        self.params = params

    def apply(self, data: list[dict]) -> list[dict]:
        result = data

        if self.params.filter:
            result = self._apply_filters(result, self.params.filter, combine="and")

        if self.params.or_:
            or_ids = self._match_ids(result, self.params.or_)
            result = [item for item in result if id(item) in or_ids]

        if self.params.s:
            result = self._apply_search(result, self.params.s)

        if self.params.sort:
            result = self._apply_sort(result, self.params.sort)

        if self.params.fields:
            field_set = {f.strip() for f in self.params.fields.split(",")}
            result = [
                {k: v for k, v in item.items() if k in field_set} for item in result
            ]

        if self.params.exclude_ids:
            exclude_set = {eid.strip() for eid in self.params.exclude_ids.split(",")}
            result = [
                item for item in result if str(item.get("id", "")) not in exclude_set
            ]

        return result

    @staticmethod
    def parse_filter_string(raw: str) -> ParsedFilter | None:
        parts = raw.split("||")
        if len(parts) < 3:
            return None
        field = parts[0].strip()
        operator = parts[1].strip()
        if not field or not operator:
            return None
        value = "||".join(parts[2:])
        return ParsedFilter(field=field, operator=operator, value=value)

    @staticmethod
    def _apply_filters(
        data: list[dict],
        filters: list[str],
        combine: str = "and",
    ) -> list[dict]:
        result = list(data)

        for f in filters:
            parsed = SqlQueryBuilder.parse_filter_string(f)
            if parsed is None:
                continue
            result = SqlQueryBuilder._apply_single_filter(result, parsed)

        return result

    @staticmethod
    def _apply_single_filter(data: list[dict], pf: ParsedFilter) -> list[dict]:
        filtered = []
        for item in data:
            if SqlQueryBuilder._match_filter(item, pf):
                filtered.append(item)
        return filtered

    @staticmethod
    def _match_filter(item: dict, pf: ParsedFilter) -> bool:
        item_val = item.get(pf.field)

        if pf.operator == "$isnull":
            return (
                item_val is None
                if pf.value.lower() != "false"
                else item_val is not None
            )
        if pf.operator == "$notnull":
            return (
                item_val is not None
                if pf.value.lower() != "false"
                else item_val is None
            )

        if item_val is None:
            return False

        str_val = str(item_val)
        value = pf.value

        if pf.operator == "$eq":
            return str_val == value
        elif pf.operator == "$ne":
            return str_val != value
        elif pf.operator == "$gt":
            return str_val > value
        elif pf.operator == "$lt":
            return str_val < value
        elif pf.operator == "$gte":
            return str_val >= value
        elif pf.operator == "$lte":
            return str_val <= value
        elif pf.operator == "$cont":
            return value.lower() in str_val.lower()
        elif pf.operator == "$starts":
            return str_val.lower().startswith(value.lower())
        elif pf.operator == "$ends":
            return str_val.lower().endswith(value.lower())
        elif pf.operator == "$in":
            values = {v.strip() for v in value.split(",")}
            return str_val in values
        elif pf.operator == "$notin":
            values = {v.strip() for v in value.split(",")}
            return str_val not in values
        elif pf.operator == "$between":
            parts = [v.strip() for v in value.split(",")]
            if len(parts) != 2:
                return False
            return parts[0] <= str_val <= parts[1]

        return False

    @staticmethod
    def _match_ids(data: list[dict], or_filters: list[str]) -> set[int]:
        ids: set[int] = set()
        for f in or_filters:
            parsed = SqlQueryBuilder.parse_filter_string(f)
            if parsed is None:
                continue
            for item in data:
                if SqlQueryBuilder._match_filter(item, parsed):
                    ids.add(id(item))
        return ids

    @staticmethod
    def _apply_search(data: list[dict], search_json: str) -> list[dict]:
        try:
            conditions = json.loads(search_json)
            if not isinstance(conditions, dict):
                return data
        except (json.JSONDecodeError, TypeError):
            return data

        result = []
        for item in data:
            if SqlQueryBuilder._match_search(item, conditions):
                result.append(item)
        return result

    @staticmethod
    def _match_search(item: dict, conditions: dict) -> bool:
        for field, condition in conditions.items():
            item_val = item.get(field)
            if item_val is None:
                return False

            str_val = str(item_val)

            if isinstance(condition, dict):
                for op, val in condition.items():
                    val_str = str(val)
                    if op == "$eq" and str_val != val_str:
                        return False
                    elif op == "$ne" and str_val == val_str:
                        return False
                    elif op == "$gt" and not (str_val > val_str):
                        return False
                    elif op == "$gte" and not (str_val >= val_str):
                        return False
                    elif op == "$lt" and not (str_val < val_str):
                        return False
                    elif op == "$lte" and not (str_val <= val_str):
                        return False
                    elif (
                        op in ("$like", "$contains")
                        and val_str.lower() not in str_val.lower()
                    ):
                        return False
                    elif op == "$in":
                        values = {
                            str(v) for v in (val if isinstance(val, list) else [val])
                        }
                        if str_val not in values:
                            return False
                    elif op == "$between":
                        parts = val if isinstance(val, list) else str(val).split(",")
                        if len(parts) != 2 or not (
                            str(parts[0]) <= str_val <= str(parts[1])
                        ):
                            return False
                    elif op == ">" and not (str_val > val_str):
                        return False
                    elif op == ">=" and not (str_val >= val_str):
                        return False
                    elif op == "<" and not (str_val < val_str):
                        return False
                    elif op == "<=" and not (str_val <= val_str):
                        return False
                    elif op == "!=" and str_val == val_str:
                        return False
            else:
                if str(condition).lower() not in str_val.lower():
                    return False

        return True

    @staticmethod
    def _apply_sort(data: list[dict], sort_str: str) -> list[dict]:
        sort_fields = []
        for part in sort_str.split(","):
            part = part.strip()
            if ":" in part:
                field, direction = part.rsplit(":", 1)
            else:
                field, direction = part, "asc"
            sort_fields.append((field.strip(), direction.strip().lower() == "asc"))

        def sort_key(item):
            keys = []
            for field_name, ascending in sort_fields:
                val = item.get(field_name, "")
                if val is None:
                    val = ""
                keys.append((str(val), ascending))
            return keys

        return sorted(data, key=sort_key)

    @staticmethod
    def paginate(data: list[dict], params: QueryParams) -> tuple[list[dict], int, int]:
        total = len(data)
        if params.ignore_limit:
            return data, total, 1

        limit = params.limit
        page = params.page
        total_pages = max(1, -(-total // limit)) if total > 0 else 1
        offset = params.offset if params.offset is not None else (page - 1) * limit

        page_data = data[offset : offset + limit]
        return page_data, total, total_pages
