"""
Datasource — domain model for a database connection profile.

Parsed from the `datasources` table in SQLite.
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Datasource:
    id: int
    name: str
    db_type: str
    host: str
    port: int
    dbname: str
    username: str
    password: str

    @classmethod
    def from_dict(cls, d: dict) -> "Datasource":
        return cls(
            id=d.get("id", 0),
            name=d.get("name", ""),
            db_type=d.get("db_type", ""),
            host=d.get("host", ""),
            port=int(d.get("port", 0)),
            dbname=d.get("dbname", ""),
            username=d.get("username", ""),
            password=d.get("password", ""),
        )
