"""
Datasource — domain models for a database connection profile.

Datasource:       read model (from DB row, includes id).
DatasourceRecord: write model — single source of truth for datasources table columns.
                  Pass to datasource_repo.save() / update() instead of flat kwargs.
                  Adding a new column = add the field here only.
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
    charset: str | None = None

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
            charset=d.get("charset"),
        )


@dataclass
class DatasourceRecord:
    """
    Single source of truth for writable datasources table columns.

    Used for both INSERT (save) and UPDATE — the repo receives this object
    instead of 7 separate keyword arguments. Adding a new column means
    adding a field here and updating the INSERT/UPDATE SQL once each.
    """

    name: str
    db_type: str
    host: str = ""
    port: str = ""
    dbname: str = ""
    username: str = ""
    password: str = ""
    charset: str | None = None
