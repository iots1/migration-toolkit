from __future__ import annotations  # Enable modern type hints

import pandas as pd
from sqlalchemy import text
from repositories.connection import get_transaction
from models.migration_config import ConfigRecord


def save(record: ConfigRecord) -> tuple[bool, str]:
    """Save or update a config. Pass a ConfigRecord — no flat kwargs."""
    import json as _json

    json_str = record.json_data
    if isinstance(json_str, dict):
        json_str = _json.dumps(json_str, ensure_ascii=False)

    # Single params dict derived from ConfigRecord — the ONLY place to update
    # when a new column is added to the configs table.
    col_params: dict = {
        "table_name": record.table_name,
        "json_data": json_str,
        "datasource_source_id": record.datasource_source_id,
        "datasource_target_id": record.datasource_target_id,
        "config_type": record.config_type,
        "script": record.script or None,
        "generate_sql": record.generate_sql or None,
        "condition": record.condition or None,
        "lookup": record.lookup or None,
    }

    try:
        with get_transaction() as conn:
            result = conn.execute(
                text("SELECT id FROM configs WHERE config_name = :name"),
                {"name": record.config_name},
            )
            existing = result.fetchone()
            if existing:
                config_id = existing[0]
                conn.execute(
                    text(
                        """UPDATE configs SET
                               table_name = :table_name,
                               json_data = :json_data,
                               datasource_source_id = :datasource_source_id,
                               datasource_target_id = :datasource_target_id,
                               config_type = :config_type,
                               script = :script,
                               generate_sql = :generate_sql,
                               condition = :condition,
                               lookup = :lookup,
                               updated_at = CURRENT_TIMESTAMP
                           WHERE id = :id"""
                    ),
                    {"id": config_id, **col_params},
                )
            else:
                conn.execute(
                    text(
                        """INSERT INTO configs (
                               config_name, table_name, json_data,
                               datasource_source_id, datasource_target_id,
                               config_type, script, generate_sql, condition, lookup,
                               updated_at
                           ) VALUES (
                               :config_name, :table_name, :json_data,
                               :datasource_source_id, :datasource_target_id,
                               :config_type, :script, :generate_sql, :condition, :lookup,
                               CURRENT_TIMESTAMP
                           )"""
                    ),
                    {"config_name": record.config_name, **col_params},
                )
                result = conn.execute(
                    text("SELECT id FROM configs WHERE config_name = :name"),
                    {"name": record.config_name},
                )
                config_id = result.scalar()

            ver_result = conn.execute(
                text(
                    "SELECT COALESCE(MAX(version), 0) FROM config_histories WHERE config_id = :cid"
                ),
                {"cid": config_id},
            )
            next_version = ver_result.scalar() + 1
            conn.execute(
                text(
                    "INSERT INTO config_histories (config_id, version, json_data, created_at)"
                    " VALUES (:config_id, :version, :json_data, CURRENT_TIMESTAMP)"
                ),
                {
                    "config_id": config_id,
                    "version": next_version,
                    "json_data": json_str,
                },
            )
        return True, f"Saved config '{record.config_name}' (version {next_version})"
    except Exception as e:
        return False, f"Error: {str(e)}"


def get_list():
    with get_transaction() as conn:
        df = pd.read_sql(
            "SELECT id::text AS id, config_name, table_name, updated_at FROM configs WHERE is_deleted = false ORDER BY updated_at DESC",
            conn,
        )
        import numpy as np

        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]):
                df[col] = np.array(df[col].fillna("").tolist(), dtype=object)
        return df


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM configs WHERE is_deleted = false AND deleted_at IS NULL"
            )
        )
        return result.scalar()


def get_all_list() -> list[dict]:
    """Get all configs as a list of dicts."""
    import json as _json

    with get_transaction() as conn:
        result = conn.execute(
            text(
                """SELECT id::text AS id, config_name, table_name, json_data,
                   datasource_source_id::text, datasource_target_id::text,
                   config_type, script, generate_sql, condition, lookup,
                   created_at, created_by, updated_at, updated_by,
                   is_deleted, deleted_at, deleted_by, deleted_reason
                   FROM configs WHERE is_deleted = false ORDER BY updated_at DESC"""
            )
        )
        rows = []
        for row in result.fetchall():
            data = dict(zip(result.keys(), row))
            raw = data.get("json_data")
            try:
                data["json_data"] = (
                    _json.loads(raw) if isinstance(raw, str) else (raw or {})
                )
            except (_json.JSONDecodeError, TypeError):
                data["json_data"] = {}
            rows.append(data)
        return rows


def get_content(config_name: str):
    import json as _json

    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT * FROM configs WHERE config_name = :name"),
            {"name": config_name},
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        data = dict(zip(columns, row))
        data["id"] = str(data["id"])

        # Parse json_data string into dict and merge with DB metadata
        raw_json = data.get("json_data", "{}")
        try:
            parsed = _json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        except (_json.JSONDecodeError, TypeError):
            parsed = {}

        # Merge: parsed JSON takes priority; preserve DB metadata keys
        merged = {**parsed}
        merged.setdefault("config_name", data.get("config_name", ""))
        merged.setdefault("name", data.get("config_name", ""))
        merged["_db_id"] = data["id"]
        merged["_db_updated_at"] = str(data.get("updated_at", ""))
        merged["condition"] = data.get("condition") or parsed.get("condition", "")
        merged["lookup"] = data.get("lookup") or parsed.get("lookup", "")
        merged["config_type"] = data.get("config_type") or parsed.get(
            "config_type", "std"
        )
        merged["script"] = data.get("script") or parsed.get("script", "")
        merged["generate_sql"] = data.get("generate_sql") or parsed.get(
            "generate_sql", ""
        )
        ds_src_id = data.get("datasource_source_id")
        merged["_datasource_source_id"] = str(ds_src_id) if ds_src_id else None
        ds_tgt_id = data.get("datasource_target_id")
        merged["_datasource_target_id"] = str(ds_tgt_id) if ds_tgt_id else None
        return merged


def get_by_id_raw(config_id: str) -> dict | None:
    """Get config by UUID — returns clean DB row with json_data parsed to dict."""
    import json as _json

    with get_transaction() as conn:
        result = conn.execute(
            text(
                """SELECT id::text AS id, config_name, table_name, json_data,
                   datasource_source_id::text, datasource_target_id::text,
                   config_type, script, generate_sql, condition, lookup,
                   created_at, created_by, updated_at, updated_by,
                   is_deleted, deleted_at, deleted_by, deleted_reason
                   FROM configs WHERE id = :id"""
            ),
            {"id": config_id},
        )
        row = result.fetchone()
        if row is None:
            return None
        data = dict(zip(result.keys(), row))
        raw = data.get("json_data")
        try:
            data["json_data"] = (
                _json.loads(raw) if isinstance(raw, str) else (raw or {})
            )
        except (_json.JSONDecodeError, TypeError):
            data["json_data"] = {}
        return data


def delete(config_name: str):
    try:
        with get_transaction() as conn:
            result = conn.execute(
                text("DELETE FROM configs WHERE config_name = :name"),
                {"name": config_name},
            )
            if result.rowcount == 0:
                return False, f"Config '{config_name}' not found"
        return True, f"Deleted config '{config_name}'"
    except Exception as e:
        return False, f"Error: {str(e)}"


def get_history(config_name: str):
    with get_transaction() as conn:
        return pd.read_sql(
            text(
                "SELECT ch.version, ch.json_data, ch.created_at FROM config_histories ch JOIN configs c ON ch.config_id = c.id WHERE c.config_name = :name ORDER BY ch.version DESC"
            ),
            conn,
            params={"name": config_name},
        )


def get_version(config_name: str, version: int):
    with get_transaction() as conn:
        result = conn.execute(
            text(
                "SELECT ch.version, ch.json_data, ch.created_at FROM config_histories ch JOIN configs c ON ch.config_id = c.id WHERE c.config_name = :name AND ch.version = :version"
            ),
            {"name": config_name, "version": version},
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        return dict(zip(columns, row))


def compare_versions(config_name: str, v1: int, v2: int):
    import json

    version1 = get_version(config_name, v1)
    version2 = get_version(config_name, v2)
    if version1 is None or version2 is None:
        return None
    try:
        data1 = json.loads(version1["json_data"])
        data2 = json.loads(version2["json_data"])
        same = data1 == data2
    except json.JSONDecodeError:
        same = version1["json_data"] == version2["json_data"]
    return {"config_name": config_name, "v1": version1, "v2": version2, "same": same}
