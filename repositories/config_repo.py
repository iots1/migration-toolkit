"""Config repository - CRUD operations for configs table."""
from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import text

from repositories.connection import get_transaction
from repositories.utils import row_to_dict, rows_to_dicts, parse_json_field
from models.migration_config import ConfigRecord


def save(record: ConfigRecord, config_id: str | None = None) -> tuple[bool, str]:
    """Save or update a config. Pass config_id to update by UUID (allows renaming)."""
    json_str = record.json_data
    if isinstance(json_str, dict):
        json_str = json.dumps(json_str, ensure_ascii=False)

    col_params: dict = {
        "config_name": record.config_name,
        "table_name": record.table_name,
        "json_data": json_str,
        "datasource_source_id": record.datasource_source_id,
        "datasource_target_id": record.datasource_target_id,
        "config_type": record.config_type,
        "script": record.script or None,
        "generate_sql": record.generate_sql or None,
        "condition": record.condition or None,
        "lookup": record.lookup or None,
        "pk_columns": record.pk_columns or None,
    }

    try:
        with get_transaction() as conn:
            if config_id:
                result = conn.execute(
                    text("SELECT id FROM configs WHERE id = :id"),
                    {"id": config_id},
                )
            else:
                result = conn.execute(
                    text("SELECT id FROM configs WHERE config_name = :name"),
                    {"name": record.config_name},
                )
            existing = result.fetchone()

            if existing:
                config_id = existing[0]
                conn.execute(
                    text("""
                        UPDATE configs SET
                            config_name = :config_name,
                            table_name = :table_name,
                            json_data = :json_data,
                            datasource_source_id = :datasource_source_id,
                            datasource_target_id = :datasource_target_id,
                            config_type = :config_type,
                            script = :script,
                            generate_sql = :generate_sql,
                            condition = :condition,
                            lookup = :lookup,
                            pk_columns = :pk_columns,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """),
                    {"id": config_id, **col_params},
                )
            else:
                conn.execute(
                    text("""
                        INSERT INTO configs (
                            config_name, table_name, json_data,
                            datasource_source_id, datasource_target_id,
                            config_type, script, generate_sql, condition, lookup, pk_columns,
                            updated_at
                        ) VALUES (
                            :config_name, :table_name, :json_data,
                            :datasource_source_id, :datasource_target_id,
                            :config_type, :script, :generate_sql, :condition, :lookup, :pk_columns,
                            CURRENT_TIMESTAMP
                        )
                    """),
                    col_params,
                )
                result = conn.execute(
                    text("SELECT id FROM configs WHERE config_name = :name"),
                    {"name": record.config_name},
                )
                config_id = result.scalar()

            ver_result = conn.execute(
                text("SELECT COALESCE(MAX(version), 0) FROM config_histories WHERE config_id = :cid"),
                {"cid": config_id},
            )
            next_version = ver_result.scalar() + 1
            conn.execute(
                text(
                    "INSERT INTO config_histories (config_id, version, json_data, created_at)"
                    " VALUES (:config_id, :version, :json_data, CURRENT_TIMESTAMP)"
                ),
                {"config_id": config_id, "version": next_version, "json_data": json_str},
            )
        return True, f"Saved config '{record.config_name}' (version {next_version})"
    except Exception as e:
        return False, f"Error: {e}"


def get_list() -> pd.DataFrame:
    """Get all configs as a pandas DataFrame (used by Streamlit views)."""
    import numpy as np

    with get_transaction() as conn:
        df = pd.read_sql(
            "SELECT id::text AS id, config_name, table_name, updated_at"
            " FROM configs WHERE is_deleted = false ORDER BY updated_at DESC",
            conn,
        )
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]):
                df[col] = np.array(df[col].fillna("").tolist(), dtype=object)
        return df


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM configs WHERE is_deleted = false AND deleted_at IS NULL")
        )
        return result.scalar()


def get_all_list() -> list[dict]:
    """Get all configs as a list of dicts with datasource details."""
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                SELECT
                    c.id::text AS id,
                    c.config_name,
                    c.table_name,
                    c.json_data,
                    c.datasource_source_id::text,
                    c.datasource_target_id::text,
                    c.config_type,
                    c.script,
                    c.generate_sql,
                    c.condition,
                    c.lookup,
                    c.pk_columns,
                    c.created_at,
                    c.created_by,
                    c.updated_at,
                    c.updated_by,
                    c.is_deleted,
                    c.deleted_at,
                    c.deleted_by,
                    c.deleted_reason,
                    ds_src.name AS datasource_source_name,
                    ds_src.db_type AS datasource_source_db_type,
                    ds_src.dbname AS datasource_source_dbname,
                    ds_tgt.name AS datasource_target_name,
                    ds_tgt.db_type AS datasource_target_db_type,
                    ds_tgt.dbname AS datasource_target_dbname
                FROM configs c
                LEFT JOIN datasources ds_src ON c.datasource_source_id = ds_src.id
                LEFT JOIN datasources ds_tgt ON c.datasource_target_id = ds_tgt.id
                WHERE c.is_deleted = false
                ORDER BY c.updated_at DESC
            """)
        )
        rows = rows_to_dicts(result)
        for row in rows:
            parse_json_field(row)
        return rows


def get_content(config_name: str) -> dict | None:
    """Get config by name with json_data merged into the returned dict."""
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT * FROM configs WHERE config_name = :name"),
            {"name": config_name},
        )
        data = row_to_dict(result)
        if data is None:
            return None
        data["id"] = str(data["id"])

        raw_json = data.get("json_data", "{}")
        try:
            parsed = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        except (json.JSONDecodeError, TypeError):
            parsed = {}

        merged = {**parsed}
        merged.setdefault("config_name", data.get("config_name", ""))
        merged.setdefault("name", data.get("config_name", ""))
        merged["_db_id"] = data["id"]
        merged["_db_updated_at"] = str(data.get("updated_at", ""))
        merged["condition"] = data.get("condition") or parsed.get("condition", "")
        merged["lookup"] = data.get("lookup") or parsed.get("lookup", "")
        merged["config_type"] = data.get("config_type") or parsed.get("config_type", "std")
        merged["script"] = data.get("script") or parsed.get("script", "")
        merged["generate_sql"] = data.get("generate_sql") or parsed.get("generate_sql", "")
        merged["pk_columns"] = data.get("pk_columns") or parsed.get("pk_columns") or None
        ds_src_id = data.get("datasource_source_id")
        merged["_datasource_source_id"] = str(ds_src_id) if ds_src_id else None
        ds_tgt_id = data.get("datasource_target_id")
        merged["_datasource_target_id"] = str(ds_tgt_id) if ds_tgt_id else None
        return merged


def get_by_id_raw(config_id: str) -> dict | None:
    """Get config by UUID — returns clean DB row with json_data parsed to dict."""
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                SELECT id::text AS id, config_name, table_name, json_data,
                       datasource_source_id::text, datasource_target_id::text,
                       config_type, script, generate_sql, condition, lookup, pk_columns,
                       created_at, created_by, updated_at, updated_by,
                       is_deleted, deleted_at, deleted_by, deleted_reason
                FROM configs WHERE id = :id
            """),
            {"id": config_id},
        )
        data = row_to_dict(result)
        if data is None:
            return None
        parse_json_field(data)
        return data


def delete(config_name: str) -> tuple[bool, str]:
    """Soft-delete a config by name."""
    try:
        with get_transaction() as conn:
            result = conn.execute(
                text(
                    "UPDATE configs SET is_deleted = true, deleted_at = CURRENT_TIMESTAMP"
                    " WHERE config_name = :name AND is_deleted = false"
                ),
                {"name": config_name},
            )
            if result.rowcount == 0:
                return False, f"Config '{config_name}' not found"
        return True, f"Deleted config '{config_name}'"
    except Exception as e:
        return False, f"Error: {e}"


def get_history(config_name: str) -> pd.DataFrame:
    with get_transaction() as conn:
        return pd.read_sql(
            text(
                "SELECT ch.version, ch.json_data, ch.created_at"
                " FROM config_histories ch"
                " JOIN configs c ON ch.config_id = c.id"
                " WHERE c.config_name = :name"
                " ORDER BY ch.version DESC"
            ),
            conn,
            params={"name": config_name},
        )


def get_version(config_name: str, version: int) -> dict | None:
    with get_transaction() as conn:
        result = conn.execute(
            text(
                "SELECT ch.version, ch.json_data, ch.created_at"
                " FROM config_histories ch"
                " JOIN configs c ON ch.config_id = c.id"
                " WHERE c.config_name = :name AND ch.version = :version"
            ),
            {"name": config_name, "version": version},
        )
        return row_to_dict(result)


def compare_versions(config_name: str, v1: int, v2: int) -> dict | None:
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
