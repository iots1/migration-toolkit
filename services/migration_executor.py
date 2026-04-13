"""
migration_executor.py — Pure Python ETL service.

Extracted from views/components/migration/step_execution.py so that the
migration logic can be reused by PipelineExecutor (background thread)
without any Streamlit dependency.

JIT Connection Pattern: creates SQLAlchemy engines with pool_pre_ping and
pool_recycle, then disposes them in a finally block regardless of outcome.
"""
from __future__ import annotations
import re as _re
import time
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dataclasses import dataclass

import services.db_connector as connector
from services.checkpoint_manager import save_checkpoint, clear_checkpoint
from services.encoding_helper import clean_dataframe
from services.query_builder import build_select_query, transform_batch, build_dtype_map, batch_insert
from services.transformers import DataTransformer


@dataclass
class MigrationResult:
    status: str              # "success" | "failed"
    rows_processed: int
    batch_count: int
    duration_seconds: float
    error_message: str = ""
    pre_count: int = 0
    post_count: int = 0    # -1 means post-verify query failed (non-fatal)


def run_single_migration(
    config: dict,
    source_conn_config: dict,
    target_conn_config: dict,
    batch_size: int = 1000,
    truncate_target: bool = False,
    test_mode: bool = False,
    skip_batches: int = 0,
    log_callback=None,
    progress_callback=None,
    checkpoint_callback=None,
) -> MigrationResult:
    """
    Run a full single-table migration and return a MigrationResult.

    Args:
        source_conn_config:  dict with keys db_type, host, port, db_name,
                             user, password, charset (optional)
        target_conn_config:  same shape
        log_callback:        fn(message: str, icon: str)
        progress_callback:   fn(batch_num: int, rows_processed: int, rows_in_batch: int)
        checkpoint_callback: fn(config_name: str, batch_num: int, rows: int)
                             — additional hook for PipelineExecutor (2D checkpoint)
    """
    def log(msg: str, icon: str = "ℹ️") -> None:
        if log_callback:
            log_callback(msg, icon)

    config_name = config.get("config_name", "migration")
    source_table = config["source"]["table"]
    target_table = config["target"]["table"]
    start_time = time.time()

    src_engine = connector.create_sqlalchemy_engine(
        **source_conn_config, pool_pre_ping=True, pool_recycle=3600
    )
    tgt_engine = connector.create_sqlalchemy_engine(
        **target_conn_config, pool_pre_ping=True, pool_recycle=3600
    )

    try:
        src_db_type = source_conn_config.get("db_type", "")
        log(f"Source connected: {src_db_type} (charset: {source_conn_config.get('charset') or 'default'})", "✅")
        log(f"Target connected: {target_conn_config.get('db_type', '')}", "✅")

        pre_count = _get_row_count(tgt_engine, target_table, log)

        if truncate_target:
            _truncate_table(tgt_engine, target_table, log)

        _validate_schema(src_engine, tgt_engine, source_table, target_table, config, log)
        _init_hn_counter(tgt_engine, target_table, config, log)

        generate_sql = (config.get("generate_sql") or "").strip()
        if generate_sql:
            select_query = generate_sql
            log("generate_sql found — using custom SQL (JOIN/WHERE included)", "📋")
            # generate_sql aliases columns to target names.
            # Remap active mappings so transform_batch finds them by target column name.
            remapped = [
                {**m, "source": m["target"]}
                for m in config.get("mappings", [])
                if not m.get("ignore", False) and m.get("target")
            ]
            config = {**config, "mappings": remapped}
            log(f"Remapped {len(remapped)} active mappings (source → target name)", "🔁")
        else:
            select_query = build_select_query(config, source_table, src_db_type)
            log("generate_sql not set — using dynamic SELECT from mappings", "🔧")
        log(f"SELECT Query: {select_query}", "🔍")
        log(f"Starting Batch Processing (Size: {batch_size})...", "🚀")

        data_iterator = pd.read_sql(select_query, src_engine, chunksize=batch_size, coerce_float=False)
        total_rows = 0
        batch_num = 0
        migration_failed = False
        error_message = ""

        for df_batch in data_iterator:
            batch_num += 1
            rows_in_batch = len(df_batch)

            if batch_num <= skip_batches:
                total_rows += rows_in_batch
                log(f"Batch {batch_num}: Skipped (checkpoint)", "⏭️")
                continue

            df_batch = clean_dataframe(df_batch)

            try:
                df_batch, bit_columns, val_warnings = transform_batch(df_batch, config)
                for w in val_warnings:
                    log(f"Batch {batch_num} — {w}", "⚠️")
            except Exception as e:
                log(f"Transformation Error in Batch {batch_num}: {e}", "⚠️")
                continue

            try:
                dtype_map = build_dtype_map(bit_columns, df_batch, target_conn_config.get("db_type", ""))
                batch_insert(df_batch, target_table, tgt_engine, dtype_map)

                total_rows += rows_in_batch
                save_checkpoint(config_name, batch_num, total_rows)
                if checkpoint_callback:
                    checkpoint_callback(config_name, batch_num, total_rows)
                if progress_callback:
                    progress_callback(batch_num, total_rows, rows_in_batch)
                log(f"Batch {batch_num}: Inserted {rows_in_batch} rows", "💾")

            except Exception as e:
                save_checkpoint(config_name, batch_num - 1, total_rows)
                short_err = str(e).split("[SQL:")[0].strip()[:300]
                error_message = short_err
                log(f"Insert Failed at Batch {batch_num}: {short_err}", "❌")
                migration_failed = True
                break

            if test_mode:
                log("Stopping after first batch (Test Mode)", "🛑")
                break

        if migration_failed:
            return MigrationResult(
                status="failed",
                rows_processed=total_rows,
                batch_count=batch_num,
                duration_seconds=time.time() - start_time,
                error_message=error_message,
                pre_count=pre_count,
            )

        post_count = _verify_post_migration(tgt_engine, target_table, pre_count, total_rows, log)
        clear_checkpoint(config_name)
        log("Checkpoint cleared (migration complete)", "🧹")

        return MigrationResult(
            status="success",
            rows_processed=total_rows,
            batch_count=batch_num,
            duration_seconds=time.time() - start_time,
            pre_count=pre_count,
            post_count=post_count,
        )

    except Exception as e:
        return MigrationResult(
            status="failed",
            rows_processed=0,
            batch_count=0,
            duration_seconds=time.time() - start_time,
            error_message=str(e),
        )
    finally:
        src_engine.dispose()
        tgt_engine.dispose()


# ---------------------------------------------------------------------------
# Private helpers — no Streamlit imports, use log callback only
# ---------------------------------------------------------------------------

def _get_row_count(engine, table: str, log) -> int:
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar() or 0
        log(f"Pre-migration count: {count:,} rows in `{table}`", "📊")
        return count
    except Exception as e:
        log(f"Could not get pre-migration count (non-critical): {e}", "⚠️")
        return 0


def _truncate_table(engine, table: str, log) -> None:
    log(f"Cleaning target table: {table}...", "🧹")
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
        log("Target table truncated successfully.", "✅")
    except Exception as e:
        log(f"TRUNCATE failed, trying DELETE FROM... ({e})", "⚠️")
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table}"))
            log("Target table cleared using DELETE.", "✅")
        except Exception as e2:
            log(f"Failed to clean table: {e2}", "❌")
            raise e2


def _validate_schema(src_engine, tgt_engine, source_table: str, target_table: str, config: dict, log) -> None:
    log("Validating Schema Compatibility...", "🧐")
    try:
        src_insp = sqlalchemy.inspect(src_engine)
        tgt_insp = sqlalchemy.inspect(tgt_engine)

        def _get_cols(insp, table):
            parts = table.split(".")
            t, s = parts[-1], parts[0] if len(parts) > 1 else None
            try:
                return {c["name"]: c["type"] for c in insp.get_columns(t, schema=s)}
            except Exception:
                return {c["name"]: c["type"] for c in insp.get_columns(table)}

        src_cols = _get_cols(src_insp, source_table)
        tgt_cols = _get_cols(tgt_insp, target_table)

        warnings = []
        for m in config.get("mappings", []):
            if m.get("ignore", False):
                continue
            sc, tc = m["source"], m["target"]
            if sc in src_cols and tc in tgt_cols:
                src_len = getattr(src_cols[sc], "length", None)
                tgt_len = getattr(tgt_cols[tc], "length", None)
                if tgt_len is not None:
                    if src_len is None:
                        warnings.append(f"- {sc} (Unknown/Text) -> {tc} (Limit: {tgt_len})")
                    elif src_len > tgt_len:
                        warnings.append(f"- {sc} (Limit: {src_len}) -> {tc} (Limit: {tgt_len})")

        if warnings:
            log("Potential Truncation Detected:\n" + "\n".join(warnings), "⚠️")
        else:
            log("Schema compatibility check passed.", "✅")
    except Exception as e:
        log(f"Skipping schema check (Non-critical): {e}", "⚠️")


def _init_hn_counter(tgt_engine, target_table: str, config: dict, log) -> None:
    for mapping in config.get("mappings", []):
        if mapping.get("ignore", False) or "GENERATE_HN" not in mapping.get("transformers", []):
            continue
        ghn_params = mapping.get("transformer_params", {}).get("GENERATE_HN", {})
        auto_detect = ghn_params.get("auto_detect_max", True)
        start_from = int(ghn_params.get("start_from", 0))

        if auto_detect:
            hn_col = mapping.get("target", mapping.get("source"))
            log(f"Auto-detecting max HN from `{target_table}.{hn_col}`...", "🔍")
            try:
                with tgt_engine.connect() as conn:
                    result = conn.execute(text(f'SELECT MAX("{hn_col}") FROM {target_table}'))
                    max_val = result.scalar()
                if max_val:
                    digits = _re.sub(r"\D", "", str(max_val))
                    start_from = int(digits) if digits else 0
                    log(f"Max HN found: `{max_val}` → counter starts at {start_from}", "✅")
                else:
                    log(f"No existing HN in target → counter starts at {start_from}", "ℹ️")
            except Exception as e:
                log(f"Auto-detect HN failed: {e} → using start_from={start_from}", "⚠️")

        DataTransformer.reset_hn_counter(start_from)
        log(f"HN Counter initialized at {start_from} (next: HN{str(start_from + 1).zfill(9)})", "🔢")
        break  # Only one GENERATE_HN per config


def _verify_post_migration(tgt_engine, target_table: str, pre_count: int, total_processed: int, log) -> int:
    """Returns post_count, or -1 if the verify query fails (non-fatal)."""
    try:
        with tgt_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}"))
            post_count = result.scalar() or 0
        actual_inserted = post_count - pre_count
        log(f"Post-migration count: {post_count:,} rows (inserted: {actual_inserted:,})", "📊")
        return post_count
    except Exception as e:
        log(f"Could not verify post-count: {e}", "⚠️")
        return -1
