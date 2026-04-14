"""
migration_executor.py — Pure Python ETL service.

Extracted from views/components/migration/step_execution.py so that the
migration logic can be reused by PipelineExecutor (background thread)
without any Streamlit dependency.

JIT Connection Pattern: creates SQLAlchemy engines with pool_pre_ping and
pool_recycle, then disposes them in a finally block regardless of outcome.
"""

from __future__ import annotations
import json as _json
import re as _re
import time
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dataclasses import dataclass
from typing import Callable, Optional

import services.db_connector as connector
from services.checkpoint_manager import save_checkpoint, clear_checkpoint
from services.encoding_helper import clean_dataframe
from services.query_builder import (
    build_select_query,
    transform_batch,
    build_dtype_map,
    batch_insert,
)
from services.transformers import DataTransformer


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------


@dataclass
class MigrationResult:
    status: str  # "success" | "failed"
    rows_processed: int
    batch_count: int
    duration_seconds: float
    error_message: str = ""
    pre_count: int = 0
    post_count: int = 0  # -1 means post-verify query failed (non-fatal)


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclass
class _BatchOutcome:
    """Carries the result of a single batch through to the callback."""

    success: bool
    rows_in_batch: int
    rows_cumulative: int
    error_message: str = ""
    warnings_json: Optional[str] = None


@dataclass
class _TruncationDetail:
    """Info about a single column that exceeds the target length limit."""

    column: str
    target_limit: int
    actual_max: int
    sample_value: str
    overflow_rows: int


# Type aliases for callbacks
LogCallback = Callable[[str, str], None]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


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
    batch_insert_callback=None,
) -> MigrationResult:
    """
    Run a full single-table migration and return a MigrationResult.

    This function **never raises**; all errors are captured in the returned
    MigrationResult with status="failed".

    Args:
        source_conn_config:  dict with keys db_type, host, port, db_name,
                             user, password, charset (optional)
        target_conn_config:  same shape
        log_callback:        fn(message: str, icon: str)
        progress_callback:   fn(batch_num: int, rows_processed: int, rows_in_batch: int)
        checkpoint_callback: fn(config_name: str, batch_num: int, rows: int)
                             — legacy hook for resumable checkpoints
        batch_insert_callback: fn(config_name, batch_round, rows_in_batch, rows_cumulative,
                                  batch_size, total_records_in_config, status, error_message,
                                  transformation_warnings)
                             — save batch record to pipeline_runs after each batch
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
        log(
            f"Source connected: {src_db_type} "
            f"(charset: {source_conn_config.get('charset') or 'default'})",
            "✅",
        )
        log(f"Target connected: {target_conn_config.get('db_type', '')}", "✅")

        pre_count = _get_row_count(tgt_engine, target_table, log)

        if truncate_target:
            _truncate_table(tgt_engine, target_table, log)

        _validate_schema(
            src_engine, tgt_engine, source_table, target_table, config, log
        )
        _init_hn_counter(tgt_engine, target_table, config, log)

        select_query, config = _prepare_select_query(
            config, source_table, src_db_type, log
        )
        log(f"SELECT Query: {select_query}", "🔍")
        log(f"Starting Batch Processing (Size: {batch_size})...", "🚀")

        total_source_rows = _count_source_rows(src_engine, select_query, source_table)

        total_rows, batch_num, error_message = _process_batches(
            src_engine=src_engine,
            tgt_engine=tgt_engine,
            select_query=select_query,
            config=config,
            config_name=config_name,
            target_table=target_table,
            target_conn_config=target_conn_config,
            batch_size=batch_size,
            skip_batches=skip_batches,
            test_mode=test_mode,
            total_source_rows=total_source_rows,
            log=log,
            progress_callback=progress_callback,
            checkpoint_callback=checkpoint_callback,
            batch_insert_callback=batch_insert_callback,
        )

        if error_message:
            return MigrationResult(
                status="failed",
                rows_processed=total_rows,
                batch_count=batch_num,
                duration_seconds=time.time() - start_time,
                error_message=error_message,
                pre_count=pre_count,
            )

        post_count = _verify_post_migration(
            tgt_engine, target_table, pre_count, total_rows, log
        )
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
        error_msg = str(e)
        log(f"Migration failed with unexpected error: {error_msg}", "❌")

        _safe_notify_callback(
            batch_insert_callback,
            config_name=config_name,
            batch_round=0,
            rows_in_batch=0,
            rows_cumulative=0,
            batch_size=batch_size,
            total_records_in_config=0,
            status="failed",
            error_message=error_msg[:300],
            transformation_warnings=None,
        )

        return MigrationResult(
            status="failed",
            rows_processed=0,
            batch_count=0,
            duration_seconds=time.time() - start_time,
            error_message=error_msg,
        )
    finally:
        src_engine.dispose()
        tgt_engine.dispose()


# Save a checkpoint to disk every N successful batches.
# Reduces disk I/O by ~10x while keeping resume granularity acceptable.
# Error checkpoints always save (regardless of interval).
_CHECKPOINT_INTERVAL = 10


# ---------------------------------------------------------------------------
# SQL identifier safety
# ---------------------------------------------------------------------------


def _quote_identifier(name: str) -> str:
    """Double-quote each part of a dotted identifier to prevent SQL injection.

    Examples:
        "cnPatientDudeeV1"      → '"cnPatientDudeeV1"'
        "public.test_patients"  → '"public"."test_patients"'
        "dbo.patients"          → '"dbo"."patients"'
    """
    return ".".join(f'"{part.strip().strip(chr(34))}"' for part in name.split("."))


# ---------------------------------------------------------------------------
# Phase helpers — each handles one phase of the migration (SRP)
# ---------------------------------------------------------------------------


def _prepare_select_query(
    config: dict, source_table: str, src_db_type: str, log: LogCallback
) -> tuple[str, dict]:
    """
    Determine the SELECT query and (possibly) remap config mappings.

    Returns (select_query, updated_config).
    """
    generate_sql = (config.get("generate_sql") or "").strip()

    if generate_sql:
        log("generate_sql found — using custom SQL (JOIN/WHERE included)", "📋")
        remapped = [
            {**m, "source": m["target"]}
            for m in config.get("mappings", [])
            if not m.get("ignore", False) and m.get("target")
        ]
        config = {**config, "mappings": remapped}
        log(f"Remapped {len(remapped)} active mappings (source → target name)", "🔁")
        return generate_sql, config

    log("generate_sql not set — using dynamic SELECT from mappings", "🔧")
    return build_select_query(config, source_table, src_db_type), config


def _count_source_rows(engine, select_query: str, source_table: str) -> int:
    """Count rows from the actual SELECT query (subquery), fallback to table count.

    When generate_sql has JOINs or WHERE filters, the result count differs
    from the raw source table count. Using the real query ensures accurate
    progress reporting via total_records_in_config.
    """
    # Try counting from the actual query first (accurate for filtered/joined queries)
    try:
        with engine.connect() as conn:
            count_sql = f"SELECT COUNT(*) FROM ({select_query}) AS _src_count"
            result = conn.execute(text(count_sql))
            return result.scalar() or 0
    except Exception:
        pass
    # Fallback: raw table count (when subquery wrapping fails, e.g. MSSQL TOP clause)
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {_quote_identifier(source_table)}")
            )
            return result.scalar() or 0
    except Exception:
        return 0


def _process_batches(
    *,
    src_engine,
    tgt_engine,
    select_query: str,
    config: dict,
    config_name: str,
    target_table: str,
    target_conn_config: dict,
    batch_size: int,
    skip_batches: int,
    test_mode: bool,
    total_source_rows: int,
    log: LogCallback,
    progress_callback,
    checkpoint_callback,
    batch_insert_callback,
) -> tuple[int, int, str]:
    """
    Iterate over source data in batches and insert into target.

    Returns (total_rows_processed, batch_count, error_message).
    error_message is empty string on success.
    """
    data_iterator = pd.read_sql(
        select_query, src_engine, chunksize=batch_size, coerce_float=False
    )
    total_rows = 0
    batch_num = 0
    error_message = ""

    for df_batch in data_iterator:
        batch_num += 1
        rows_in_batch = len(df_batch)

        if batch_num <= skip_batches:
            total_rows += rows_in_batch
            log(f"Batch {batch_num}: Skipped (checkpoint)", "⏭️")
            continue

        outcome = _process_single_batch(
            df_batch=df_batch,
            batch_num=batch_num,
            config=config,
            config_name=config_name,
            target_table=target_table,
            target_conn_config=target_conn_config,
            tgt_engine=tgt_engine,
            total_rows=total_rows,
            log=log,
            progress_callback=progress_callback,
            checkpoint_callback=checkpoint_callback,
        )

        if outcome is None:
            # transform failed — skip this batch entirely
            continue

        total_rows = outcome.rows_cumulative

        _safe_notify_callback(
            batch_insert_callback,
            config_name=config_name,
            batch_round=batch_num - 1,
            rows_in_batch=outcome.rows_in_batch if outcome.success else 0,
            rows_cumulative=outcome.rows_cumulative,
            batch_size=batch_size,
            total_records_in_config=total_source_rows,
            status="success" if outcome.success else "failed",
            error_message=outcome.error_message or None,
            transformation_warnings=outcome.warnings_json,
        )

        if not outcome.success:
            error_message = outcome.error_message
            break

        if test_mode:
            log("Stopping after first batch (Test Mode)", "🛑")
            break

    return total_rows, batch_num, error_message


def _process_single_batch(
    *,
    df_batch: pd.DataFrame,
    batch_num: int,
    config: dict,
    config_name: str,
    target_table: str,
    target_conn_config: dict,
    tgt_engine,
    total_rows: int,
    log: LogCallback,
    progress_callback,
    checkpoint_callback,
) -> Optional[_BatchOutcome]:
    """
    Clean, transform, and insert a single batch.

    Returns:
        _BatchOutcome on success or insert failure (always notify callback).
        None if transformation fails (skip batch, no callback needed).
    """
    rows_in_batch = len(df_batch)
    df_batch = clean_dataframe(df_batch)

    # --- Transform ---
    try:
        df_batch, bit_columns, val_warnings = transform_batch(df_batch, config)
    except Exception as e:
        log(f"Transformation Error in Batch {batch_num}: {e}", "⚠️")
        return None

    warnings_json = (
        _json.dumps(val_warnings, ensure_ascii=False) if val_warnings else None
    )
    for w in val_warnings:
        log(f"Batch {batch_num} — {w}", "⚠️")

    # --- Insert ---
    try:
        dtype_map = build_dtype_map(
            bit_columns, df_batch, target_conn_config.get("db_type", "")
        )
        batch_insert(df_batch, target_table, tgt_engine, dtype_map)
    except Exception as e:
        user_message = _build_user_friendly_error(
            e, df_batch, config, tgt_engine, target_table, batch_num, log
        )

        save_checkpoint(config_name, batch_num - 1, total_rows)

        return _BatchOutcome(
            success=False,
            rows_in_batch=rows_in_batch,
            rows_cumulative=total_rows,
            error_message=user_message,
            warnings_json=warnings_json,
        )

    # --- Success bookkeeping ---
    total_rows += rows_in_batch
    # Save checkpoint every N batches to reduce disk I/O.
    # Error path (above) always saves so resume works correctly.
    if batch_num % _CHECKPOINT_INTERVAL == 0:
        save_checkpoint(config_name, batch_num, total_rows)

    if checkpoint_callback:
        checkpoint_callback(config_name, batch_num, total_rows)
    if progress_callback:
        progress_callback(batch_num, total_rows, rows_in_batch)

    log(f"Batch {batch_num}: Inserted {rows_in_batch} rows", "💾")

    return _BatchOutcome(
        success=True,
        rows_in_batch=rows_in_batch,
        rows_cumulative=total_rows,
        warnings_json=warnings_json,
    )


# ---------------------------------------------------------------------------
# Callback safety wrapper
# ---------------------------------------------------------------------------


def _safe_notify_callback(callback, **kwargs) -> None:
    """Call batch_insert_callback if provided, swallowing errors."""
    if not callback:
        return
    try:
        callback(**kwargs)
    except Exception:
        pass  # callback failure must never break the migration


# ---------------------------------------------------------------------------
# Private helpers — no Streamlit imports, use log callback only
# ---------------------------------------------------------------------------


def _build_user_friendly_error(
    error: Exception,
    df: pd.DataFrame,
    config: dict,
    tgt_engine,
    target_table: str,
    batch_num: int,
    log: LogCallback,
) -> str:
    """
    Build a human-readable error message for insert failures.

    For truncation errors: identifies ALL offending columns with details.
    For other errors: strips SQL noise and adds batch context.
    """
    error_str = str(error)
    is_truncation = (
        "Truncation" in type(error).__name__ or "truncat" in error_str.lower()
    )

    if not is_truncation:
        short_err = error_str.split("[SQL:")[0].strip()[:300]
        log(f"Insert Failed at Batch {batch_num}: {short_err}", "❌")
        return short_err

    # --- Truncation: find all offending columns via direct df ↔ schema scan ---
    details = _find_all_truncated_columns(df, tgt_engine, target_table)

    if not details:
        # Schema scan failed — surface what we can from the raw error + df
        return _build_truncation_fallback(error_str, df, batch_num, log)

    log(
        f"Insert Failed at Batch {batch_num}: "
        f"{len(details)} column(s) exceed target size limit",
        "❌",
    )

    lines = []
    for d in details:
        sample_display = (
            f"{d.sample_value[:50]}..." if len(d.sample_value) > 50 else d.sample_value
        )
        log(
            f"  Column `{d.column}`: limit {d.target_limit} chars, "
            f"found {d.actual_max} chars ({d.overflow_rows} rows overflow) "
            f'— sample: "{sample_display}"',
            "⚠️",
        )
        lines.append(
            f"[{d.column}] limit={d.target_limit}, actual={d.actual_max}, "
            f"overflow_rows={d.overflow_rows}"
        )

    suggestion = "Increase column size in target DB or add TRUNCATE_TO transformer"
    log(f"  Suggestion: {suggestion}", "💡")

    return f"Data too long: {'; '.join(lines)}. {suggestion}"[:500]


def _build_truncation_fallback(
    error_str: str, df: pd.DataFrame, batch_num: int, log: LogCallback
) -> str:
    """
    Fallback when schema inspection fails: extract the VARCHAR limit from the
    error string and find the longest string column in the batch to point the
    user at the most likely offender.
    """
    import re as _re2

    # Extract VARCHAR limit from psycopg2: "character varying(10)"
    limit_match = _re2.search(r"character varying\((\d+)\)", error_str)
    target_limit = int(limit_match.group(1)) if limit_match else None

    # Find the string column in df with the longest actual value
    best_col: str | None = None
    best_max: int = 0
    best_sample: str = ""
    for col in df.columns:
        try:
            lengths = df[col].fillna("").astype(str).str.len()
            col_max = int(lengths.max())
            if col_max > best_max:
                best_max = col_max
                best_col = col
                best_sample = str(df.at[lengths.idxmax(), col])
        except Exception:
            continue

    if best_col and target_limit and best_max > target_limit:
        sample_display = (
            f"{best_sample[:50]}..." if len(best_sample) > 50 else best_sample
        )
        msg = (
            f"Data too long: [{best_col}] limit={target_limit}, actual={best_max}, "
            f"overflow_rows=unknown. "
            f"Increase column size in target DB or add TRUNCATE_TO transformer"
        )
        log(f"Insert Failed at Batch {batch_num}: {len(df.columns)} columns scanned — "
            f"likely offender `{best_col}`: found {best_max} chars "
            f"(target limit: {target_limit}) — sample: \"{sample_display}\"", "❌")
    elif best_col and best_max > 0:
        limit_hint = f" (target limit: {target_limit})" if target_limit else ""
        msg = (
            f"Data too long{limit_hint}: [{best_col}] actual={best_max} chars. "
            f"Increase column size in target DB or add TRUNCATE_TO transformer"
        )
        log(f"Insert Failed at Batch {batch_num}: likely offender `{best_col}` "
            f"({best_max} chars){limit_hint}", "❌")
    else:
        limit_hint = f" (limit: {target_limit} chars)" if target_limit else ""
        msg = f"Data too long for a VARCHAR column{limit_hint} — could not identify column automatically"
        log(f"Insert Failed at Batch {batch_num}: truncation error{limit_hint}", "❌")

    return msg[:500]


def _get_row_count(engine, table: str, log: LogCallback) -> int:
    try:
        quoted = _quote_identifier(table)
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {quoted}"))
            count = result.scalar() or 0
        log(f"Pre-migration count: {count:,} rows in `{table}`", "📊")
        return count
    except Exception as e:
        log(f"Could not get pre-migration count (non-critical): {e}", "⚠️")
        return 0


def _truncate_table(engine, table: str, log: LogCallback) -> None:
    log(f"Cleaning target table: {table}...", "🧹")
    quoted = _quote_identifier(table)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {quoted}"))
        log("Target table truncated successfully.", "✅")
    except Exception as e:
        log(f"TRUNCATE failed, trying DELETE FROM... ({e})", "⚠️")
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {quoted}"))
            log("Target table cleared using DELETE.", "✅")
        except Exception as e2:
            log(f"Failed to clean table: {e2}", "❌")
            raise e2


def _validate_schema(
    src_engine, tgt_engine, source_table: str, target_table: str, config: dict,
    log: LogCallback,
) -> None:
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
                        warnings.append(
                            f"- {sc} (Unknown/Text) -> {tc} (Limit: {tgt_len})"
                        )
                    elif src_len > tgt_len:
                        warnings.append(
                            f"- {sc} (Limit: {src_len}) -> {tc} (Limit: {tgt_len})"
                        )

        if warnings:
            log("Potential Truncation Detected:\n" + "\n".join(warnings), "⚠️")
        else:
            log("Schema compatibility check passed.", "✅")
    except Exception as e:
        log(f"Skipping schema check (Non-critical): {e}", "⚠️")


def _init_hn_counter(
    tgt_engine, target_table: str, config: dict, log: LogCallback,
) -> None:
    for mapping in config.get("mappings", []):
        if mapping.get("ignore", False) or "GENERATE_HN" not in mapping.get(
            "transformers", []
        ):
            continue
        ghn_params = mapping.get("transformer_params", {}).get("GENERATE_HN", {})
        auto_detect = ghn_params.get("auto_detect_max", True)
        start_from = int(ghn_params.get("start_from", 0))

        if auto_detect:
            hn_col = mapping.get("target", mapping.get("source"))
            log(f"Auto-detecting max HN from `{target_table}.{hn_col}`...", "🔍")
            try:
                with tgt_engine.connect() as conn:
                    result = conn.execute(
                        text(f'SELECT MAX("{hn_col}") FROM {_quote_identifier(target_table)}')
                    )
                    max_val = result.scalar()
                if max_val:
                    digits = _re.sub(r"\D", "", str(max_val))
                    start_from = int(digits) if digits else 0
                    log(
                        f"Max HN found: `{max_val}` → counter starts at {start_from}",
                        "✅",
                    )
                else:
                    log(
                        f"No existing HN in target → counter starts at {start_from}",
                        "ℹ️",
                    )
            except Exception as e:
                log(
                    f"Auto-detect HN failed: {e} → using start_from={start_from}", "⚠️"
                )

        DataTransformer.reset_hn_counter(start_from)
        log(
            f"HN Counter initialized at {start_from} "
            f"(next: HN{str(start_from + 1).zfill(9)})",
            "🔢",
        )
        break  # Only one GENERATE_HN per config


def _find_all_truncated_columns(
    df: pd.DataFrame, tgt_engine, target_table: str,
) -> list[_TruncationDetail]:
    """
    Scan every df column directly against the target schema's VARCHAR limits.

    After transform_batch(), df columns are already target column names, so we
    compare df columns ↔ target schema without needing config mappings.
    Case-insensitive matching handles PostgreSQL's lowercase normalization.
    """
    results: list[_TruncationDetail] = []
    try:
        tgt_insp = sqlalchemy.inspect(tgt_engine)
        parts = target_table.split(".")
        t, s = parts[-1], parts[0] if len(parts) > 1 else None
        try:
            raw_cols = tgt_insp.get_columns(t, schema=s)
        except Exception:
            raw_cols = tgt_insp.get_columns(target_table)

        # lower(col_name) → (actual_col_name, type)
        tgt_cols_lower: dict[str, tuple[str, object]] = {
            c["name"].lower(): (c["name"], c["type"]) for c in raw_cols
        }

        for col in df.columns:
            col_key = col.lower()
            if col_key not in tgt_cols_lower:
                continue

            _, col_type = tgt_cols_lower[col_key]
            target_limit = getattr(col_type, "length", None)
            if target_limit is None:
                continue

            str_lengths = df[col].fillna("").astype(str).str.len()
            actual_max = int(str_lengths.max())
            if actual_max <= target_limit:
                continue

            overflow_rows = int((str_lengths > target_limit).sum())
            sample_idx = str_lengths.idxmax()
            sample_value = str(df.at[sample_idx, col])

            results.append(
                _TruncationDetail(
                    column=col,
                    target_limit=target_limit,
                    actual_max=actual_max,
                    sample_value=sample_value,
                    overflow_rows=overflow_rows,
                )
            )
    except Exception:
        pass
    return results


def _verify_post_migration(
    tgt_engine, target_table: str, pre_count: int, total_processed: int,
    log: LogCallback,
) -> int:
    """Returns post_count, or -1 if the verify query fails (non-fatal)."""
    try:
        quoted = _quote_identifier(target_table)
        with tgt_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {quoted}"))
            post_count = result.scalar() or 0
        actual_inserted = post_count - pre_count
        log(
            f"Post-migration count: {post_count:,} rows "
            f"(inserted: {actual_inserted:,})",
            "📊",
        )
        return post_count
    except Exception as e:
        log(f"Could not verify post-count: {e}", "⚠️")
        return -1
