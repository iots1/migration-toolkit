"""
migration_executor.py — Pure Python ETL service.

Extracted from views/components/migration/step_execution.py so that the
migration logic can be reused by PipelineExecutor (background thread)
without any Streamlit dependency.

JIT Connection Pattern: creates SQLAlchemy engines with pool_pre_ping and
pool_recycle, then disposes them in a finally block regardless of outcome.

Resilience features (Phases 1-7):
    - Atomic checkpoint writes (os.replace)
    - Cursor-based pagination (no OFFSET O(N) slowdown)
    - Batch-level transaction isolation (COPY in explicit tx)
    - Retry with exponential backoff + engine.dispose()
    - UPSERT / UPSERT_IGNORE strategies
    - Graceful shutdown via threading.Event
    - File-based heartbeat
    - Memory guard with adaptive batch sizing
    - TCP keepalive on migration engines
"""

from __future__ import annotations

import gc
import json as _json
import os
import re as _re
import time
import uuid
import threading
import pandas as pd
import sqlalchemy
from sqlalchemy import text, event
from sqlalchemy.exc import OperationalError, DisconnectionError, InterfaceError
from dataclasses import dataclass
from typing import Callable, Optional

import services.db_connector as connector
from services.checkpoint_manager import (
    save_checkpoint,
    clear_checkpoint,
    load_checkpoint,
)
from services.encoding_helper import clean_dataframe
from services.query_builder import (
    build_select_query,
    transform_batch,
    build_dtype_map,
    batch_insert,
    build_paginated_select,
    build_paginated_select_expanded,
    select_pagination_builder,
)
from services.transformers import DataTransformer


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CHECKPOINT_INTERVAL = 1

MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 45]

MEMORY_WARN_THRESHOLD = 85
MEMORY_ABORT_THRESHOLD = 95
MEMORY_PER_BATCH_TARGET_MB = 200
REEVAL_INTERVAL = 100

STALE_THRESHOLD_SECONDS = 300

HEARTBEAT_DIR_ENV = "HEARTBEAT_DIR"


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------


@dataclass
class MigrationResult:
    status: str
    rows_processed: int
    batch_count: int
    duration_seconds: float
    error_message: str = ""
    pre_count: int = 0
    post_count: int = -1


class MigrationInterrupted(Exception):
    """Raised when shutdown_event is set between batches."""


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclass
class _BatchOutcome:
    success: bool
    rows_in_batch: int
    rows_cumulative: int
    error_message: str = ""
    warnings_json: Optional[str] = None


@dataclass
class _TruncationDetail:
    column: str
    target_limit: int
    actual_max: int
    sample_value: str
    overflow_rows: int


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
    shutdown_event: threading.Event | None = None,
    job_id: str | None = None,
    migration_logger=None,
) -> MigrationResult:
    """
    Run a full single-table migration and return a MigrationResult.

    This function **never raises**; all errors are captured in the returned
    MigrationResult with status="failed".
    """

    def log(msg: str, icon: str = "ℹ️") -> None:
        if log_callback:
            log_callback(msg, icon)

    config_name = config.get("config_name", "migration")
    src_db_type = source_conn_config.get("db_type", "")
    tgt_db_type = target_conn_config.get("db_type", "")
    source_table = _qualify_table_name(config.get("source", {}).get("table", ""), src_db_type)
    target_table = _qualify_table_name(config.get("target", {}).get("table", ""), tgt_db_type)
    start_time = time.time()
    mlog = migration_logger

    if config.get("config_type") == "custom":
        tgt_engine = connector.create_sqlalchemy_engine(
            **target_conn_config, pool_pre_ping=True, pool_recycle=1800
        )
        try:
            log(f"Target connected: {target_conn_config.get('db_type', '')}", "✅")
            result = _run_custom_script(
                config=config,
                tgt_engine=tgt_engine,
                start_time=start_time,
                log=log,
            )
        finally:
            tgt_engine.dispose()

        _safe_notify_callback(
            batch_insert_callback,
            config_name=config_name,
            batch_round=result.batch_count,
            rows_in_batch=result.rows_processed,
            rows_cumulative=result.rows_processed,
            batch_size=batch_size,
            total_records_in_config=result.rows_processed,
            status=result.status,
            error_message=result.error_message or None,
            transformation_warnings=None,
        )
        return result

    src_engine = connector.create_sqlalchemy_engine(
        **source_conn_config, pool_pre_ping=True, pool_recycle=1800,
        pool_size=2, max_overflow=1,
    )
    tgt_engine = connector.create_sqlalchemy_engine(
        **target_conn_config, pool_pre_ping=True, pool_recycle=1800,
        pool_size=2, max_overflow=1,
    )

    _tune_pg_migration_session(src_engine)
    _tune_pg_migration_session(tgt_engine)
    _set_keepalive(src_engine)
    _set_keepalive(tgt_engine)

    try:
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

        insert_strategy = config.get("insert_strategy", "append")

        tgt_pk_columns = _detect_pk_columns(tgt_engine, target_table)
        if insert_strategy in ("upsert", "upsert_ignore") and not tgt_pk_columns:
            log(
                f"WARNING: insert_strategy='{insert_strategy}' requires a primary key "
                f"on the target table. Falling back to 'append'.",
                "⚠️",
            )
            insert_strategy = "append"

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
            shutdown_event=shutdown_event,
            job_id=job_id,
            insert_strategy=insert_strategy,
            tgt_pk_columns=tgt_pk_columns,
            migration_logger=mlog,
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

        if mlog:
            mlog.log(step=config_name, batch=batch_num, event="step_completed",
                     total_rows=total_rows, duration_s=round(time.time() - start_time, 1))

        return MigrationResult(
            status="success",
            rows_processed=total_rows,
            batch_count=batch_num,
            duration_seconds=time.time() - start_time,
            pre_count=pre_count,
            post_count=post_count,
        )

    except MigrationInterrupted:
        log("Migration interrupted by shutdown signal", "🛑")
        if mlog:
            mlog.log(step=config_name, event="step_interrupted")
        return MigrationResult(
            status="interrupted",
            rows_processed=0,
            batch_count=0,
            duration_seconds=time.time() - start_time,
            error_message="Shutdown requested",
        )
    except Exception as e:
        error_msg = str(e)
        log(f"Migration failed with unexpected error: {error_msg}", "❌")

        if mlog:
            mlog.log(step=config_name, event="step_failed", error=error_msg[:500])

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


# ---------------------------------------------------------------------------
# PK Detection
# ---------------------------------------------------------------------------


def _detect_pk_columns(engine, table: str) -> list[str] | None:
    """Detect primary key columns, falling back to smallest unique index."""
    try:
        insp = sqlalchemy.inspect(engine)
        pk = insp.get_pk_constraint(table)
        if pk and pk.get("constrained_columns"):
            return list(pk["constrained_columns"])
    except Exception:
        pass

    try:
        insp = sqlalchemy.inspect(engine)
        unique_indexes = insp.get_unique_constraints(table)
        if unique_indexes:
            shortest = min(unique_indexes, key=lambda u: len(u["column_names"]))
            return list(shortest["column_names"])
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


def _get_heartbeat_dir() -> str:
    from pathlib import Path

    env_dir = os.getenv(HEARTBEAT_DIR_ENV)
    if env_dir:
        return env_dir
    return str(Path.home() / ".his_analyzer" / "heartbeats")


def _write_heartbeat(job_id: str, step: str, batch: int) -> None:
    hb_dir = _get_heartbeat_dir()
    path = os.path.join(hb_dir, f"{job_id}.heartbeat")
    os.makedirs(hb_dir, exist_ok=True)
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            f.write(f"{step}|{batch}|{time.time()}")
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise


def _read_heartbeat(job_id: str) -> dict | None:
    path = os.path.join(_get_heartbeat_dir(), f"{job_id}.heartbeat")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            content = f.read().strip()
        step, batch, ts = content.split("|")
        return {"step": step, "batch": int(batch), "timestamp": float(ts)}
    except Exception:
        return None


def _clean_heartbeat(job_id: str) -> None:
    path = os.path.join(_get_heartbeat_dir(), f"{job_id}.heartbeat")
    if os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Memory Guard
# ---------------------------------------------------------------------------


def _check_memory(log: LogCallback, batch_num: int) -> bool:
    """Check system memory. Returns True if memory is OK, False if aborting."""
    try:
        import psutil

        mem = psutil.virtual_memory()
        if mem.percent > MEMORY_WARN_THRESHOLD:
            log(
                f"Batch {batch_num}: Memory at {mem.percent}% — running GC",
                "⚠️",
            )
            gc.collect()
            mem = psutil.virtual_memory()
            if mem.percent > MEMORY_ABORT_THRESHOLD:
                raise MemoryError(
                    f"Memory critically high ({mem.percent}%) — aborting to prevent OOM kill. "
                    f"Reduce batch_size or add more RAM."
                )
    except MemoryError:
        raise
    except ImportError:
        pass
    except PermissionError:
        pass
    except Exception:
        pass
    return True


# ---------------------------------------------------------------------------
# TCP Keepalive
# ---------------------------------------------------------------------------


def _set_keepalive(engine) -> None:
    """Register TCP keepalive on new connections (defense-in-depth)."""
    if "postgresql" not in str(engine.url):
        return

    @event.listens_for(engine, "connect")
    def _apply_keepalive(dbapi_conn, connection_record):
        try:
            dbapi_conn.set_keepalives(1)
            dbapi_conn.set_keepalives_idle(30)
            dbapi_conn.set_keepalives_interval(10)
            dbapi_conn.set_keepalives_count(5)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# SQL identifier safety
# ---------------------------------------------------------------------------


def _tune_pg_migration_session(engine) -> None:
    """Auto-configure PostgreSQL session parameters for migration workloads."""
    if "postgresql" not in str(engine.url):
        return

    @event.listens_for(engine, "connect")
    def _apply_migration_tuning(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("SET statement_timeout = 0")
        cursor.execute("SET work_mem = '256MB'")
        cursor.execute("SET max_parallel_workers_per_gather = 4")
        cursor.execute("SET maintenance_work_mem = '512MB'")
        cursor.execute("SET effective_cache_size = '4GB'")
        cursor.close()


def _quote_identifier(name: str) -> str:
    return ".".join(f'"{part.strip().strip(chr(34))}"' for part in name.split("."))


def _qualify_table_name(table: str, db_type: str) -> str:
    if not table:
        return table
    if db_type == "Microsoft SQL Server" and "." not in table:
        return f"dbo.{table}"
    return table


def _qualify_sql_tables(sql: str, db_type: str) -> str:
    if db_type != "Microsoft SQL Server":
        return sql
    pattern = r'\b(FROM|JOIN)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)\b'

    def _replacer(m):
        keyword, table_ref = m.group(1), m.group(2)
        if "." not in table_ref:
            return f"{keyword} dbo.{table_ref}"
        return m.group(0)

    return _re.sub(pattern, _replacer, sql, flags=_re.IGNORECASE)


# ---------------------------------------------------------------------------
# SQL statement splitter (dollar-quote aware)
# ---------------------------------------------------------------------------


def _split_sql_statements(script: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(script)
    in_dollar_quote = False
    dollar_tag = ""

    while i < n:
        if script[i] == "$":
            end = script.find("$", i + 1)
            if end != -1:
                tag = script[i : end + 1]
                if in_dollar_quote:
                    if tag == dollar_tag:
                        buf.append(tag)
                        i = end + 1
                        in_dollar_quote = False
                        dollar_tag = ""
                        continue
                else:
                    buf.append(tag)
                    i = end + 1
                    in_dollar_quote = True
                    dollar_tag = tag
                    continue

        if not in_dollar_quote and script[i] == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(script[i])
        i += 1

    stmt = "".join(buf).strip()
    if stmt:
        statements.append(stmt)

    return statements


# ---------------------------------------------------------------------------
# Custom script executor (config_type == "custom")
# ---------------------------------------------------------------------------


def _run_custom_script(
    config: dict,
    tgt_engine,
    start_time: float,
    log: LogCallback,
) -> MigrationResult:
    config_name = config.get("config_name", "custom")
    script: str = (config.get("script") or "").strip()

    if not script:
        msg = f"[{config_name}] config_type=custom but script is empty"
        log(msg, "❌")
        return MigrationResult(
            status="failed",
            rows_processed=0,
            batch_count=0,
            duration_seconds=time.time() - start_time,
            error_message=msg,
        )

    statements = _split_sql_statements(script)
    log(f"[{config_name}] Running custom script ({len(statements)} statement(s))", "📝")

    rows_affected = 0
    with tgt_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("SET statement_timeout = 0"))
        for idx, stmt in enumerate(statements, start=1):
            try:
                result = conn.execute(text(stmt))
                affected = result.rowcount if result.rowcount != -1 else 0
                rows_affected += max(affected, 0)
                log(f"  Statement {idx}/{len(statements)}: OK (rows affected: {affected})", "✅")
            except Exception as e:
                short_err = str(e).split("[SQL:")[0].strip()[:300]
                msg = f"Statement {idx} failed: {short_err}"
                log(f"[{config_name}] {msg}", "❌")
                return MigrationResult(
                    status="failed",
                    rows_processed=rows_affected,
                    batch_count=idx,
                    duration_seconds=time.time() - start_time,
                    error_message=msg,
                )

    duration = time.time() - start_time
    log(
        f"[{config_name}] Custom script completed in {duration:.1f}s "
        f"(total rows affected: {rows_affected})",
        "✅",
    )
    return MigrationResult(
        status="success",
        rows_processed=rows_affected,
        batch_count=len(statements),
        duration_seconds=duration,
    )


# ---------------------------------------------------------------------------
# Phase helpers — each handles one phase of the migration (SRP)
# ---------------------------------------------------------------------------


def _prepare_select_query(
    config: dict, source_table: str, src_db_type: str, log: LogCallback
) -> tuple[str, dict]:
    generate_sql = (config.get("generate_sql") or "").strip()

    if generate_sql:
        log("generate_sql found — using custom SQL (JOIN/WHERE included)", "📋")
        generate_sql = _qualify_sql_tables(generate_sql.rstrip(";"), src_db_type)
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
    try:
        with engine.connect() as conn:
            count_sql = f"SELECT COUNT(*) FROM ({select_query}) AS _src_count"
            result = conn.execute(text(count_sql))
            return result.scalar() or 0
    except Exception:
        pass
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {_quote_identifier(source_table)}")
            )
            return result.scalar() or 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Batch processing — cursor-based pagination (Phase 2)
# ---------------------------------------------------------------------------


def _load_last_seen_pk(checkpoint: dict | None, config_name: str) -> tuple | None:
    """Extract last_seen_pk from checkpoint for cursor-based resume."""
    if not checkpoint:
        return None
    pk_list = checkpoint.get("last_seen_pk")
    if pk_list and isinstance(pk_list, list) and len(pk_list) > 0:
        return tuple(pk_list)
    return None


def _check_shutdown(shutdown_event: threading.Event | None) -> None:
    """Raise MigrationInterrupted if shutdown is requested."""
    if shutdown_event and shutdown_event.is_set():
        raise MigrationInterrupted("Shutdown requested")


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
    shutdown_event: threading.Event | None = None,
    job_id: str | None = None,
    insert_strategy: str = "append",
    tgt_pk_columns: list[str] | None = None,
    migration_logger=None,
) -> tuple[int, int, str]:
    """
    Iterate over source data in cursor-paginated batches and insert into target.

    Returns (total_rows_processed, batch_count, error_message).
    """
    source_table = config.get("source", {}).get("table", "")

    config_pk = config.get("pk_columns")
    pk_columns = config_pk if config_pk else _detect_pk_columns(src_engine, source_table)

    if pk_columns is None:
        log(
            "WARNING: No PK or unique index detected on source table. "
            "Falling back to OFFSET-based pagination (non-deterministic for resume). "
            "Consider adding a PK, unique index, or specify 'pk_columns' in the config.",
            "⚠️",
        )
        return _process_batches_offset(
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
            shutdown_event=shutdown_event,
            job_id=job_id,
            insert_strategy=insert_strategy,
            tgt_pk_columns=tgt_pk_columns,
            migration_logger=migration_logger,
        )

    checkpoint = load_checkpoint(config_name)
    last_seen_pk = _load_last_seen_pk(checkpoint, config_name)

    if skip_batches > 0 and last_seen_pk is None:
        log(
            f"Checkpoint has skip_batches={skip_batches} but no last_seen_pk. "
            f"OFFSET skip will be applied before cursor pagination begins.",
            "⚠️",
        )

    pagination_fn = select_pagination_builder(src_engine)

    total_rows = checkpoint.get("rows_processed", 0) if checkpoint else 0
    batch_num = checkpoint.get("last_batch", 0) if checkpoint else 0

    adaptive_batch_size = batch_size
    first_batch = True

    log(
        f"Cursor-based pagination on PK: {pk_columns}"
        + (f" | Resuming from PK={last_seen_pk}" if last_seen_pk else ""),
        "🔍",
    )

    while True:
        _check_shutdown(shutdown_event)

        _check_memory(log, batch_num + 1)
        if migration_logger:
            try:
                import psutil as _psutil
                migration_logger.record_memory(_psutil.virtual_memory().percent)
            except Exception:
                pass

        query, params = pagination_fn(
            select_query, pk_columns, last_seen_pk, adaptive_batch_size
        )

        batch_start = time.time()

        try:
            df_batch = _read_batch_with_retry(
                src_engine, query, params, batch_num + 1, log
            )
        except Exception as e:
            save_checkpoint(config_name, batch_num, total_rows, last_seen_pk=last_seen_pk)
            return total_rows, batch_num, f"Source read error: {e}"

        if df_batch.empty:
            break

        rows_in_batch = len(df_batch)

        try:
            last_seen_pk = tuple(df_batch[pk].iloc[-1] for pk in pk_columns)
        except (KeyError, IndexError):
            log(
                f"WARNING: PK column missing from batch DataFrame. "
                f"Expected PK columns: {pk_columns}, got: {list(df_batch.columns)}",
                "⚠️",
            )
            save_checkpoint(config_name, batch_num, total_rows)
            return total_rows, batch_num, (
                f"PK column missing from query result. "
                f"Expected: {pk_columns}, got: {list(df_batch.columns)}"
            )

        batch_num += 1

        if skip_batches > 0:
            skip_batches -= 1
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
            insert_strategy=insert_strategy,
            tgt_pk_columns=tgt_pk_columns,
            migration_logger=migration_logger,
        )

        if outcome is None:
            del df_batch
            gc.collect()
            continue

        total_rows = outcome.rows_cumulative

        _safe_notify_callback(
            batch_insert_callback,
            config_name=config_name,
            batch_round=batch_num - 1,
            rows_in_batch=outcome.rows_in_batch if outcome.success else 0,
            rows_cumulative=outcome.rows_cumulative,
            batch_size=adaptive_batch_size,
            total_records_in_config=total_source_rows,
            status="success" if outcome.success else "failed",
            error_message=outcome.error_message or None,
            transformation_warnings=outcome.warnings_json,
        )

        if not outcome.success:
            save_checkpoint(config_name, batch_num - 1, total_rows, last_seen_pk=last_seen_pk)
            return total_rows, batch_num, outcome.error_message

        save_checkpoint(config_name, batch_num, total_rows, last_seen_pk=last_seen_pk)

        if job_id:
            try:
                _write_heartbeat(job_id, config_name, batch_num)
            except Exception:
                pass

        if checkpoint_callback:
            checkpoint_callback(config_name, batch_num, total_rows)
        if progress_callback:
            progress_callback(batch_num, total_rows, rows_in_batch)

        log(f"Batch {batch_num}: Inserted {rows_in_batch} rows", "💾")

        batch_duration = time.time() - batch_start
        if migration_logger:
            migration_logger.log(
                step=config_name, batch=batch_num, event="batch_inserted",
                rows=rows_in_batch, duration_s=round(batch_duration, 3),
                total_rows=total_rows,
            )
            migration_logger.record_batch_time(config_name, batch_duration)
            migration_logger.record_rows(config_name, total_rows)
            eta = migration_logger.estimate_eta(
                config_name, batch_num, total_source_rows, total_rows
            )
            if eta and batch_num % 10 == 0:
                log(f"ETA: {eta}", "⏱️")

        if first_batch:
            try:
                batch_mem_mb = df_batch.memory_usage(deep=True).sum() / (1024 * 1024)
                if batch_mem_mb > MEMORY_PER_BATCH_TARGET_MB:
                    scale = MEMORY_PER_BATCH_TARGET_MB / batch_mem_mb
                    adaptive_batch_size = max(100, int(batch_size * scale))
                    log(
                        f"Batch 1 memory: {batch_mem_mb:.0f}MB — "
                        f"reducing batch_size from {batch_size} to {adaptive_batch_size}",
                        "⚠️",
                    )
            except Exception:
                pass
            first_batch = False

        if batch_num > 1 and batch_num % REEVAL_INTERVAL == 0:
            try:
                batch_mem_mb = df_batch.memory_usage(deep=True).sum() / (1024 * 1024)
                if batch_mem_mb > MEMORY_PER_BATCH_TARGET_MB * 1.5:
                    scale = MEMORY_PER_BATCH_TARGET_MB / batch_mem_mb
                    adaptive_batch_size = max(100, int(adaptive_batch_size * scale))
                    log(
                        f"Batch {batch_num}: memory {batch_mem_mb:.0f}MB — "
                        f"reducing batch_size to {adaptive_batch_size}",
                        "⚠️",
                    )
                elif batch_mem_mb < MEMORY_PER_BATCH_TARGET_MB * 0.3:
                    new_size = min(batch_size, int(adaptive_batch_size * 1.5))
                    if new_size != adaptive_batch_size:
                        adaptive_batch_size = new_size
                        log(
                            f"Batch {batch_num}: memory {batch_mem_mb:.0f}MB — "
                            f"increasing batch_size to {adaptive_batch_size}",
                            "ℹ️",
                        )
            except Exception:
                pass

        del df_batch
        gc.collect()

        if test_mode:
            log("Stopping after first batch (Test Mode)", "🛑")
            break

    return total_rows, batch_num, ""


# ---------------------------------------------------------------------------
# OFFSET fallback for tables without PK
# ---------------------------------------------------------------------------


def _process_batches_offset(
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
    shutdown_event: threading.Event | None = None,
    job_id: str | None = None,
    insert_strategy: str = "append",
    tgt_pk_columns: list[str] | None = None,
    migration_logger=None,
) -> tuple[int, int, str]:
    """Fallback: OFFSET-based pagination for tables without PK.

    For PostgreSQL source: uses ctid for deterministic physical ordering.
    For MSSQL source: uses ROW_NUMBER() OVER (ORDER BY (SELECT 0)) surrogate key.
    For other databases: raises ValueError.
    """
    dialect = src_engine.dialect.name if hasattr(src_engine, "dialect") else ""

    if dialect == "postgresql":
        wrapped = (
            f"SELECT *, ctid FROM ({select_query}) AS _offset_src "
            f"ORDER BY ctid LIMIT :batch_size OFFSET :offset"
        )
    elif dialect == "mssql":
        wrapped = (
            f"SELECT * FROM ("
            f"  SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS _surrogate_row_num "
            f"  FROM ({select_query}) AS _offset_src"
            f") AS _offset_paged "
            f"WHERE _surrogate_row_num > :offset "
            f"AND _surrogate_row_num <= :offset + :batch_size "
            f"ORDER BY _surrogate_row_num"
        )
    else:
        raise ValueError(
            f"Cannot paginate table without PK or unique index on '{dialect}'. "
            f"Specify 'pk_columns' in the config or add a primary key to the source table."
        )

    checkpoint = load_checkpoint(config_name)
    total_rows = checkpoint.get("rows_processed", 0) if checkpoint else 0
    start_batch = checkpoint.get("last_batch", 0) if checkpoint else 0

    offset = max(skip_batches, start_batch) * batch_size
    batch_num = max(skip_batches, start_batch)

    log(
        f"OFFSET-based pagination ({dialect}) starting at offset={offset}",
        "⚠️",
    )

    while True:
        _check_shutdown(shutdown_event)
        _check_memory(log, batch_num + 1)
        batch_start = time.time()

        try:
            df_batch = _read_batch_with_retry(
                src_engine,
                wrapped,
                {"batch_size": batch_size, "offset": offset},
                batch_num + 1,
                log,
            )
        except Exception as e:
            save_checkpoint(config_name, batch_num, total_rows)
            return total_rows, batch_num, f"Source read error: {e}"

        if df_batch.empty:
            break

        rows_in_batch = len(df_batch)

        if "ctid" in df_batch.columns:
            df_batch = df_batch.drop(columns=["ctid"])

        if "_surrogate_row_num" in df_batch.columns:
            df_batch = df_batch.drop(columns=["_surrogate_row_num"])

        batch_num += 1

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
            insert_strategy=insert_strategy,
            tgt_pk_columns=tgt_pk_columns,
            migration_logger=migration_logger,
        )

        if outcome is None:
            del df_batch
            gc.collect()
            offset += batch_size
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
            save_checkpoint(config_name, batch_num - 1, total_rows)
            return total_rows, batch_num, outcome.error_message

        save_checkpoint(config_name, batch_num, total_rows)

        if job_id:
            try:
                _write_heartbeat(job_id, config_name, batch_num)
            except Exception:
                pass

        if checkpoint_callback:
            checkpoint_callback(config_name, batch_num, total_rows)
        if progress_callback:
            progress_callback(batch_num, total_rows, rows_in_batch)

        log(f"Batch {batch_num}: Inserted {rows_in_batch} rows", "💾")

        batch_duration = time.time() - batch_start
        if migration_logger:
            migration_logger.log(
                step=config_name, batch=batch_num, event="batch_inserted",
                rows=rows_in_batch, duration_s=round(batch_duration, 3),
                total_rows=total_rows, pagination="offset",
            )
            migration_logger.record_batch_time(config_name, batch_duration)
            migration_logger.record_rows(config_name, total_rows)

        offset += batch_size
        del df_batch
        gc.collect()

        if test_mode:
            log("Stopping after first batch (Test Mode)", "🛑")
            break

    return total_rows, batch_num, ""


# ---------------------------------------------------------------------------
# Single batch processing (with retry)
# ---------------------------------------------------------------------------


MAX_QUARANTINED_PER_BATCH = 100
QUARANTINE_CHUNK_SIZE = 50


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
    insert_strategy: str = "append",
    tgt_pk_columns: list[str] | None = None,
    migration_logger=None,
) -> Optional[_BatchOutcome]:
    """Clean, transform, and insert a single batch with retry."""
    rows_in_batch = len(df_batch)
    df_batch = clean_dataframe(df_batch)

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

    dtype_map = build_dtype_map(
        bit_columns, df_batch, target_conn_config.get("db_type", "")
    )

    try:
        _insert_with_retry(
            df_batch, target_table, tgt_engine, dtype_map,
            batch_num, log, insert_strategy, tgt_pk_columns,
        )
    except Exception as batch_error:
        if config.get("error_handling") != "skip_bad_rows":
            user_message = _build_user_friendly_error(
                batch_error, df_batch, config, tgt_engine, target_table, batch_num, log
            )
            return _BatchOutcome(
                success=False,
                rows_in_batch=rows_in_batch,
                rows_cumulative=total_rows,
                error_message=user_message,
                warnings_json=warnings_json,
            )

        log(
            f"Batch {batch_num}: Batch insert failed — "
            f"switching to quarantine mode",
            "⚠️",
        )

        inserted, quarantined = _insert_with_quarantine(
            df_batch=df_batch,
            target_table=target_table,
            tgt_engine=tgt_engine,
            dtype_map=dtype_map,
            batch_num=batch_num,
            config_name=config_name,
            config=config,
            log=log,
            migration_logger=migration_logger,
        )

        if migration_logger and quarantined > 0:
            migration_logger.record_quarantined(config_name, quarantined)
            migration_logger.log(
                step=config_name, batch=batch_num, event="rows_quarantined",
                quarantined=quarantined, inserted=inserted,
            )

        total_rows += inserted

        if checkpoint_callback:
            checkpoint_callback(config_name, batch_num, total_rows)
        if progress_callback:
            progress_callback(batch_num, total_rows, inserted)

        return _BatchOutcome(
            success=True,
            rows_in_batch=inserted,
            rows_cumulative=total_rows,
            warnings_json=warnings_json,
        )

    total_rows += rows_in_batch

    if checkpoint_callback:
        checkpoint_callback(config_name, batch_num, total_rows)
    if progress_callback:
        progress_callback(batch_num, total_rows, rows_in_batch)

    return _BatchOutcome(
        success=True,
        rows_in_batch=rows_in_batch,
        rows_cumulative=total_rows,
        warnings_json=warnings_json,
    )


# ---------------------------------------------------------------------------
# Dead Letter Queue — quarantine bad rows (Phase 9)
# ---------------------------------------------------------------------------


def _insert_with_quarantine(
    *,
    df_batch: pd.DataFrame,
    target_table: str,
    tgt_engine,
    dtype_map: dict,
    batch_num: int,
    config_name: str,
    config: dict,
    log: LogCallback,
    migration_logger=None,
) -> tuple[int, int]:
    """Try sub-batch insert. On failure, row-by-row to isolate bad rows.

    Returns (inserted_count, quarantined_count).
    """
    inserted = 0
    quarantined = 0

    for chunk_start in range(0, len(df_batch), QUARANTINE_CHUNK_SIZE):
        if quarantined >= MAX_QUARANTINED_PER_BATCH:
            break

        chunk_end = min(chunk_start + QUARANTINE_CHUNK_SIZE, len(df_batch))
        chunk = df_batch.iloc[chunk_start:chunk_end]

        try:
            _insert_with_retry(
                chunk, target_table, tgt_engine, dtype_map,
                batch_num, log, "append", None,
            )
            inserted += len(chunk)
        except Exception:
            for idx in chunk.index:
                if quarantined >= MAX_QUARANTINED_PER_BATCH:
                    break
                row_df = df_batch.loc[[idx]]
                try:
                    _insert_with_retry(
                        row_df, target_table, tgt_engine, dtype_map,
                        batch_num, log, "append", None,
                    )
                    inserted += 1
                except Exception as row_error:
                    quarantined += 1
                    if migration_logger:
                        try:
                            row_data = df_batch.loc[idx].to_dict()
                            for k, v in row_data.items():
                                if hasattr(v, "item"):
                                    row_data[k] = v.item()
                                elif hasattr(v, "isoformat"):
                                    row_data[k] = v.isoformat()
                            migration_logger.log(
                                step=config_name, batch=batch_num,
                                event="row_quarantined",
                                row_idx=int(idx) if isinstance(idx, int) else str(idx),
                                row=row_data,
                                error=str(row_error)[:300],
                            )
                        except Exception:
                            pass

    if quarantined > 0:
        log(
            f"Batch {batch_num}: Quarantined {quarantined} bad rows, "
            f"inserted {inserted}",
            "⚠️",
        )

    return inserted, quarantined


# ---------------------------------------------------------------------------
# Retry with backoff (Phase 4)
# ---------------------------------------------------------------------------


def _insert_with_retry(
    df: pd.DataFrame,
    target_table: str,
    tgt_engine,
    dtype_map: dict,
    batch_num: int,
    log: LogCallback,
    insert_strategy: str = "append",
    tgt_pk_columns: list[str] | None = None,
) -> None:
    """Insert a batch with retry on transient errors. Disposes stale pool."""
    for attempt in range(MAX_RETRIES):
        try:
            batch_insert(
                df, target_table, tgt_engine, dtype_map,
                insert_strategy=insert_strategy,
                pk_columns=tgt_pk_columns,
            )
            return
        except (OperationalError, DisconnectionError, InterfaceError) as e:
            if attempt == MAX_RETRIES - 1:
                raise
            delay = RETRY_DELAYS[attempt]
            log(
                f"Batch {batch_num}: transient error, retry {attempt + 1}/{MAX_RETRIES} "
                f"in {delay}s: {e}",
                "⚠️",
            )
            time.sleep(delay)
            tgt_engine.dispose()
        except Exception:
            raise


def _read_batch_with_retry(
    src_engine, query, params: dict, batch_num: int, log: LogCallback,
    max_retries: int = 3,
) -> pd.DataFrame:
    """Read a batch from source with retry on transient errors.

    Accepts either a plain string or sqlalchemy.text() clause.
    """
    if isinstance(query, str):
        query = text(query)
    for attempt in range(max_retries):
        try:
            with src_engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params, coerce_float=False)
            return df
        except (OperationalError, DisconnectionError) as e:
            if attempt == max_retries - 1:
                raise
            delay = RETRY_DELAYS[attempt]
            log(
                f"Batch {batch_num}: source read error, retry {attempt + 1}/{max_retries}: {e}",
                "⚠️",
            )
            time.sleep(delay)
            src_engine.dispose()
        except Exception:
            raise
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Callback safety wrapper
# ---------------------------------------------------------------------------


def _safe_notify_callback(callback, **kwargs) -> None:
    if not callback:
        return
    try:
        callback(**kwargs)
    except Exception:
        pass


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
    error_str = str(error)
    is_truncation = (
        "Truncation" in type(error).__name__ or "truncat" in error_str.lower()
    )

    if not is_truncation:
        short_err = error_str.split("[SQL:")[0].strip()[:300]
        log(f"Insert Failed at Batch {batch_num}: {short_err}", "❌")
        return short_err

    details = _find_all_truncated_columns(df, tgt_engine, target_table)

    if not details:
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
    limit_match = _re.search(r"character varying\((\d+)\)", error_str)
    target_limit = int(limit_match.group(1)) if limit_match else None

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
        break


def _find_all_truncated_columns(
    df: pd.DataFrame, tgt_engine, target_table: str,
) -> list[_TruncationDetail]:
    results: list[_TruncationDetail] = []
    try:
        tgt_insp = sqlalchemy.inspect(tgt_engine)
        parts = target_table.split(".")
        t, s = parts[-1], parts[0] if len(parts) > 1 else None
        try:
            raw_cols = tgt_insp.get_columns(t, schema=s)
        except Exception:
            raw_cols = tgt_insp.get_columns(target_table)

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
