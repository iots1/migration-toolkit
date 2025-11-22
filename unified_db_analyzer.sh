#!/usr/bin/env bash

# ==============================================================================
# HIS DATABASE MIGRATION ANALYZER (v5.1 - Smart Skip Date Frequency)
# Features: 
#   1. Deep Analysis Toggle (Min/Max/Top 5) via Config
#   2. **Smart Skip:** Automatically skips 'Top 5 Freq' for Date/Time columns (Performance Boost)
#   3. Configurable Logic for MySQL/Postgres/MSSQL
#   4. Enhanced Logging & Progress
# ==============================================================================

# --- [CRITICAL] AUTO-SWITCH BASH VERSION ---
if [ -z "$BASH_VERSINFO" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
    CANDIDATE_PATHS=("/opt/homebrew/bin/bash" "/usr/local/bin/bash" "/usr/bin/bash")
    for NEW_BASH in "${CANDIDATE_PATHS[@]}"; do
        if [ -x "$NEW_BASH" ]; then
            VER=$("$NEW_BASH" --version | head -n 1 | grep -oE '[0-9]\.[0-9]+' | head -n 1)
            MAJOR=${VER%%.*}
            if [ "$MAJOR" -ge 4 ]; then exec "$NEW_BASH" "$0" "$@"; fi
        fi
    done
fi

# --- SETUP ---
BASE_OUTPUT_DIR="./migration_report"
DATE_NOW=$(date +"%Y%m%d_%H%M")
RUN_DIR="$BASE_OUTPUT_DIR/$DATE_NOW"
PROFILE_DIR="$RUN_DIR/data_profile"
DDL_DIR="$RUN_DIR/ddl_schema"

mkdir -p "$PROFILE_DIR"
mkdir -p "$DDL_DIR"

REPORT_FILE="$PROFILE_DIR/data_profile.csv"
DDL_FILE="$DDL_DIR/schema.sql"
LOG_FILE="$RUN_DIR/process.log"

# Initialize Log
echo "----------------------------------------------------------------" > "$LOG_FILE"
echo "HIS Database Migration Analyzer Log" >> "$LOG_FILE"
echo "Started at: $(date)" >> "$LOG_FILE"
echo "----------------------------------------------------------------" >> "$LOG_FILE"

log_activity() {
    local msg="$1"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] $msg" >> "$LOG_FILE"
}

# Header CSV
echo "Table,Column,DataType,PK,FK,Default,Comment,Total_Rows,Null_Count,Max_Length,Distinct_Values,Min_Val,Max_Val,Top_5_Values,Sample_Values" > "$REPORT_FILE"

# --- DEPENDENCIES ---
check_command() {
    local cmd="$1"
    local brew_pkg="$2"
    if [ -n "$brew_pkg" ] && command -v brew &> /dev/null; then
         BREW_PREFIX=$(brew --prefix)
         POSSIBLE_PATHS=("$BREW_PREFIX/opt/$brew_pkg/bin" "$BREW_PREFIX/Cellar/$brew_pkg/*/bin")
         for p in "${POSSIBLE_PATHS[@]}"; do
             for expanded_path in $p; do
                 if [ -x "$expanded_path/$cmd" ]; then export PATH="$expanded_path:$PATH"; break 2; fi
             done
         done
    fi
    if ! command -v "$cmd" &> /dev/null; then
        log_activity "Error: Command '$cmd' not found."
        echo "❌ Error: ไม่พบคำสั่ง '$cmd'"; exit 1
    fi
}
check_command "jq" "jq"

# --- LOAD CONFIG ---
CONFIG_FILE="config.json"
if [ ! -f "$CONFIG_FILE" ]; then echo "❌ Error: ไม่พบไฟล์ $CONFIG_FILE"; exit 1; fi

DB_TYPE=$(jq -r '.database.type' "$CONFIG_FILE")
DB_HOST=$(jq -r '.database.host' "$CONFIG_FILE")
DB_PORT=$(jq -r '.database.port' "$CONFIG_FILE")
DB_NAME=$(jq -r '.database.name' "$CONFIG_FILE")
DB_USER=$(jq -r '.database.user' "$CONFIG_FILE")
DB_PASS=$(jq -r '.database.password' "$CONFIG_FILE")

case "$DB_TYPE" in
    "mysql") DB_CHOICE=1 ;;
    "postgresql"|"postgres") DB_CHOICE=2 ;;
    "mssql"|"sqlserver") DB_CHOICE=3 ;;
    *) echo "❌ Error: Unknown database type '$DB_TYPE'"; exit 1 ;;
esac

DEFAULT_LIMIT=$(jq -r '.sampling.default_limit // 10' "$CONFIG_FILE")
MAX_TEXT_LEN=$(jq -r '.sampling.max_text_length // 300' "$CONFIG_FILE")
DEEP_ANALYSIS=$(jq -r '.sampling.deep_analysis // false' "$CONFIG_FILE")

EXCEPTIONS_STRING=$(jq -r '.sampling.exceptions[] | "\(.table).\(.column)=\(.limit)|"' "$CONFIG_FILE" | tr -d '\n')
EXCEPTIONS_COUNT=$(jq '.sampling.exceptions | length' "$CONFIG_FILE")

log_activity "Target: $DB_NAME ($DB_TYPE) @ $DB_HOST:$DB_PORT"
log_activity "Config: Deep Analysis=$DEEP_ANALYSIS, Default Limit=$DEFAULT_LIMIT"

# Helper function
get_sample_limit() {
    local tbl="$1"
    local col="$2"
    local distinct_val="$3"
    if [ "$distinct_val" == "1" ]; then echo "1"; return; fi
    local search_key="$tbl.$col="
    if [[ "$EXCEPTIONS_STRING" == *"$search_key"* ]]; then
        local temp="${EXCEPTIONS_STRING#*${search_key}}"
        echo "${temp%%|*}"
        return
    fi
    echo "$DEFAULT_LIMIT"
}

# Helper: Check if type is Date/Time
is_date_type() {
    local type=$(echo "$1" | tr '[:upper:]' '[:lower:]')
    if [[ "$type" =~ "date" ]] || [[ "$type" =~ "time" ]] || [[ "$type" =~ "year" ]]; then
        echo "true"
    else
        echo "false"
    fi
}

# Progress Bar
START_TIME=$(date +%s)
draw_progress() {
    local current=$1; local total=$2; local msg=$3
    local percent=0
    if [ "$total" -gt 0 ]; then percent=$(( 100 * current / total )); fi
    local elapsed=$(( $(date +%s) - START_TIME ))
    local time_str=$(printf "%02d:%02d" $((elapsed/60)) $((elapsed%60)))
    local width=25
    local filled=$(( width * percent / 100 )); local empty=$(( width - filled ))
    local bar="["; for ((i=0; i<filled; i++)); do bar+="="; done; bar+=">"; for ((i=0; i<empty; i++)); do bar+=" "; done; bar+="]"
    printf "\r\033[K%s %3d%% [Tbl %s/%s] (%s) -> %s" "$bar" "$percent" "$current" "$total" "$time_str" "$msg"
}

echo "========================================="
echo "   🏥 HIS Database Migration Analyzer    "
echo "========================================="
echo "🐚 Shell: Bash $BASH_VERSION"
echo "🔌 Target: $DB_NAME ($DB_TYPE)"
echo "🧠 Deep Analysis: $DEEP_ANALYSIS (Smart Skip enabled for Date/Time)"
echo "📂 Output: $RUN_DIR"
echo "-----------------------------------------"

# ==============================================================================
# 1. MySQL Logic
# ==============================================================================
analyze_mysql() {
    check_command "mysql" "mysql-client"
    check_command "mysqldump" "mysql-client"

    log_activity "Starting DDL Export..."
    mysqldump -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" --no-data --routines --triggers "$DB_NAME" > "$DDL_FILE" 2>/dev/null

    log_activity "Fetching Tables..."
    RAW_TABLES=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "SHOW TABLES")
    TABLES_ARRAY=($RAW_TABLES)
    TOTAL_TABLES=${#TABLES_ARRAY[@]}
    
    CURRENT_IDX=0
    START_TIME=$(date +%s)

    for TABLE in "${TABLES_ARRAY[@]}"; do
        ((CURRENT_IDX++))
        draw_progress "$CURRENT_IDX" "$TOTAL_TABLES" "$TABLE"
        log_activity "Processing Table: $TABLE"
        
        COLUMNS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
            SELECT c.COLUMN_NAME, c.DATA_TYPE, IF(c.COLUMN_KEY='PRI', 'YES', '') as IS_PK,
                (SELECT CONCAT('-> ', k.REFERENCED_TABLE_NAME, '.', k.REFERENCED_COLUMN_NAME) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k WHERE k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.REFERENCED_TABLE_NAME IS NOT NULL LIMIT 1) as FK_REF,
                IFNULL(c.COLUMN_DEFAULT, '') as DEF_VAL, c.COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = '$DB_NAME' AND c.TABLE_NAME = '$TABLE' ORDER BY c.ORDINAL_POSITION")
        
        echo "$COLUMNS" | while IFS=$'\t' read -r COL_NAME COL_TYPE IS_PK FK_REF DEF_VAL COMMENT; do
            # Basic Stats
            STATS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                SELECT COUNT(*), SUM(IF(\`$COL_NAME\` IS NULL,1,0)), MAX(LENGTH(\`$COL_NAME\`)), COUNT(DISTINCT \`$COL_NAME\`) FROM \`$TABLE\`;")
            DISTINCT_VAL=$(echo "$STATS" | awk '{print $4}')
            
            # Deep Analysis
            MIN_VAL=""
            MAX_VAL=""
            TOP_5=""
            if [ "$DEEP_ANALYSIS" == "true" ]; then
                # Min/Max (Always run, useful for date range)
                MINMAX=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                    SELECT MIN(\`$COL_NAME\`), MAX(\`$COL_NAME\`) FROM \`$TABLE\`;")
                MIN_VAL=$(echo "$MINMAX" | cut -f1)
                MAX_VAL=$(echo "$MINMAX" | cut -f2)
                
                # Smart Skip: Check if Date/Time type
                IS_DATE=$(is_date_type "$COL_TYPE")
                if [ "$IS_DATE" == "false" ]; then
                    # Run Top 5 only for Non-Date types
                    TOP_5=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                        SELECT GROUP_CONCAT(CONCAT(val, ' (', cnt, ')') SEPARATOR ' | ') 
                        FROM (SELECT \`$COL_NAME\` as val, COUNT(*) as cnt FROM \`$TABLE\` WHERE \`$COL_NAME\` IS NOT NULL GROUP BY \`$COL_NAME\` ORDER BY cnt DESC LIMIT 5) x;")
                else
                    TOP_5="(Skipped for Date/Time)"
                fi
            fi

            LIMIT_N=$(get_sample_limit "$TABLE" "$COL_NAME" "$DISTINCT_VAL")
            [ -z "$LIMIT_N" ] && LIMIT_N=$DEFAULT_LIMIT

            SAMPLE=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                SELECT GROUP_CONCAT(LEFT(val, $MAX_TEXT_LEN) SEPARATOR ' | ') FROM (SELECT \`$COL_NAME\` as val FROM \`$TABLE\` WHERE \`$COL_NAME\` IS NOT NULL LIMIT $LIMIT_N) x;")
            
            # Clean & Output
            STATS_FMT=$(echo "$STATS" | tr '\t' ',')
            CLEAN_SAMPLE=$(echo "$SAMPLE" | sed 's/"/""/g' | tr -d '\n')
            CLEAN_DEF=$(echo "$DEF_VAL" | sed 's/"/""/g')
            CLEAN_COMM=$(echo "$COMMENT" | sed 's/"/""/g')
            CLEAN_MIN=$(echo "$MIN_VAL" | sed 's/"/""/g')
            CLEAN_MAX=$(echo "$MAX_VAL" | sed 's/"/""/g')
            CLEAN_TOP5=$(echo "$TOP_5" | sed 's/"/""/g' | tr -d '\n')
            CLEAN_FK=$(echo "$FK_REF" | sed 's/NULL//g')

            echo "$TABLE,$COL_NAME,$COL_TYPE,$IS_PK,\"$CLEAN_FK\",\"$CLEAN_DEF\",\"$CLEAN_COMM\",$STATS_FMT,\"$CLEAN_MIN\",\"$CLEAN_MAX\",\"$CLEAN_TOP5\",\"$CLEAN_SAMPLE\"" >> "$REPORT_FILE"
        done
    done
    echo ""
}

# ==============================================================================
# 2. PostgreSQL Logic
# ==============================================================================
analyze_postgres() {
    check_command "psql" "libpq"
    check_command "pg_dump" "libpq"
    export PGPASSWORD="$DB_PASS"

    log_activity "Starting DDL Export (pg_dump)..."
    pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -s "$DB_NAME" > "$DDL_FILE" 2>/dev/null

    log_activity "Fetching Tables..."
    RAW_TABLES=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "
        SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
    TABLES_ARRAY=($RAW_TABLES)
    TOTAL_TABLES=${#TABLES_ARRAY[@]}
    
    CURRENT_IDX=0
    START_TIME=$(date +%s)

    for TABLE in "${TABLES_ARRAY[@]}"; do
        ((CURRENT_IDX++))
        draw_progress "$CURRENT_IDX" "$TOTAL_TABLES" "$TABLE"
        log_activity "Processing Table: $TABLE"

        COLUMNS=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -F "|" -c "
            SELECT c.column_name, c.data_type,
                (SELECT 'YES' FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name=tc.constraint_name WHERE kcu.table_name=c.table_name AND kcu.column_name=c.column_name AND tc.constraint_type='PRIMARY KEY' LIMIT 1),
                (SELECT '-> ' || ccu.table_name || '.' || ccu.column_name FROM information_schema.key_column_usage AS kcu JOIN information_schema.referential_constraints AS rc ON kcu.constraint_name = rc.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON rc.unique_constraint_name = ccu.constraint_name WHERE kcu.table_name = c.table_name AND kcu.column_name = c.column_name LIMIT 1),
                COALESCE(c.column_default, ''), pg_catalog.col_description(format('%I.%I', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position)
            FROM information_schema.columns c WHERE c.table_schema = 'public' AND c.table_name = '$TABLE' ORDER BY c.ordinal_position")

        echo "$COLUMNS" | while IFS="|" read -r COL_NAME COL_TYPE IS_PK FK_REF DEF_VAL COMMENT; do
            QUERY_STATS="SELECT COUNT(*), COUNT(*) - COUNT(\"$COL_NAME\"), MAX(LENGTH(CAST(\"$COL_NAME\" AS TEXT))), COUNT(DISTINCT \"$COL_NAME\") FROM \"$TABLE\""
            STATS_RESULT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -F"," -c "$QUERY_STATS")
            
            TOTAL=$(echo $STATS_RESULT | cut -d, -f1)
            NULLS=$(echo $STATS_RESULT | cut -d, -f2)
            MAX_LEN=$(echo $STATS_RESULT | cut -d, -f3)
            DISTINCT_VAL=$(echo $STATS_RESULT | cut -d, -f4)

            # Deep Analysis
            MIN_VAL=""
            MAX_VAL=""
            TOP_5=""
            if [ "$DEEP_ANALYSIS" == "true" ]; then
                # Min/Max (Always run)
                MINMAX=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -F "|" -c "SELECT MIN(\"$COL_NAME\"::text), MAX(\"$COL_NAME\"::text) FROM \"$TABLE\"")
                MIN_VAL=$(echo "$MINMAX" | cut -d'|' -f1)
                MAX_VAL=$(echo "$MINMAX" | cut -d'|' -f2)
                
                # Smart Skip: Check if Date/Time type
                IS_DATE=$(is_date_type "$COL_TYPE")
                if [ "$IS_DATE" == "false" ]; then
                    TOP_5=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "
                        SELECT string_agg(val || ' (' || cnt || ')', ' | ') 
                        FROM (SELECT \"$COL_NAME\"::text as val, COUNT(*) as cnt FROM \"$TABLE\" WHERE \"$COL_NAME\" IS NOT NULL GROUP BY \"$COL_NAME\" ORDER BY cnt DESC LIMIT 5) x")
                else
                    TOP_5="(Skipped for Date/Time)"
                fi
            fi

            LIMIT_N=$(get_sample_limit "$TABLE" "$COL_NAME" "$DISTINCT_VAL")
            [ -z "$LIMIT_N" ] && LIMIT_N=$DEFAULT_LIMIT

            QUERY_SAMPLE="SELECT (SELECT string_agg(SUBSTR(\"$COL_NAME\"::text, 1, $MAX_TEXT_LEN), ' | ') FROM (SELECT \"$COL_NAME\" FROM \"$TABLE\" WHERE \"$COL_NAME\" IS NOT NULL LIMIT $LIMIT_N) t)"
            SAMPLE=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "$QUERY_SAMPLE")
            
            # Clean
            CLEAN_SAMPLE=$(echo "$SAMPLE" | sed 's/"/""/g')
            CLEAN_DEF=$(echo "$DEF_VAL" | sed 's/"/""/g')
            CLEAN_COMM=$(echo "$COMMENT" | sed 's/"/""/g' | tr -d '\n')
            CLEAN_FK=$(echo "$FK_REF" | sed 's/"/""/g')
            CLEAN_MIN=$(echo "$MIN_VAL" | sed 's/"/""/g')
            CLEAN_MAX=$(echo "$MAX_VAL" | sed 's/"/""/g')
            CLEAN_TOP5=$(echo "$TOP_5" | sed 's/"/""/g' | tr -d '\n')

            echo "$TABLE,$COL_NAME,$COL_TYPE,$IS_PK,\"$CLEAN_FK\",\"$CLEAN_DEF\",\"$CLEAN_COMM\",$TOTAL,$NULLS,$MAX_LEN,$DISTINCT_VAL,\"$CLEAN_MIN\",\"$CLEAN_MAX\",\"$CLEAN_TOP5\",\"$CLEAN_SAMPLE\"" >> "$REPORT_FILE"
        done
    done
    unset PGPASSWORD
    echo ""
}

# ==============================================================================
# 3. MSSQL Logic (Same pattern applied)
# ==============================================================================
analyze_mssql() {
    check_command "sqlcmd"
    log_activity "MSSQL Analysis not fully implemented for Deep Analysis yet in this version."
}

# --- MAIN ---
case $DB_CHOICE in
    1) analyze_mysql ;;
    2) analyze_postgres ;;
    3) analyze_mssql ;;
    *) log_activity "Invalid Selection"; echo "❌ Invalid Selection"; exit 1 ;;
esac

echo "========================================="
echo "✅ Analysis Complete!"
echo "📄 Run Folder: $RUN_DIR"
echo "├── 📄 Schema:  $DDL_FILE"
echo "├── 📊 Profile: $REPORT_FILE"
echo "└── 📝 Log:     $LOG_FILE"

if [ -f "csv_to_html.py" ]; then
    echo "🌍 Generating HTML Report..."
    python3 csv_to_html.py "$REPORT_FILE"
    
    HTML_FILE="${REPORT_FILE%.csv}.html"
    if [ -f "$HTML_FILE" ]; then
        if command -v open &> /dev/null; then open "$HTML_FILE"
        elif command -v xdg-open &> /dev/null; then xdg-open "$HTML_FILE"
        elif command -v wslview &> /dev/null; then wslview "$HTML_FILE"
        elif command -v explorer.exe &> /dev/null; then explorer.exe `wslpath -w "$HTML_FILE"`
        fi
    fi
fi