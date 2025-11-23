#!/usr/bin/env bash

# ==============================================================================
# HIS DATABASE MIGRATION ANALYZER (v6.8 - Fix MSSQL CSV Output Format)
# Features:
#   1. **Fix:** Fixed CSV output format - changed SELECT to PRINT statement
#   2. **Fix:** Fixed @TableSizeMB and @DType variable scope issues in dynamic SQL
#   3. **Fix:** Added '-C' flag to sqlcmd to trust self-signed certificates
#   4. **New:** Auto-install support for sqlcmd (mssql-tools18)
#   5. Table Size (MB) Calculation & Data Composition Analysis
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
echo "Table,Column,DataType,PK,FK,Default,Comment,Total_Rows,Table_Size_MB,Null_Count,Empty_Count,Zero_Count,Max_Length,Distinct_Values,Min_Val,Max_Val,Top_5_Values,Sample_Values" > "$REPORT_FILE"

# --- DEPENDENCIES ---
check_command() {
    local cmd="$1"
    local brew_pkg="$2"

    # 1. Auto-detect Homebrew Keg-Only Paths
    if [ -n "$brew_pkg" ] && command -v brew &> /dev/null; then
         BREW_PREFIX=$(brew --prefix)
         POSSIBLE_PATHS=(
             "$BREW_PREFIX/opt/$brew_pkg/bin"
             "$BREW_PREFIX/Cellar/$brew_pkg/*/bin"
             "/usr/local/opt/$brew_pkg/bin" 
         )
         for p in "${POSSIBLE_PATHS[@]}"; do
             for expanded_path in $p; do
                 if [ -x "$expanded_path/$cmd" ]; then
                     export PATH="$expanded_path:$PATH"
                     break 2
                 fi
             done
         done
    fi

    # 2. If still not found, prompt to install
    if ! command -v "$cmd" &> /dev/null; then
        echo "❌ Error: ไม่พบคำสั่ง '$cmd'"
        if command -v brew &> /dev/null && [ -n "$brew_pkg" ]; then
            echo "🍺 ตรวจพบ Homebrew..."
            read -p "❓ ต้องการติดตั้ง '$brew_pkg' เดี๋ยวนี้หรือไม่? (y/N): " install_choice
            if [[ "$install_choice" =~ ^[Yy]$ ]]; then
                echo "📦 Installing $brew_pkg ..."
                
                if [ "$cmd" == "sqlcmd" ]; then
                    echo "   -> Tapping microsoft/mssql-release..."
                    brew tap microsoft/mssql-release
                    brew update
                fi

                brew install "$brew_pkg"
                
                BREW_PREFIX=$(brew --prefix)
                POSSIBLE_PATHS=(
                    "$BREW_PREFIX/opt/$brew_pkg/bin"
                    "$BREW_PREFIX/Cellar/$brew_pkg/*/bin"
                )
                for p in "${POSSIBLE_PATHS[@]}"; do
                    for expanded_path in $p; do
                        if [ -x "$expanded_path/$cmd" ]; then
                            echo "🔗 Linking binary from $expanded_path"
                            export PATH="$expanded_path:$PATH"
                            break 2
                        fi
                    done
                done

                if command -v "$cmd" &> /dev/null; then 
                    echo "✅ ติดตั้งสำเร็จ! ดำเนินการต่อ..."
                    return 0
                else 
                    echo "❌ ติดตั้งเสร็จสิ้นแต่ยังเรียกใช้คำสั่งไม่ได้ (ลอง restart terminal)"
                    exit 1
                fi
            else
                echo "❌ ยกเลิกการติดตั้ง"
                exit 1
            fi
        else
            echo "❌ กรุณาติดตั้ง '$brew_pkg' หรือ '$cmd' ด้วยตนเอง"
            exit 1
        fi
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

SELECTED_TABLES_STR=$(jq -r '.database.tables[]?' "$CONFIG_FILE" | tr '\n' ' ')

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

get_sample_limit() {
    local tbl="$1"; local col="$2"; local distinct_val="$3"
    if [ "$distinct_val" == "1" ]; then echo "1"; return; fi
    local search_key="$tbl.$col="
    if [[ "$EXCEPTIONS_STRING" == *"$search_key"* ]]; then
        local temp="${EXCEPTIONS_STRING#*${search_key}}"
        echo "${temp%%|*}"
        return
    fi
    echo "$DEFAULT_LIMIT"
}

is_date_type() {
    local type=$(echo "$1" | tr '[:upper:]' '[:lower:]')
    if [[ "$type" =~ "date" ]] || [[ "$type" =~ "time" ]] || [[ "$type" =~ "year" ]]; then echo "true"; else echo "false"; fi
}

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
echo "🧠 Deep Analysis: $DEEP_ANALYSIS"
if [ -n "$SELECTED_TABLES_STR" ]; then echo "🎯 Scope: Selected tables only"; else echo "🎯 Scope: All tables"; fi
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
    if [ -n "$SELECTED_TABLES_STR" ]; then
        TABLES_ARRAY=($SELECTED_TABLES_STR)
    else
        RAW_TABLES=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "SHOW TABLES")
        TABLES_ARRAY=($RAW_TABLES)
    fi
    TOTAL_TABLES=${#TABLES_ARRAY[@]}
    
    CURRENT_IDX=0; START_TIME=$(date +%s)

    for TABLE in "${TABLES_ARRAY[@]}"; do
        ((CURRENT_IDX++))
        draw_progress "$CURRENT_IDX" "$TOTAL_TABLES" "$TABLE"
        log_activity "Processing Table: $TABLE"
        
        CHECK_EXIST=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "SHOW TABLES LIKE '$TABLE'")
        if [ -z "$CHECK_EXIST" ]; then continue; fi
        
        TBL_SIZE=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
            SELECT ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = '$DB_NAME' AND TABLE_NAME = '$TABLE';")
        [ -z "$TBL_SIZE" ] && TBL_SIZE="0"

        COLUMNS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
            SELECT c.COLUMN_NAME, c.DATA_TYPE, IF(c.COLUMN_KEY='PRI', 'YES', '') as IS_PK,
                (SELECT CONCAT('-> ', k.REFERENCED_TABLE_NAME, '.', k.REFERENCED_COLUMN_NAME) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k WHERE k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.REFERENCED_TABLE_NAME IS NOT NULL LIMIT 1) as FK_REF,
                IFNULL(c.COLUMN_DEFAULT, '') as DEF_VAL, c.COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = '$DB_NAME' AND c.TABLE_NAME = '$TABLE' ORDER BY c.ORDINAL_POSITION")
        
        echo "$COLUMNS" | while IFS=$'\t' read -r COL_NAME COL_TYPE IS_PK FK_REF DEF_VAL COMMENT; do
            STATS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                SELECT COUNT(*), SUM(IF(\`$COL_NAME\` IS NULL,1,0)), SUM(IF(CAST(\`$COL_NAME\` AS CHAR) = '', 1, 0)), SUM(IF(CAST(\`$COL_NAME\` AS CHAR) = '0', 1, 0)), MAX(LENGTH(\`$COL_NAME\`)), COUNT(DISTINCT \`$COL_NAME\`) FROM \`$TABLE\`;")
            read TOTAL NULLS EMPTIES ZEROS MAX_LEN DISTINCT_VAL <<< "$STATS"
            
            MIN_VAL=""; MAX_VAL=""; TOP_5=""
            if [ "$DEEP_ANALYSIS" == "true" ]; then
                MINMAX=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "SELECT MIN(\`$COL_NAME\`), MAX(\`$COL_NAME\`) FROM \`$TABLE\`;")
                MIN_VAL=$(echo "$MINMAX" | cut -f1); MAX_VAL=$(echo "$MINMAX" | cut -f2)
                IS_DATE=$(is_date_type "$COL_TYPE")
                if [ "$IS_DATE" == "false" ]; then
                    TOP_5=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "SELECT GROUP_CONCAT(CONCAT(val, ' (', cnt, ')') SEPARATOR ' | ') FROM (SELECT \`$COL_NAME\` as val, COUNT(*) as cnt FROM \`$TABLE\` WHERE \`$COL_NAME\` IS NOT NULL GROUP BY \`$COL_NAME\` ORDER BY cnt DESC LIMIT 5) x;")
                else
                    TOP_5="(Skipped for Date/Time)"
                fi
            fi

            LIMIT_N=$(get_sample_limit "$TABLE" "$COL_NAME" "$DISTINCT_VAL")
            [ -z "$LIMIT_N" ] && LIMIT_N=$DEFAULT_LIMIT
            SAMPLE=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "SELECT GROUP_CONCAT(LEFT(val, $MAX_TEXT_LEN) SEPARATOR ' | ') FROM (SELECT \`$COL_NAME\` as val FROM \`$TABLE\` WHERE \`$COL_NAME\` IS NOT NULL LIMIT $LIMIT_N) x;")
            
            CLEAN_SAMPLE=$(echo "$SAMPLE" | sed 's/"/""/g' | tr -d '\n'); CLEAN_DEF=$(echo "$DEF_VAL" | sed 's/"/""/g'); CLEAN_COMM=$(echo "$COMMENT" | sed 's/"/""/g'); CLEAN_FK=$(echo "$FK_REF" | sed 's/NULL//g'); CLEAN_MIN=$(echo "$MIN_VAL" | sed 's/"/""/g'); CLEAN_MAX=$(echo "$MAX_VAL" | sed 's/"/""/g'); CLEAN_TOP5=$(echo "$TOP_5" | sed 's/"/""/g' | tr -d '\n')

            echo "$TABLE,$COL_NAME,$COL_TYPE,$IS_PK,\"$CLEAN_FK\",\"$CLEAN_DEF\",\"$CLEAN_COMM\",$TOTAL,$TBL_SIZE,$NULLS,$EMPTIES,$ZEROS,$MAX_LEN,$DISTINCT_VAL,\"$CLEAN_MIN\",\"$CLEAN_MAX\",\"$CLEAN_TOP5\",\"$CLEAN_SAMPLE\"" >> "$REPORT_FILE"
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

    log_activity "Starting DDL Export..."
    pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -s "$DB_NAME" > "$DDL_FILE" 2>/dev/null

    log_activity "Fetching Tables..."
    if [ -n "$SELECTED_TABLES_STR" ]; then
        TABLES_ARRAY=($SELECTED_TABLES_STR)
    else
        RAW_TABLES=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
        TABLES_ARRAY=($RAW_TABLES)
    fi
    TOTAL_TABLES=${#TABLES_ARRAY[@]}
    
    CURRENT_IDX=0; START_TIME=$(date +%s)

    for TABLE in "${TABLES_ARRAY[@]}"; do
        ((CURRENT_IDX++))
        draw_progress "$CURRENT_IDX" "$TOTAL_TABLES" "$TABLE"
        log_activity "Processing Table: $TABLE"
        
        CHECK_EXIST=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT to_regclass('public.$TABLE')")
        if [ -z "$CHECK_EXIST" ] || [ "$CHECK_EXIST" == "" ]; then continue; fi

        TBL_SIZE=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT ROUND(pg_total_relation_size('\"$TABLE\"') / 1024.0 / 1024.0, 2)")
        [ -z "$TBL_SIZE" ] && TBL_SIZE="0"

        COLUMNS=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -F "|" -c "
            SELECT c.column_name, c.data_type,
                (SELECT 'YES' FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name=tc.constraint_name WHERE kcu.table_name=c.table_name AND kcu.column_name=c.column_name AND tc.constraint_type='PRIMARY KEY' LIMIT 1),
                (SELECT '-> ' || ccu.table_name || '.' || ccu.column_name FROM information_schema.key_column_usage AS kcu JOIN information_schema.referential_constraints AS rc ON kcu.constraint_name = rc.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON rc.unique_constraint_name = ccu.constraint_name WHERE kcu.table_name = c.table_name AND kcu.column_name = c.column_name LIMIT 1),
                COALESCE(c.column_default, ''), pg_catalog.col_description(format('%I.%I', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position)
            FROM information_schema.columns c WHERE c.table_schema = 'public' AND c.table_name = '$TABLE' ORDER BY c.ordinal_position")

        echo "$COLUMNS" | while IFS="|" read -r COL_NAME COL_TYPE IS_PK FK_REF DEF_VAL COMMENT; do
            QUERY_STATS="SELECT COUNT(*), COUNT(*) - COUNT(\"$COL_NAME\"), COUNT(*) FILTER (WHERE CAST(\"$COL_NAME\" AS TEXT) = ''), COUNT(*) FILTER (WHERE CAST(\"$COL_NAME\" AS TEXT) = '0'), MAX(LENGTH(CAST(\"$COL_NAME\" AS TEXT))), COUNT(DISTINCT \"$COL_NAME\") FROM \"$TABLE\""
            STATS_RESULT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -F"," -c "$QUERY_STATS")
            IFS=',' read -r TOTAL NULLS EMPTIES ZEROS MAX_LEN DISTINCT_VAL <<< "$STATS_RESULT"

            MIN_VAL=""; MAX_VAL=""; TOP_5=""
            if [ "$DEEP_ANALYSIS" == "true" ]; then
                MINMAX=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -F "|" -c "SELECT MIN(\"$COL_NAME\"::text), MAX(\"$COL_NAME\"::text) FROM \"$TABLE\"")
                MIN_VAL=$(echo "$MINMAX" | cut -d'|' -f1); MAX_VAL=$(echo "$MINMAX" | cut -d'|' -f2)
                IS_DATE=$(is_date_type "$COL_TYPE")
                if [ "$IS_DATE" == "false" ]; then
                    TOP_5=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT string_agg(val || ' (' || cnt || ')', ' | ') FROM (SELECT \"$COL_NAME\"::text as val, COUNT(*) as cnt FROM \"$TABLE\" WHERE \"$COL_NAME\" IS NOT NULL GROUP BY \"$COL_NAME\" ORDER BY cnt DESC LIMIT 5) x")
                else
                    TOP_5="(Skipped for Date/Time)"
                fi
            fi

            LIMIT_N=$(get_sample_limit "$TABLE" "$COL_NAME" "$DISTINCT_VAL")
            [ -z "$LIMIT_N" ] && LIMIT_N=$DEFAULT_LIMIT
            QUERY_SAMPLE="SELECT (SELECT string_agg(SUBSTR(\"$COL_NAME\"::text, 1, $MAX_TEXT_LEN), ' | ') FROM (SELECT \"$COL_NAME\" FROM \"$TABLE\" WHERE \"$COL_NAME\" IS NOT NULL LIMIT $LIMIT_N) t)"
            SAMPLE=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "$QUERY_SAMPLE")
            
            CLEAN_SAMPLE=$(echo "$SAMPLE" | sed 's/"/""/g'); CLEAN_DEF=$(echo "$DEF_VAL" | sed 's/"/""/g'); CLEAN_COMM=$(echo "$COMMENT" | sed 's/"/""/g' | tr -d '\n'); CLEAN_FK=$(echo "$FK_REF" | sed 's/"/""/g'); CLEAN_MIN=$(echo "$MIN_VAL" | sed 's/"/""/g'); CLEAN_MAX=$(echo "$MAX_VAL" | sed 's/"/""/g'); CLEAN_TOP5=$(echo "$TOP_5" | sed 's/"/""/g' | tr -d '\n')

            echo "$TABLE,$COL_NAME,$COL_TYPE,$IS_PK,\"$CLEAN_FK\",\"$CLEAN_DEF\",\"$CLEAN_COMM\",$TOTAL,$TBL_SIZE,$NULLS,$EMPTIES,$ZEROS,$MAX_LEN,$DISTINCT_VAL,\"$CLEAN_MIN\",\"$CLEAN_MAX\",\"$CLEAN_TOP5\",\"$CLEAN_SAMPLE\"" >> "$REPORT_FILE"
        done
    done
    unset PGPASSWORD
    echo ""
}

# ==============================================================================
# 3. MSSQL Logic (Updated with -C flag)
# ==============================================================================
analyze_mssql() {
    check_command "sqlcmd" "mssql-tools18"
    
    if command -v mssql-scripter &> /dev/null; then
        log_activity "Starting DDL Export..."
        mssql-scripter -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" --schema-and-data schema --file-path "$DDL_FILE" > /dev/null
    else
        log_activity "mssql-scripter not found. Skipping DDL."
    fi

    log_activity "Fetching Tables..."
    
    if [ -n "$SELECTED_TABLES_STR" ]; then
        TABLES_ARRAY=($SELECTED_TABLES_STR)
    else
        # Added -C flag
        RAW_TABLES=$(sqlcmd -S "$DB_HOST,$DB_PORT" -C -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" -h-1 -W -Q "SET NOCOUNT ON; SELECT name FROM sys.tables WHERE type='U' ORDER BY name")
        IFS=$'\n' read -rd '' -a TABLES_ARRAY <<< "$RAW_TABLES"
    fi
    
    TOTAL_TABLES=${#TABLES_ARRAY[@]}
    CURRENT_IDX=0; START_TIME=$(date +%s)

    for TABLE in "${TABLES_ARRAY[@]}"; do
        ((CURRENT_IDX++))
        TABLE=$(echo "$TABLE" | xargs)
        if [ -z "$TABLE" ]; then continue; fi
        
        draw_progress "$CURRENT_IDX" "$TOTAL_TABLES" "$TABLE"
        log_activity "Processing Table: $TABLE"

        TSQL="
        SET NOCOUNT ON;
        DECLARE @TName NVARCHAR(255) = '$TABLE';
        DECLARE @CName NVARCHAR(255), @DType NVARCHAR(100), @SQL NVARCHAR(MAX);
        DECLARE @PK NVARCHAR(10), @FK NVARCHAR(255), @Def NVARCHAR(MAX), @Comm NVARCHAR(MAX);
        DECLARE @DefaultLimit INT = $DEFAULT_LIMIT;
        DECLARE @MaxTextLen INT = $MAX_TEXT_LEN;
        DECLARE @DeepAnalysis VARCHAR(5) = '$DEEP_ANALYSIS';
        
        DECLARE @TableSizeMB DECIMAL(10,2);
        SELECT @TableSizeMB = CAST(SUM(used_page_count) * 8.0 / 1024 AS DECIMAL(10,2)) FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID(@TName);
        SET @TableSizeMB = ISNULL(@TableSizeMB, 0);

        DECLARE cur CURSOR FOR 
            SELECT c.name, ty.name,
                CASE WHEN EXISTS(SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id WHERE i.is_primary_key=1 AND ic.object_id=t.object_id AND ic.column_id=c.column_id) THEN 'YES' ELSE '' END,
                ISNULL((SELECT TOP 1 '-> ' + OBJECT_NAME(fkc.referenced_object_id) + '.' + COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) FROM sys.foreign_key_columns fkc WHERE fkc.parent_object_id=t.object_id AND fkc.parent_column_id=c.column_id), ''),
                ISNULL(object_definition(c.default_object_id), ''), ISNULL(CAST(ep.value AS NVARCHAR(MAX)), '')
            FROM sys.tables t JOIN sys.columns c ON t.object_id = c.object_id JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
            WHERE t.name = @TName ORDER BY c.column_id;

        OPEN cur; FETCH NEXT FROM cur INTO @CName, @DType, @PK, @FK, @Def, @Comm;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                IF @DType NOT IN ('image','text','ntext','binary','geography','geometry','varbinary')
                BEGIN
                    SET @SQL = N'
                    DECLARE @Total BIGINT, @Nulls BIGINT, @Empties BIGINT, @Zeros BIGINT, @MaxLen INT, @Dist BIGINT;
                    DECLARE @MinVal NVARCHAR(MAX), @MaxVal NVARCHAR(MAX), @Top5 NVARCHAR(MAX), @Sample NVARCHAR(MAX);
                    DECLARE @TblSizeMB DECIMAL(10,2) = ' + CAST(@TableSizeMB AS VARCHAR) + ';

                    SELECT
                        @Total = COUNT(*),
                        @Nulls = SUM(CASE WHEN [' + @CName + '] IS NULL THEN 1 ELSE 0 END),
                        @Empties = SUM(CASE WHEN CAST([' + @CName + '] AS NVARCHAR(MAX)) = '''' THEN 1 ELSE 0 END),
                        @Zeros = SUM(CASE WHEN CAST([' + @CName + '] AS NVARCHAR(MAX)) = ''0'' THEN 1 ELSE 0 END),
                        @MaxLen = MAX(LEN([' + @CName + '])),
                        @Dist = COUNT(DISTINCT [' + @CName + '])
                    FROM [' + @TName + '];

                    IF ''' + @DeepAnalysis + ''' = ''true''
                    BEGIN
                        SELECT @MinVal = CAST(MIN([' + @CName + ']) AS NVARCHAR(MAX)), @MaxVal = CAST(MAX([' + @CName + ']) AS NVARCHAR(MAX)) FROM [' + @TName + '];
                        IF ''' + @DType + ''' NOT LIKE ''%date%'' AND ''' + @DType + ''' NOT LIKE ''%time%''
                        BEGIN
                             SELECT @Top5 = STUFF((SELECT '' | '' + CAST(val AS NVARCHAR(MAX)) + '' ('' + CAST(cnt AS NVARCHAR(MAX)) + '')'' FROM (SELECT TOP 5 CAST([' + @CName + '] AS NVARCHAR(MAX)) as val, COUNT(*) as cnt FROM [' + @TName + '] WHERE [' + @CName + '] IS NOT NULL GROUP BY [' + @CName + '] ORDER BY cnt DESC) x FOR XML PATH('''')), 1, 3, '''');
                        END
                        ELSE SET @Top5 = ''(Skipped for Date/Time)'';
                    END

                    DECLARE @Limit INT = ' + CAST(@DefaultLimit AS VARCHAR) + ';
                    IF @Dist = 1 SET @Limit = 1;

                    SELECT @Sample = STUFF((SELECT TOP (@Limit) '' | '' + REPLACE(LEFT(CAST([' + @CName + '] AS NVARCHAR(MAX)), ' + CAST(@MaxTextLen AS VARCHAR) + '), CHAR(34), CHAR(34) + CHAR(34)) FROM [' + @TName + '] WHERE [' + @CName + '] IS NOT NULL FOR XML PATH('''')), 1, 3, '''');

                    PRINT ''' + @TName + ','' + ''' + @CName + ','' + ''' + @DType + ','' + ''' + @PK + ','' + CHAR(34) + REPLACE(REPLACE(''' + REPLACE(@FK,'''','''''') + ''', CHAR(34), CHAR(34) + CHAR(34)), ''NULL'', '''') + CHAR(34) + '','' + CHAR(34) + REPLACE(''' + REPLACE(@Def,'''','''''') + ''', CHAR(34), CHAR(34) + CHAR(34)) + CHAR(34) + '','' + CHAR(34) + REPLACE(''' + REPLACE(@Comm,'''','''''') + ''', CHAR(34), CHAR(34) + CHAR(34)) + CHAR(34) + '','' +
                          CAST(@Total AS VARCHAR) + '','' + CAST(@TblSizeMB AS VARCHAR) + '','' + CAST(@Nulls AS VARCHAR) + '','' + CAST(@Empties AS VARCHAR) + '','' + CAST(@Zeros AS VARCHAR) + '','' +
                          ' + CASE WHEN @DType LIKE '%char%' THEN 'CAST(ISNULL(@MaxLen,0) AS VARCHAR)' ELSE '''0''' END + ' + '','' + CAST(@Dist AS VARCHAR) + '','' + CHAR(34) + ISNULL(REPLACE(@MinVal, CHAR(34), CHAR(34) + CHAR(34)), '''') + CHAR(34) + '','' + CHAR(34) + ISNULL(REPLACE(@MaxVal, CHAR(34), CHAR(34) + CHAR(34)), '''') + CHAR(34) + '','' + CHAR(34) + ISNULL(REPLACE(@Top5, CHAR(34), CHAR(34) + CHAR(34)), '''') + CHAR(34) + '','' + CHAR(34) + ISNULL(REPLACE(@Sample, CHAR(34), CHAR(34) + CHAR(34)), '''') + CHAR(34);
                    ';

                    EXEC(@SQL);
                END
                ELSE
                BEGIN
                    PRINT '$TABLE,' + @CName + ',' + @DType + ',' + @PK + ',' + @FK + ',,,0,' + CAST(@TableSizeMB AS VARCHAR) + ',0,0,0,0,0,\"\",\"\",\"\",\"Skipped BLOB\"';
                END
            END TRY
            BEGIN CATCH
                PRINT '$TABLE,' + @CName + ',' + @DType + ',ERROR,,,,0,' + CAST(@TableSizeMB AS VARCHAR) + ',0,0,0,0,0,\"\",\"\",\"\",\"Error: ' + REPLACE(ERROR_MESSAGE(), ',', ';') + '\"';
            END CATCH
            FETCH NEXT FROM cur INTO @CName, @DType, @PK, @FK, @Def, @Comm;
        END
        CLOSE cur; DEALLOCATE cur;
        "
        # Added -C flag
        sqlcmd -S "$DB_HOST,$DB_PORT" -C -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" -h-1 -W -Q "$TSQL" >> "$REPORT_FILE"
    done
    echo ""
}

# --- MAIN ---
case $DB_CHOICE in 1) analyze_mysql ;; 2) analyze_postgres ;; 3) analyze_mssql ;; *) log_activity "Invalid"; exit 1 ;; esac

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