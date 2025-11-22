#!/usr/bin/env bash

# ==============================================================================
# HIS DATABASE MIGRATION ANALYZER (v4.3 - Auto Bash Switch & Logging)
# Feature: 
#   1. Auto-detect & Switch to Homebrew Bash (Fixes 3.2 issue on macOS)
#   2. Process Logging to file
#   3. Detailed Progress Bar with Timer
#   4. Optimized Config Lookup (Fast execution)
# ==============================================================================

# --- [CRITICAL] AUTO-SWITCH BASH VERSION ---
# หากกำลังรันบน Bash เก่า (macOS Default 3.2) ให้ลองหา Bash ใหม่แล้ว Switch ทันที
if [ -z "$BASH_VERSINFO" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
    CANDIDATE_PATHS=(
        "/opt/homebrew/bin/bash"  # Apple Silicon
        "/usr/local/bin/bash"     # Intel Mac / Linux
        "/usr/bin/bash"           # Standard Linux
    )
    for NEW_BASH in "${CANDIDATE_PATHS[@]}"; do
        if [ -x "$NEW_BASH" ]; then
            # ตรวจสอบว่าเป็น Bash จริงๆ และเวอร์ชันใหม่กว่า
            VER=$("$NEW_BASH" --version | head -n 1 | grep -oE '[0-9]\.[0-9]+' | head -n 1)
            MAJOR=${VER%%.*}
            
            if [ "$MAJOR" -ge 4 ]; then
                # เจอตัวใหม่! สั่ง exec ใหม่ด้วยตัวนี้ทันที (ส่งต่อ arguments เดิมทั้งหมด)
                exec "$NEW_BASH" "$0" "$@"
            fi
        fi
    done
    # ถ้าหาไม่เจอ จะรันต่อไปด้วย Bash 3.2
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
echo "Shell Version: $BASH_VERSION" >> "$LOG_FILE"
echo "----------------------------------------------------------------" >> "$LOG_FILE"

# Function to write log
log_activity() {
    local msg="$1"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] $msg" >> "$LOG_FILE"
}

# Header CSV
echo "Table,Column,DataType,PK,FK,Default,Comment,Total_Rows,Null_Count,Max_Length,Distinct_Values,Sample_Values" > "$REPORT_FILE"

# ------------------------------------------------------------------------------
# FUNCTION: Draw Progress Bar
# ------------------------------------------------------------------------------
START_TIME=$(date +%s)

draw_progress() {
    # params: $1=current_step, $2=total_steps, $3=table_name
    local current=$1
    local total=$2
    local table_name=$3
    
    local percent=0
    if [ "$total" -gt 0 ]; then
        percent=$(( 100 * current / total ))
    fi
    
    local now=$(date +%s)
    local elapsed=$(( now - START_TIME ))
    local min=$(( elapsed / 60 ))
    local sec=$(( elapsed % 60 ))
    local time_str=$(printf "%02d:%02d" $min $sec)

    local width=25
    local filled=$(( width * percent / 100 ))
    local empty=$(( width - filled ))
    
    local bar="["
    for ((i=0; i<filled; i++)); do bar+="="; done
    bar+=">"
    for ((i=0; i<empty; i++)); do bar+=" "; done
    bar+="]"

    # Print Format: [=====>] 50% [Table 5/10] (Time: 00:05) -> Processing: table_name
    printf "\r\033[K%s %3d%% [Table %s/%s] (Time: %s) -> Processing: %s" "$bar" "$percent" "$current" "$total" "$time_str" "$table_name"
}

# ------------------------------------------------------------------------------
# FUNCTION: Check Dependencies
# ------------------------------------------------------------------------------
check_command() {
    local cmd="$1"
    local brew_pkg="$2"
    
    # Auto-detect Homebrew Keg-Only Paths
    if [ -n "$brew_pkg" ] && command -v brew &> /dev/null; then
         BREW_PREFIX=$(brew --prefix)
         POSSIBLE_PATHS=("$BREW_PREFIX/opt/$brew_pkg/bin" "$BREW_PREFIX/Cellar/$brew_pkg/*/bin")
         for p in "${POSSIBLE_PATHS[@]}"; do
             for expanded_path in $p; do
                 if [ -x "$expanded_path/$cmd" ]; then
                     export PATH="$expanded_path:$PATH"
                     break 2
                 fi
             done
         done
    fi

    if ! command -v "$cmd" &> /dev/null; then
        log_activity "Error: Command '$cmd' not found."
        echo "❌ Error: ไม่พบคำสั่ง '$cmd'"
        if command -v brew &> /dev/null && [ -n "$brew_pkg" ]; then
            echo "🍺 ตรวจพบ Homebrew..."
            read -p "❓ ต้องการติดตั้ง '$brew_pkg' เดี๋ยวนี้หรือไม่? (y/N): " install_choice
            if [[ "$install_choice" =~ ^[Yy]$ ]]; then
                echo "📦 Installing $brew_pkg ..."
                brew install "$brew_pkg"
                echo "✅ ติดตั้งเสร็จสิ้น กรุณารันสคริปต์ใหม่อีกครั้ง"
                exit 0
            else
                exit 1
            fi
        else
            echo "❌ กรุณาติดตั้ง '$brew_pkg' หรือ '$cmd' ด้วยตนเอง"
            if [ "$cmd" == "jq" ]; then echo "   (Try: brew install jq)"; fi
            exit 1
        fi
    fi
}

# --- INITIAL CHECKS ---
check_command "jq" "jq"

# Check Bash version log
if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
    log_activity "Warning: Still running on Bash $BASH_VERSION despite auto-switch attempt."
else
    log_activity "Running on Bash $BASH_VERSION"
fi

# ------------------------------------------------------------------------------
# LOAD CONFIGURATION
# ------------------------------------------------------------------------------
CONFIG_FILE="config.json"
if [ ! -f "$CONFIG_FILE" ]; then 
    log_activity "Error: config.json not found."
    echo "❌ Error: ไม่พบไฟล์ $CONFIG_FILE"; exit 1; 
fi

log_activity "Loading configuration from $CONFIG_FILE"

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
    *) log_activity "Error: Unknown database type $DB_TYPE"; echo "❌ Error: Unknown database type '$DB_TYPE'"; exit 1 ;;
esac

DEFAULT_LIMIT=$(jq -r '.sampling.default_limit // 10' "$CONFIG_FILE")
MAX_TEXT_LEN=$(jq -r '.sampling.max_text_length // 300' "$CONFIG_FILE")

# Load Exceptions into String (Universal Method compatible with Bash 3.2 & 4.0)
EXCEPTIONS_STRING=$(jq -r '.sampling.exceptions[] | "\(.table).\(.column)=\(.limit)|"' "$CONFIG_FILE" | tr -d '\n')
EXCEPTIONS_COUNT=$(jq '.sampling.exceptions | length' "$CONFIG_FILE")

log_activity "Target: $DB_NAME ($DB_TYPE) @ $DB_HOST:$DB_PORT"
log_activity "Config: Default Limit=$DEFAULT_LIMIT, Exceptions Count=$EXCEPTIONS_COUNT"

# Hybrid Helper function
get_sample_limit() {
    local tbl="$1"
    local col="$2"
    local distinct_val="$3"

    if [ "$distinct_val" == "1" ]; then
        echo "1"
        return
    fi

    # Optimized String Search (No grep/subshells)
    local search_key="$tbl.$col="
    if [[ "$EXCEPTIONS_STRING" == *"$search_key"* ]]; then
        local temp="${EXCEPTIONS_STRING#*${search_key}}"
        local val="${temp%%|*}"
        echo "$val"
        return
    fi
    echo "$DEFAULT_LIMIT"
}

echo "========================================="
echo "   🏥 HIS Database Migration Analyzer    "
echo "========================================="
echo "🐚 Shell: Bash $BASH_VERSION"
echo "🔌 Target: $DB_NAME ($DB_TYPE) @ $DB_HOST:$DB_PORT"
echo "🛠  Config: Default Limit=$DEFAULT_LIMIT, Exceptions=$EXCEPTIONS_COUNT"
echo "📂 Output: $RUN_DIR"
echo "-----------------------------------------"

# ==============================================================================
# 1. MySQL Logic
# ==============================================================================
analyze_mysql() {
    check_command "mysql" "mysql-client"
    check_command "mysqldump" "mysql-client"

    echo "[Step 1/2] Generating DDL..."
    log_activity "Starting DDL Export..."
    if mysqldump -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" --no-data --routines --triggers "$DB_NAME" > "$DDL_FILE" 2>/dev/null; then
        log_activity "DDL Export Success: $DDL_FILE"
    else
        log_activity "DDL Export Failed"
    fi

    echo "[Step 2/2] Profiling Data (MySQL)..."
    RAW_TABLES=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "SHOW TABLES")
    TABLES_ARRAY=($RAW_TABLES)
    TOTAL_TABLES=${#TABLES_ARRAY[@]}
    
    log_activity "Found $TOTAL_TABLES tables to process."
    
    CURRENT_IDX=0
    START_TIME=$(date +%s)

    for TABLE in "${TABLES_ARRAY[@]}"; do
        ((CURRENT_IDX++))
        draw_progress "$CURRENT_IDX" "$TOTAL_TABLES" "$TABLE"
        log_activity "Processing Table [$CURRENT_IDX/$TOTAL_TABLES]: $TABLE"
        
        COLUMNS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
            SELECT c.COLUMN_NAME, c.DATA_TYPE, IF(c.COLUMN_KEY='PRI', 'YES', '') as IS_PK,
                (SELECT CONCAT('-> ', k.REFERENCED_TABLE_NAME, '.', k.REFERENCED_COLUMN_NAME) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k WHERE k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.REFERENCED_TABLE_NAME IS NOT NULL LIMIT 1) as FK_REF,
                IFNULL(c.COLUMN_DEFAULT, '') as DEF_VAL, c.COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = '$DB_NAME' AND c.TABLE_NAME = '$TABLE' ORDER BY c.ORDINAL_POSITION")
        
        echo "$COLUMNS" | while IFS=$'\t' read -r COL_NAME COL_TYPE IS_PK FK_REF DEF_VAL COMMENT; do
            STATS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                SELECT COUNT(*), SUM(IF(\`$COL_NAME\` IS NULL,1,0)), MAX(LENGTH(\`$COL_NAME\`)), COUNT(DISTINCT \`$COL_NAME\`) FROM \`$TABLE\`;")
            
            DISTINCT_VAL=$(echo "$STATS" | awk '{print $4}')
            LIMIT_N=$(get_sample_limit "$TABLE" "$COL_NAME" "$DISTINCT_VAL")
            [ -z "$LIMIT_N" ] && LIMIT_N=$DEFAULT_LIMIT

            SAMPLE=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                SELECT GROUP_CONCAT(LEFT(val, $MAX_TEXT_LEN) SEPARATOR ' | ') FROM (SELECT \`$COL_NAME\` as val FROM \`$TABLE\` WHERE \`$COL_NAME\` IS NOT NULL LIMIT $LIMIT_N) x;")
            
            STATS_FMT=$(echo "$STATS" | tr '\t' ',')
            SAMPLE_CLEAN=$(echo "$SAMPLE" | sed 's/"/""/g' | tr -d '\n')
            DEF_CLEAN=$(echo "$DEF_VAL" | sed 's/"/""/g')
            COMMENT_CLEAN=$(echo "$COMMENT" | sed 's/"/""/g')
            FK_FINAL=$(echo "$FK_REF" | sed 's/NULL//g')
            echo "$TABLE,$COL_NAME,$COL_TYPE,$IS_PK,\"$FK_FINAL\",\"$DEF_CLEAN\",\"$COMMENT_CLEAN\",$STATS_FMT,\"$SAMPLE_CLEAN\"" >> "$REPORT_FILE"
        done
    done
    echo ""
    log_activity "Analysis Completed Successfully."
}

# ==============================================================================
# 2. PostgreSQL Logic
# ==============================================================================
analyze_postgres() {
    check_command "psql" "libpq"
    check_command "pg_dump" "libpq"
    export PGPASSWORD="$DB_PASS"

    echo "[Step 1/2] Generating DDL..."
    log_activity "Starting DDL Export (pg_dump)..."
    if pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -s "$DB_NAME" > "$DDL_FILE" 2>/dev/null; then
        log_activity "DDL Export Success: $DDL_FILE"
    else
        log_activity "DDL Export Failed (Check connection or permissions)"
    fi

    echo "[Step 2/2] Profiling Data (PostgreSQL)..."
    RAW_TABLES=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "
        SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
    TABLES_ARRAY=($RAW_TABLES)
    TOTAL_TABLES=${#TABLES_ARRAY[@]}
    
    log_activity "Found $TOTAL_TABLES tables to process."
    
    CURRENT_IDX=0
    START_TIME=$(date +%s)

    for TABLE in "${TABLES_ARRAY[@]}"; do
        ((CURRENT_IDX++))
        draw_progress "$CURRENT_IDX" "$TOTAL_TABLES" "$TABLE"
        log_activity "Processing Table [$CURRENT_IDX/$TOTAL_TABLES]: $TABLE"

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

            LIMIT_N=$(get_sample_limit "$TABLE" "$COL_NAME" "$DISTINCT_VAL")
            [ -z "$LIMIT_N" ] && LIMIT_N=$DEFAULT_LIMIT

            QUERY_SAMPLE="SELECT (SELECT string_agg(SUBSTR(\"$COL_NAME\"::text, 1, $MAX_TEXT_LEN), ' | ') FROM (SELECT \"$COL_NAME\" FROM \"$TABLE\" WHERE \"$COL_NAME\" IS NOT NULL LIMIT $LIMIT_N) t)"
            SAMPLE=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "$QUERY_SAMPLE")
            
            SAMPLE_CLEAN=$(echo "$SAMPLE" | sed 's/"/""/g')
            DEF_CLEAN=$(echo "$DEF_VAL" | sed 's/"/""/g')
            COMMENT_CLEAN=$(echo "$COMMENT" | sed 's/"/""/g' | tr -d '\n')
            FK_FINAL=$(echo "$FK_REF" | sed 's/"/""/g')
            echo "$TABLE,$COL_NAME,$COL_TYPE,$IS_PK,\"$FK_FINAL\",\"$DEF_CLEAN\",\"$COMMENT_CLEAN\",$TOTAL,$NULLS,$MAX_LEN,$DISTINCT_VAL,\"$SAMPLE_CLEAN\"" >> "$REPORT_FILE"
        done
    done
    unset PGPASSWORD
    echo ""
    log_activity "Analysis Completed Successfully."
}

# ==============================================================================
# 3. MSSQL Logic
# ==============================================================================
analyze_mssql() {
    check_command "sqlcmd"
    log_activity "Starting MSSQL Analysis..."
    
    TSQL="
    SET NOCOUNT ON;
    DECLARE @TName NVARCHAR(255), @CName NVARCHAR(255), @DType NVARCHAR(100), @SQL NVARCHAR(MAX);
    DECLARE @PK NVARCHAR(10), @FK NVARCHAR(255), @Def NVARCHAR(MAX), @Comm NVARCHAR(MAX);
    DECLARE @DefaultLimit INT = $DEFAULT_LIMIT;
    DECLARE @MaxTextLen INT = $MAX_TEXT_LEN;
    DECLARE cur CURSOR FOR 
        SELECT t.name, c.name, ty.name,
            CASE WHEN EXISTS(SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id WHERE i.is_primary_key=1 AND ic.object_id=t.object_id AND ic.column_id=c.column_id) THEN 'YES' ELSE '' END,
            ISNULL((SELECT TOP 1 '-> ' + OBJECT_NAME(fkc.referenced_object_id) + '.' + COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) FROM sys.foreign_key_columns fkc WHERE fkc.parent_object_id=t.object_id AND fkc.parent_column_id=c.column_id), ''),
            ISNULL(object_definition(c.default_object_id), ''), ISNULL(ep.value, '')
        FROM sys.tables t JOIN sys.columns c ON t.object_id = c.object_id JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
        WHERE t.type='U' ORDER BY t.name;

    OPEN cur; FETCH NEXT FROM cur INTO @TName, @CName, @DType, @PK, @FK, @Def, @Comm;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
             IF @DType NOT IN ('image','text','ntext','binary','geography','geometry')
             BEGIN
                SET @SQL = N'
                DECLARE @Dist BIGINT = (SELECT COUNT(DISTINCT [' + @CName + ']) FROM [' + @TName + ']);
                DECLARE @Limit INT = ' + CAST(@DefaultLimit AS VARCHAR) + ';
                IF @Dist = 1 SET @Limit = 1;
                SELECT ''' + @TName + ''',''' + @CName + ''',''' + @DType + ''',''' + @PK + ''',''' + @FK + ''',''' + REPLACE(@Def,'''','''''') + ''',''' + REPLACE(@Comm,'''','''''') + ''',' +
                           N'CAST(COUNT(*) AS VARCHAR) + '','' + ' + N'CAST(SUM(CASE WHEN [' + @CName + '] IS NULL THEN 1 ELSE 0 END) AS VARCHAR) + '','' + ' +
                           CASE WHEN @DType LIKE '%char%' THEN N'CAST(MAX(LEN([' + @CName + '])) AS VARCHAR) + '','' + ' ELSE N'0,'','' + ' END +
                           N'CAST(@Dist AS VARCHAR) + '','' + ' +
                           N'\"' + CAST((SELECT TOP (@Limit) REPLACE(LEFT(CAST([' + @CName + '] AS NVARCHAR(MAX)), ' + CAST(@MaxTextLen AS VARCHAR) + '), '\"', '\"\"') FROM [' + @TName + '] WHERE [' + @CName + '] IS NOT NULL FOR XML PATH('''')) AS NVARCHAR(MAX)) + '\"' +
                           N' FROM [' + @TName + ']';
                EXEC(@SQL);
             END
             ELSE BEGIN PRINT @TName + ',' + @CName + ',' + @DType + ',' + @PK + ',' + @FK + ',,SKIPPED_BLOB,0,0,0,0,\"\"'; END
        END TRY
        BEGIN CATCH PRINT @TName + ',' + @CName + ',' + @DType + ',ERROR,ERROR,ERROR,ERROR,-1,-1,-1,-1,\"ERROR\"'; END CATCH
        FETCH NEXT FROM cur INTO @TName, @CName, @DType, @PK, @FK, @Def, @Comm;
    END
    CLOSE cur; DEALLOCATE cur;
    "
    sqlcmd -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" -W -h-1 -Q "$TSQL" -s "," >> "$REPORT_FILE"
    log_activity "MSSQL Analysis logic executed."
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
    
    # Auto Open HTML File
    HTML_FILE="${REPORT_FILE%.csv}.html"
    if [ -f "$HTML_FILE" ]; then
        if command -v open &> /dev/null; then open "$HTML_FILE"
        elif command -v xdg-open &> /dev/null; then xdg-open "$HTML_FILE"
        elif command -v wslview &> /dev/null; then wslview "$HTML_FILE"
        elif command -v explorer.exe &> /dev/null; then explorer.exe `wslpath -w "$HTML_FILE"`
        fi
    fi
fi