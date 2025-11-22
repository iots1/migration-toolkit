#!/bin/bash

# ==============================================================================
# HIS DATABASE MIGRATION ANALYZER (v2.0)
# รองรับ: MySQL, PostgreSQL, Microsoft SQL Server
# Feature: Structure Analysis (PK/FK/Default/Comment) + Data Profiling
# ==============================================================================

# --- CONFIGURATION & SETUP ---
OUTPUT_DIR="./migration_report"
mkdir -p "$OUTPUT_DIR"
DATE_NOW=$(date +"%Y%m%d_%H%M")
REPORT_FILE="$OUTPUT_DIR/data_profile_$DATE_NOW.csv"
DDL_FILE="$OUTPUT_DIR/ddl_schema_$DATE_NOW.sql"

# เขียน Header ของไฟล์ CSV (เพิ่ม PK, FK, Default, Comment)
echo "Table,Column,DataType,PK,FK,Default,Comment,Total_Rows,Null_Count,Max_Length,Distinct_Values,Sample_Values" > "$REPORT_FILE"

# ------------------------------------------------------------------------------
# FUNCTION: Check Command
# ------------------------------------------------------------------------------
check_command() {
    local cmd="$1"
    local brew_pkg="$2"

    if ! command -v "$cmd" &> /dev/null; then
        echo "❌ Error: ไม่พบคำสั่ง '$cmd'"
        
        if command -v brew &> /dev/null && [ -n "$brew_pkg" ]; then
            echo "🍺 ตรวจพบ Homebrew..."
            read -p "❓ ต้องการติดตั้ง '$brew_pkg' เดี๋ยวนี้หรือไม่? (y/N): " install_choice
            if [[ "$install_choice" =~ ^[Yy]$ ]]; then
                echo "📦 Installing $brew_pkg ..."
                brew install "$brew_pkg"
                
                BREW_PREFIX=$(brew --prefix)
                POSSIBLE_PATHS=("$BREW_PREFIX/opt/$brew_pkg/bin" "$BREW_PREFIX/Cellar/$brew_pkg/*/bin")
                for p in "${POSSIBLE_PATHS[@]}"; do
                    for expanded_path in $p; do
                        if [ -f "$expanded_path/$cmd" ]; then
                            echo "🔗 Linking binary from $expanded_path"
                            export PATH="$expanded_path:$PATH"
                            break 2
                        fi
                    done
                done

                if command -v "$cmd" &> /dev/null; then return 0; else echo "❌ Failed to link path."; exit 1; fi
            else
                exit 1
            fi
        else
            echo "❌ Please install '$brew_pkg' manually."
            exit 1
        fi
    fi
}

# --- LOAD .ENV ---
if [ -f .env ]; then export $(grep -v '^#' .env | xargs); fi

echo "========================================="
echo "   🏥 HIS Database Migration Analyzer    "
echo "========================================="

# --- MENU & INPUTS ---
if [ -z "$DB_CHOICE" ]; then
    echo "1) MySQL / MariaDB"
    echo "2) PostgreSQL"
    echo "3) Microsoft SQL Server"
    read -p "Select Database [1-3]: " DB_CHOICE
fi

if [ -z "$DB_HOST" ]; then read -p "Host [localhost]: " DB_HOST; DB_HOST=${DB_HOST:-localhost}; fi
case $DB_CHOICE in 1) D_PORT="3306";; 2) D_PORT="5432";; 3) D_PORT="1433";; esac
if [ -z "$DB_PORT" ]; then read -p "Port [$D_PORT]: " DB_PORT; DB_PORT=${DB_PORT:-$D_PORT}; fi
if [ -z "$DB_NAME" ]; then read -p "Database Name: " DB_NAME; fi
if [ -z "$DB_USER" ]; then read -p "Username: " DB_USER; fi
if [ -z "$DB_PASS" ]; then read -s -p "Password: " DB_PASS; echo ""; fi

echo "-----------------------------------------"
echo "🔌 Target: $DB_NAME @ $DB_HOST:$DB_PORT"
echo "📂 Report: $REPORT_FILE"
echo "-----------------------------------------"

# ==============================================================================
# 1. MySQL Logic
# ==============================================================================
analyze_mysql() {
    check_command "mysql" "mysql-client"
    check_command "mysqldump" "mysql-client"

    echo "[1/2] Generating DDL..."
    mysqldump -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" --no-data --routines --triggers "$DB_NAME" > "$DDL_FILE" 2>/dev/null

    echo "[2/2] Profiling Data (MySQL)..."
    TABLES=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "SHOW TABLES")

    for TABLE in $TABLES; do
        echo "      Processing: $TABLE"
        # Query ดึง Metadata: PK, FK, Default, Comment
        COLUMNS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
            SELECT 
                c.COLUMN_NAME, 
                c.DATA_TYPE,
                IF(c.COLUMN_KEY='PRI', 'YES', '') as IS_PK,
                (SELECT IF(COUNT(*)>0,'YES','') FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k WHERE k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.REFERENCED_TABLE_NAME IS NOT NULL) as IS_FK,
                IFNULL(c.COLUMN_DEFAULT, '') as DEF_VAL,
                c.COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = '$DB_NAME' AND c.TABLE_NAME = '$TABLE' 
            ORDER BY c.ORDINAL_POSITION")
        
        # Parse output line by line (Tab separated)
        echo "$COLUMNS" | while IFS=$'\t' read -r COL_NAME COL_TYPE IS_PK IS_FK DEF_VAL COMMENT; do
            # Data Stats
            STATS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                SELECT COUNT(*), SUM(IF(\`$COL_NAME\` IS NULL,1,0)), MAX(LENGTH(\`$COL_NAME\`)), COUNT(DISTINCT \`$COL_NAME\`) FROM \`$TABLE\`;")
            
            # Sample
            SAMPLE=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -N -B -e "
                SELECT GROUP_CONCAT(LEFT(val, 50) SEPARATOR ' | ') FROM (SELECT \`$COL_NAME\` as val FROM \`$TABLE\` WHERE \`$COL_NAME\` IS NOT NULL LIMIT 3) x;")
            
            # Clean CSV Text
            STATS_FMT=$(echo "$STATS" | tr '\t' ',')
            SAMPLE_CLEAN=$(echo "$SAMPLE" | sed 's/"/""/g' | tr -d '\n')
            DEF_CLEAN=$(echo "$DEF_VAL" | sed 's/"/""/g')
            COMMENT_CLEAN=$(echo "$COMMENT" | sed 's/"/""/g')

            echo "$TABLE,$COL_NAME,$COL_TYPE,$IS_PK,$IS_FK,\"$DEF_CLEAN\",\"$COMMENT_CLEAN\",$STATS_FMT,\"$SAMPLE_CLEAN\"" >> "$REPORT_FILE"
        done
    done
}

# ==============================================================================
# 2. PostgreSQL Logic
# ==============================================================================
analyze_postgres() {
    check_command "psql" "libpq"
    check_command "pg_dump" "libpq"
    export PGPASSWORD="$DB_PASS"

    echo "[1/2] Generating DDL..."
    pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -s "$DB_NAME" > "$DDL_FILE" 2>/dev/null

    echo "[2/2] Profiling Data (PostgreSQL)..."
    TABLES=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -c "
        SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")

    for TABLE in $TABLES; do
        echo "      Processing: $TABLE"
        # Query Metadata ซับซ้อนหน่อยสำหรับ PG
        COLUMNS=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -F "|" -c "
            SELECT 
                c.column_name, 
                c.data_type,
                (SELECT 'YES' FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name=tc.constraint_name WHERE kcu.table_name=c.table_name AND kcu.column_name=c.column_name AND tc.constraint_type='PRIMARY KEY' LIMIT 1),
                (SELECT 'YES' FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name=tc.constraint_name WHERE kcu.table_name=c.table_name AND kcu.column_name=c.column_name AND tc.constraint_type='FOREIGN KEY' LIMIT 1),
                COALESCE(c.column_default, ''),
                pg_catalog.col_description(format('%I.%I', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position)
            FROM information_schema.columns c 
            WHERE c.table_schema = 'public' AND c.table_name = '$TABLE' 
            ORDER BY c.ordinal_position")

        echo "$COLUMNS" | while IFS="|" read -r COL_NAME COL_TYPE IS_PK IS_FK DEF_VAL COMMENT; do
            QUERY="SELECT COUNT(*), COUNT(*) - COUNT(\"$COL_NAME\"), MAX(LENGTH(CAST(\"$COL_NAME\" AS TEXT))), COUNT(DISTINCT \"$COL_NAME\"), (SELECT string_agg(SUBSTR(\"$COL_NAME\"::text, 1, 50), ' | ') FROM (SELECT \"$COL_NAME\" FROM \"$TABLE\" WHERE \"$COL_NAME\" IS NOT NULL LIMIT 3) t) FROM \"$TABLE\""
            RESULT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -A -F"," -c "$QUERY")
            
            TOTAL=$(echo $RESULT | cut -d, -f1)
            NULLS=$(echo $RESULT | cut -d, -f2)
            MAX_LEN=$(echo $RESULT | cut -d, -f3)
            DISTINCT=$(echo $RESULT | cut -d, -f4)
            SAMPLE=$(echo $RESULT | cut -d, -f5-)
            
            SAMPLE_CLEAN=$(echo "$SAMPLE" | sed 's/"/""/g')
            DEF_CLEAN=$(echo "$DEF_VAL" | sed 's/"/""/g')
            COMMENT_CLEAN=$(echo "$COMMENT" | sed 's/"/""/g' | tr -d '\n')

            echo "$TABLE,$COL_NAME,$COL_TYPE,$IS_PK,$IS_FK,\"$DEF_CLEAN\",\"$COMMENT_CLEAN\",$TOTAL,$NULLS,$MAX_LEN,$DISTINCT,\"$SAMPLE_CLEAN\"" >> "$REPORT_FILE"
        done
    done
    unset PGPASSWORD
}

# ==============================================================================
# 3. MSSQL Logic
# ==============================================================================
analyze_mssql() {
    check_command "sqlcmd"
    if command -v mssql-scripter &> /dev/null; then
        echo "[1/2] Generating DDL..."
        mssql-scripter -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASS" -d "$DB_NAME" --schema-and-data schema --file-path "$DDL_FILE" > /dev/null
    else
        echo "[1/2] Skipping DDL..."
    fi

    echo "[2/2] Profiling Data (MSSQL)..."
    # Update T-SQL to join extended properties and constraints
    TSQL="
    SET NOCOUNT ON;
    DECLARE @TName NVARCHAR(255), @CName NVARCHAR(255), @DType NVARCHAR(100), @SQL NVARCHAR(MAX);
    DECLARE @PK NVARCHAR(10), @FK NVARCHAR(10), @Def NVARCHAR(MAX), @Comm NVARCHAR(MAX);

    DECLARE cur CURSOR FOR 
        SELECT t.name, c.name, ty.name,
            CASE WHEN EXISTS(SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id WHERE i.is_primary_key=1 AND ic.object_id=t.object_id AND ic.column_id=c.column_id) THEN 'YES' ELSE '' END,
            CASE WHEN EXISTS(SELECT 1 FROM sys.foreign_key_columns fkc WHERE fkc.parent_object_id=t.object_id AND fkc.parent_column_id=c.column_id) THEN 'YES' ELSE '' END,
            ISNULL(object_definition(c.default_object_id), ''),
            ISNULL(ep.value, '')
        FROM sys.tables t 
        JOIN sys.columns c ON t.object_id = c.object_id 
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
        WHERE t.type='U' ORDER BY t.name;

    OPEN cur;
    FETCH NEXT FROM cur INTO @TName, @CName, @DType, @PK, @FK, @Def, @Comm;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
             IF @DType NOT IN ('image','text','ntext','binary','geography','geometry')
             BEGIN
                SET @SQL = N'SELECT ''' + @TName + ''',''' + @CName + ''',''' + @DType + ''',''' + @PK + ''',''' + @FK + ''',''' + REPLACE(@Def,'''','''''') + ''',''' + REPLACE(@Comm,'''','''''') + ''',' +
                           N'CAST(COUNT(*) AS VARCHAR) + '','' + ' +
                           N'CAST(SUM(CASE WHEN [' + @CName + '] IS NULL THEN 1 ELSE 0 END) AS VARCHAR) + '','' + ' +
                           CASE WHEN @DType LIKE '%char%' THEN N'CAST(MAX(LEN([' + @CName + '])) AS VARCHAR) + '','' + ' ELSE N'0,'','' + ' END +
                           N'CAST(COUNT(DISTINCT [' + @CName + ']) AS VARCHAR) + '','' + ' +
                           N'\"' + CAST((SELECT TOP 1 REPLACE(LEFT(CAST([' + @CName + '] AS NVARCHAR(MAX)), 50), '\"', '\"\"') FROM [' + @TName + '] WHERE [' + @CName + '] IS NOT NULL) AS NVARCHAR(MAX)) + '\"' +
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
}

# --- MAIN ---
case $DB_CHOICE in
    1) analyze_mysql ;;
    2) analyze_postgres ;;
    3) analyze_mssql ;;
    *) echo "❌ Invalid Selection"; exit 1 ;;
esac

echo "========================================="
echo "✅ Analysis Complete!"
echo "📄 DDL: $DDL_FILE"
echo "📊 CSV: $REPORT_FILE"

if [ -f "csv_to_html.py" ]; then
    echo "🌍 Generating HTML Report..."
    python3 csv_to_html.py "$REPORT_FILE"
fi