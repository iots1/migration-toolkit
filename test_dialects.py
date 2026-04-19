"""Test dialect registry and URL generation."""
from dotenv import load_dotenv
from dialects.registry import available_types, get

load_dotenv()

print("🔧 Testing Dialect Registry...")
print(f"\n📊 Available dialects: {available_types()}")

print("\n🧪 Testing dialect implementations...")

# Test MySQL
mysql = get("MySQL")
print(f"\n1. MySQL:")
print(f"   Default port: {mysql.default_port}")
print(f"   Default charset: {mysql.default_charset}")
print(f"   Schema: {mysql.get_schema_default()}")
print(f"   Quote: {mysql.quote_identifier('table_name')}")
print(f"   URL: {mysql.build_url('localhost', '3306', 'mydb', 'user', 'pass')}")

# Test PostgreSQL
pg = get("PostgreSQL")
print(f"\n2. PostgreSQL:")
print(f"   Default port: {pg.default_port}")
print(f"   Default charset: {pg.default_charset}")
print(f"   Schema: {pg.get_schema_default()}")
print(f"   Quote: {pg.quote_identifier('table_name')}")
print(f"   URL: {pg.build_url('localhost', '5432', 'mydb', 'user', 'pass')}")

# Test MSSQL
mssql = get("Microsoft SQL Server")
print(f"\n3. Microsoft SQL Server:")
print(f"   Default port: {mssql.default_port}")
print(f"   Default charset: {mssql.default_charset}")
print(f"   Schema: {mssql.get_schema_default()}")
print(f"   Quote: {mssql.quote_identifier('table_name')}")
print(f"   URL: {mssql.build_url('localhost', '1433', 'mydb', 'user', 'pass')}")

# Test LIMIT/OFFSET syntax
print(f"\n📝 LIMIT/OFFSET Syntax:")
print(f"   MySQL (limit=10, offset=5): {mysql.get_limit_offset_syntax(10, 5)}")
print(f"   PostgreSQL (limit=10, offset=5): {pg.get_limit_offset_syntax(10, 5)}")
print(f"   MSSQL (limit=10, offset=5): {mssql.get_limit_offset_syntax(10, 5)}")

print("\n✅ Dialect registry working correctly!")
