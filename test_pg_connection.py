"""
Test PostgreSQL connection before proceeding with migration.

Run this to verify your DATABASE_URL is correct:
    python test_pg_connection.py
"""
import os
from dotenv import load_dotenv
from repositories.connection import test_connection

# Load environment
load_dotenv()

print("🔧 Testing PostgreSQL connection...")
print(f"   DATABASE_URL: {os.environ.get('DATABASE_URL', 'NOT SET')}")

ok, msg = test_connection()

if ok:
    print(f"   {msg}")
    print("\n✅ Ready to proceed with Phase 2!")
else:
    print(f"   {msg}")
    print("\n❌ Please fix your database connection:")
    print("   1. Create database: CREATE DATABASE his_analyzer;")
    print("   2. Create user: CREATE USER his_user WITH PASSWORD 'your_password';")
    print("   3. Grant privileges: GRANT ALL PRIVILEGES ON DATABASE his_analyzer TO his_user;")
    print("   4. Update .env file with correct DATABASE_URL")
