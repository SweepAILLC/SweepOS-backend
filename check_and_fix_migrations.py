#!/usr/bin/env python3
"""
Script to check database migration status and fix if needed.
Run this to ensure the database is at the correct migration version.
"""
import sys
import os

# Add the backend directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from app.core.config import settings

def check_migration_status():
    """Check current migration status"""
    engine = create_engine(settings.DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # Check if alembic_version table exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'alembic_version'
                )
            """))
            table_exists = result.scalar()
            
            if not table_exists:
                print("❌ Alembic version table does not exist. Database may not be initialized.")
                print("   Run: make migrate-up")
                return False
            
            # Get current version
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            current_version = result.scalar()
            
            print(f"✅ Current database version: {current_version}")
            print(f"   Expected latest version: 015")
            
            if current_version != '015':
                print(f"⚠️  Database is not at the latest version!")
                print(f"   Run: make migrate-up")
                return False
            else:
                print("✅ Database is at the correct version (015)")
                return True
                
    except Exception as e:
        print(f"❌ Error checking migration status: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        engine.dispose()

if __name__ == "__main__":
    print("Checking database migration status...")
    print(f"Database URL: {settings.DATABASE_URL.split('@')[1] if '@' in settings.DATABASE_URL else 'hidden'}")
    print()
    
    success = check_migration_status()
    
    if not success:
        print("\nTo fix, run:")
        print("  make migrate-up")
        sys.exit(1)
    else:
        print("\n✅ Database migration status is correct!")
        sys.exit(0)


