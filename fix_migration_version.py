#!/usr/bin/env python3
"""
Script to fix database migration version mismatch.
If the database thinks it's at version 017 but that doesn't exist,
we'll reset it to the latest actual version (015).
"""
import sys
import os

# Add the backend directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from app.core.config import settings

def fix_migration_version():
    """Fix the migration version in the database"""
    engine = create_engine(settings.DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # Check current version
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            current_version = result.scalar()
            
            print(f"Current database version: {current_version}")
            print(f"Latest available version: 015")
            
            if current_version in ['016', '017']:
                print(f"\n⚠️  Database version {current_version} doesn't exist in codebase!")
                print(f"   Resetting to version 015 (latest available)...")
                
                # Update the version to 015
                conn.execute(text("UPDATE alembic_version SET version_num = '015'"))
                conn.commit()
                
                print(f"✅ Successfully reset database version to 015")
                print(f"\nNow you can run: make migrate-up")
                return True
            elif current_version == '015':
                print("✅ Database is already at the correct version (015)")
                return True
            else:
                print(f"⚠️  Database version {current_version} is older than expected.")
                print(f"   Run: make migrate-up to upgrade")
                return False
                
    except Exception as e:
        print(f"❌ Error fixing migration version: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        engine.dispose()

if __name__ == "__main__":
    print("Fixing database migration version...")
    print(f"Database URL: {settings.DATABASE_URL.split('@')[1] if '@' in settings.DATABASE_URL else 'hidden'}")
    print()
    
    success = fix_migration_version()
    
    if success:
        print("\n✅ Migration version fixed!")
        sys.exit(0)
    else:
        print("\n❌ Could not fix migration version")
        sys.exit(1)


