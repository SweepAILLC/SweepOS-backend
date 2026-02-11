#!/usr/bin/env python3
"""
Reset the entire database: drop all tables (alembic downgrade base)
then recreate the schema (alembic upgrade head).

Usage:
    From repo root with Docker:
        make clean-db

    From backend container or with backend venv:
        python scripts/clean_db.py
        # or
        alembic downgrade base && alembic upgrade head
"""
import os
import subprocess
import sys

def main():
    backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    os.chdir(backend_dir)

    print("⚠️  This will DELETE ALL DATA and recreate an empty schema.")
    if os.isatty(0):
        reply = input("Type 'yes' to continue: ").strip().lower()
        if reply != 'yes':
            print("Aborted.")
            sys.exit(1)

    for cmd, label in [
        (["alembic", "downgrade", "base"], "Dropping all tables"),
        (["alembic", "upgrade", "head"], "Recreating schema"),
    ]:
        print(f"\n→ {label}...")
        r = subprocess.run(cmd)
        if r.returncode != 0:
            print(f"Failed: {' '.join(cmd)}")
            sys.exit(r.returncode)
    print("\n✅ Database reset complete. All tables are empty.")


if __name__ == "__main__":
    main()
