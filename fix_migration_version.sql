-- SQL script to fix migration version mismatch
-- If database thinks it's at version 017 but that doesn't exist,
-- reset it to 015 (the latest actual version)

-- Check current version
SELECT version_num FROM alembic_version;

-- Reset to version 015 (latest available)
UPDATE alembic_version SET version_num = '015';

-- Verify
SELECT version_num FROM alembic_version;


