-- Script to update theory_id column to support decimal sub-IDs (e.g., 1.1, 1.2, 1.3)
-- This allows stratification to create related theories with sub-IDs

-- Step 1: Add a temporary column
ALTER TABLE SoftCollection_theories 
ADD (theory_id_temp VARCHAR2(20));

-- Step 2: Copy existing data to the temporary column  
UPDATE SoftCollection_theories 
SET theory_id_temp = TO_CHAR(theory_id);

-- Step 3: Drop the original NUMBER column
ALTER TABLE SoftCollection_theories 
DROP COLUMN theory_id;

-- Step 4: Rename the temporary column to theory_id
ALTER TABLE SoftCollection_theories 
RENAME COLUMN theory_id_temp TO theory_id;

-- Step 5: Add NOT NULL constraint if needed
-- ALTER TABLE SoftCollection_theories 
-- MODIFY (theory_id VARCHAR2(20) NOT NULL);

-- Verify the change
DESCRIBE SoftCollection_theories;

-- Test query to see current data
SELECT DISTINCT theory_id FROM SoftCollection_theories ORDER BY theory_id;

COMMIT; 