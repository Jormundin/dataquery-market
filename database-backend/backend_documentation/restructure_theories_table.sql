-- Restructure SoftCollection_theories table to Campaign Registry Format
-- This changes from storing individual user records to one record per campaign

-- Step 1: Create backup of current data (optional but recommended)
CREATE TABLE SoftCollection_theories_backup AS 
SELECT * FROM SoftCollection_theories;

-- Step 2: Add user_count column to track number of users per campaign
ALTER TABLE SoftCollection_theories 
ADD (user_count NUMBER DEFAULT 0);

-- Step 3: Update existing data to consolidate records per theory_id
-- Calculate user count for existing theories
UPDATE SoftCollection_theories t1
SET user_count = (
    SELECT COUNT(DISTINCT IIN) 
    FROM SoftCollection_theories t2 
    WHERE t2.theory_id = t1.theory_id
)
WHERE user_count = 0;

-- Step 4: Remove duplicate records - keep only one record per theory_id
-- Keep the record with the earliest load_date (first created)
DELETE FROM SoftCollection_theories t1
WHERE EXISTS (
    SELECT 1 FROM SoftCollection_theories t2
    WHERE t2.theory_id = t1.theory_id
    AND t2.load_date < t1.load_date
);

-- Step 5: Remove duplicate records with same load_date - keep one with lowest ROWID
DELETE FROM SoftCollection_theories t1
WHERE EXISTS (
    SELECT 1 FROM SoftCollection_theories t2
    WHERE t2.theory_id = t1.theory_id
    AND t2.load_date = t1.load_date
    AND t2.ROWID < t1.ROWID
);

-- Step 6: Drop the IIN column as we no longer store individual users
ALTER TABLE SoftCollection_theories 
DROP COLUMN IIN;

-- Step 7: Add constraints
ALTER TABLE SoftCollection_theories 
ADD CONSTRAINT uk_theories_theory_id UNIQUE (theory_id);

ALTER TABLE SoftCollection_theories 
MODIFY (user_count NUMBER NOT NULL);

-- Step 8: Verify the restructuring
SELECT 'Records after restructuring: ' || COUNT(*) as status FROM SoftCollection_theories;
SELECT 'Unique theory IDs: ' || COUNT(DISTINCT theory_id) as status FROM SoftCollection_theories;

-- Step 9: Show sample of restructured data
SELECT theory_id, theory_name, user_count, 
       TO_CHAR(theory_start_date, 'YYYY-MM-DD') as start_date,
       TO_CHAR(theory_end_date, 'YYYY-MM-DD') as end_date,
       created_by
FROM SoftCollection_theories 
ORDER BY theory_start_date DESC;

COMMIT;

-- Note: Keep the backup table until you verify everything works correctly
-- To drop backup later: DROP TABLE SoftCollection_theories_backup; 