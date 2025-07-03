-- Enhanced Theory ID Column Update for SC Format
-- Supports format: SC00000001.1, SC00000001.2, SC00000002.1, etc.

-- Step 1: Increase column size to support longer SC format IDs
ALTER TABLE SoftCollection_theories 
MODIFY (theory_id VARCHAR2(50));

-- Step 2: Verify the column change
DESCRIBE SoftCollection_theories;

-- Step 3: Check current data (optional)
SELECT DISTINCT theory_id FROM SoftCollection_theories ORDER BY theory_id;

-- Step 4: Create an index for better performance (optional)
CREATE INDEX idx_theories_theory_id ON SoftCollection_theories(theory_id);

COMMIT;

-- Testing the new format
-- Sample insert to verify the column can handle the new format:
-- INSERT INTO SoftCollection_theories (IIN, theory_name, theory_description, load_date, theory_start_date, theory_end_date, theory_id, created_by)
-- VALUES ('TEST123456789', 'Test Theory', 'Test Description', SYSDATE, SYSDATE, SYSDATE+30, 'SC00000001.1', 'SYSTEM');
-- ROLLBACK; -- Don't actually insert, just test 