-- =====================================================
-- SC_theory_users Table Creation Script for SPSS Database
-- =====================================================
-- This script creates the SC_theory_users table in the SPSS database
-- The table structure is identical to SC_local_target in DSSB_APP
--
-- Execute this script in the SPSS Oracle database as the target user
-- =====================================================

-- Drop table if it exists (optional - remove comment if needed)
-- DROP TABLE SC_theory_users;

-- Create the main SC_theory_users table
CREATE TABLE SC_theory_users (
    IIN VARCHAR2(12) NOT NULL,
    THEORY_ID VARCHAR2(50) NOT NULL,
    date_start DATE,
    date_end DATE,
    insert_datetime DATE DEFAULT SYSDATE,
    tab1 VARCHAR2(4000),
    tab2 VARCHAR2(4000),
    tab3 VARCHAR2(4000),
    tab4 VARCHAR2(4000),
    tab5 VARCHAR2(4000)
);

-- Add table comment
COMMENT ON TABLE SC_theory_users IS 'Theory users table for SPSS - contains target group users from stratification campaigns';

-- Add column comments
COMMENT ON COLUMN SC_theory_users.IIN IS 'Individual Identification Number (12 digits)';
COMMENT ON COLUMN SC_theory_users.THEORY_ID IS 'Theory/Campaign ID in format SC00000001.1, SC00000001.2, etc.';
COMMENT ON COLUMN SC_theory_users.date_start IS 'Campaign start date';
COMMENT ON COLUMN SC_theory_users.date_end IS 'Campaign end date';
COMMENT ON COLUMN SC_theory_users.insert_datetime IS 'Timestamp when record was inserted';
COMMENT ON COLUMN SC_theory_users.tab1 IS 'Additional field 1 - typically contains group description';
COMMENT ON COLUMN SC_theory_users.tab2 IS 'Additional field 2 - custom group-specific data';
COMMENT ON COLUMN SC_theory_users.tab3 IS 'Additional field 3 - custom group-specific data';
COMMENT ON COLUMN SC_theory_users.tab4 IS 'Additional field 4 - custom group-specific data';
COMMENT ON COLUMN SC_theory_users.tab5 IS 'Additional field 5 - reserved for future use';

-- Create indexes for optimal performance
CREATE INDEX idx_sc_theory_users_theory_id ON SC_theory_users(THEORY_ID);
COMMENT ON INDEX idx_sc_theory_users_theory_id IS 'Index on THEORY_ID for fast campaign-based queries';

CREATE INDEX idx_sc_theory_users_iin ON SC_theory_users(IIN);
COMMENT ON INDEX idx_sc_theory_users_iin IS 'Index on IIN for fast user-based lookups';

CREATE INDEX idx_sc_theory_users_dates ON SC_theory_users(date_start, date_end);
COMMENT ON INDEX idx_sc_theory_users_dates IS 'Composite index on campaign dates for date range queries';

CREATE INDEX idx_sc_theory_users_insert_dt ON SC_theory_users(insert_datetime);
COMMENT ON INDEX idx_sc_theory_users_insert_dt IS 'Index on insert timestamp for chronological queries';

-- Create composite index for common query patterns
CREATE INDEX idx_sc_theory_users_composite ON SC_theory_users(THEORY_ID, IIN, date_start);
COMMENT ON INDEX idx_sc_theory_users_composite IS 'Composite index for complex filtering operations';

-- Optional: Create constraints (uncomment if needed)
-- ALTER TABLE SC_theory_users ADD CONSTRAINT chk_iin_length CHECK (LENGTH(IIN) = 12);
-- ALTER TABLE SC_theory_users ADD CONSTRAINT chk_theory_id_format CHECK (REGEXP_LIKE(THEORY_ID, '^SC[0-9]{8}\.[0-9]+$'));
-- ALTER TABLE SC_theory_users ADD CONSTRAINT chk_date_range CHECK (date_end >= date_start);

-- Grant permissions (adjust username as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SC_theory_users TO your_app_user;

-- Verify table creation
SELECT 
    table_name,
    num_rows,
    last_analyzed
FROM user_tables 
WHERE table_name = 'SC_THEORY_USERS';

-- Verify indexes
SELECT 
    index_name,
    column_name,
    column_position
FROM user_ind_columns 
WHERE table_name = 'SC_THEORY_USERS'
ORDER BY index_name, column_position;

-- Display table structure
DESCRIBE SC_theory_users;

PROMPT 'SC_theory_users table created successfully!';
PROMPT 'Table is ready to receive data from SoftCollection stratification process.';
PROMPT '';
PROMPT 'Next steps:';
PROMPT '1. Configure SPSS_ORACLE_* environment variables in your application';
PROMPT '2. Test connection using /databases/test-all-connections endpoint';
PROMPT '3. Run stratification to verify dual database insertion'; 