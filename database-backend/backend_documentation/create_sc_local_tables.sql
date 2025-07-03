-- Create SC Local Control and Target Tables for Stratification
-- These tables store the actual user assignments from stratification

-- SC_local_control: Control group (first stratification group)
CREATE TABLE SC_local_control (
    IIN VARCHAR2(20) NOT NULL,
    THEORY_ID VARCHAR2(50) NOT NULL,
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    insert_datetime DATE DEFAULT SYSDATE,
    tab1 VARCHAR2(500),
    tab2 VARCHAR2(500), 
    tab3 VARCHAR2(500),
    tab4 VARCHAR2(500),
    tab5 VARCHAR2(500),
    CONSTRAINT pk_sc_local_control PRIMARY KEY (IIN, THEORY_ID)
);

-- SC_local_target: Target groups (all other stratification groups combined)
CREATE TABLE SC_local_target (
    IIN VARCHAR2(20) NOT NULL,
    THEORY_ID VARCHAR2(50) NOT NULL,
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    insert_datetime DATE DEFAULT SYSDATE,
    tab1 VARCHAR2(500),
    tab2 VARCHAR2(500),
    tab3 VARCHAR2(500), 
    tab4 VARCHAR2(500),
    tab5 VARCHAR2(500),
    CONSTRAINT pk_sc_local_target PRIMARY KEY (IIN, THEORY_ID)
);

-- Create indexes for better performance
CREATE INDEX idx_sc_control_theory_id ON SC_local_control(THEORY_ID);
CREATE INDEX idx_sc_control_dates ON SC_local_control(date_start, date_end);
CREATE INDEX idx_sc_target_theory_id ON SC_local_target(THEORY_ID);
CREATE INDEX idx_sc_target_dates ON SC_local_target(date_start, date_end);

-- Add comments for documentation
COMMENT ON TABLE SC_local_control IS 'Control group users from stratification campaigns';
COMMENT ON TABLE SC_local_target IS 'Target group users from stratification campaigns';

COMMENT ON COLUMN SC_local_control.IIN IS 'Individual Identification Number';
COMMENT ON COLUMN SC_local_control.THEORY_ID IS 'Campaign ID (SC format)';
COMMENT ON COLUMN SC_local_control.date_start IS 'Campaign start date';
COMMENT ON COLUMN SC_local_control.date_end IS 'Campaign end date';
COMMENT ON COLUMN SC_local_control.insert_datetime IS 'Record insertion timestamp';
COMMENT ON COLUMN SC_local_control.tab1 IS 'Additional field 1 (description, etc.)';
COMMENT ON COLUMN SC_local_control.tab2 IS 'Additional field 2';
COMMENT ON COLUMN SC_local_control.tab3 IS 'Additional field 3';
COMMENT ON COLUMN SC_local_control.tab4 IS 'Additional field 4';
COMMENT ON COLUMN SC_local_control.tab5 IS 'Additional field 5';

-- Same comments for target table
COMMENT ON COLUMN SC_local_target.IIN IS 'Individual Identification Number';
COMMENT ON COLUMN SC_local_target.THEORY_ID IS 'Campaign ID (SC format)';
COMMENT ON COLUMN SC_local_target.date_start IS 'Campaign start date';
COMMENT ON COLUMN SC_local_target.date_end IS 'Campaign end date';
COMMENT ON COLUMN SC_local_target.insert_datetime IS 'Record insertion timestamp';
COMMENT ON COLUMN SC_local_target.tab1 IS 'Additional field 1 (description, etc.)';
COMMENT ON COLUMN SC_local_target.tab2 IS 'Additional field 2';
COMMENT ON COLUMN SC_local_target.tab3 IS 'Additional field 3';
COMMENT ON COLUMN SC_local_target.tab4 IS 'Additional field 4';
COMMENT ON COLUMN SC_local_target.tab5 IS 'Additional field 5';

-- Verify table creation
SELECT 'SC_local_control created: ' || COUNT(*) as status 
FROM user_tables WHERE table_name = 'SC_LOCAL_CONTROL';

SELECT 'SC_local_target created: ' || COUNT(*) as status 
FROM user_tables WHERE table_name = 'SC_LOCAL_TARGET';

COMMIT; 