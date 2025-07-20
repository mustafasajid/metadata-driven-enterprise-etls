-- Sample data for connection_metadata
INSERT INTO connection_metadata VALUES
('conn_oracle', 'oracle', 'oracle.example.com', 1521, 'ORCL', 'HR', 'oracle_user', 'encrypted_pw1', NULL),
('conn_sqlserver', 'sqlserver', 'sqlserver.example.com', 1433, 'AdventureWorks', 'dbo', 'sa', 'encrypted_pw2', NULL),
('conn_postgres', 'postgresql', 'pg.example.com', 5432, 'salesdb', 'public', 'pguser', 'encrypted_pw3', NULL);

-- Sample data for table_metadata
INSERT INTO table_metadata VALUES
('tbl_emp', 'conn_oracle', 'EMPLOYEES', 'dw_employees', 'full', 'EMPLOYEE_ID', NULL, 'HIRE_DATE', '/mnt/datalake/dw_employees', 'daily', 'Y', 'Employee master table', 'HIRE_DATE', 'DEPARTMENT_ID', 4, 'overwrite', true),
('tbl_sales', 'conn_sqlserver', 'Sales', 'dw_sales', 'incremental', 'SaleID', 'LastModified', 'SaleDate', '/mnt/datalake/dw_sales', 'daily', 'Y', 'Sales fact table', 'SaleDate', 'RegionID', 8, 'merge', false);

-- Sample data for column_metadata
INSERT INTO column_metadata VALUES
('tbl_emp', 'EMPLOYEE_ID', 'NUMBER', 'long', false, 'Y'),
('tbl_emp', 'FIRST_NAME', 'VARCHAR2', 'string', true, 'N'),
('tbl_emp', 'HIRE_DATE', 'DATE', 'timestamp', false, 'N'),
('tbl_sales', 'SaleID', 'int', 'long', false, 'Y'),
('tbl_sales', 'Amount', 'decimal', 'double', true, 'N'),
('tbl_sales', 'SaleDate', 'datetime', 'timestamp', false, 'N');

-- Sample data for datamart_metadata
INSERT INTO datamart_metadata VALUES
('dm_sales', 'Sales', 'Sales datamart'),
('dm_hr', 'HR', 'HR datamart');

-- Sample data for table_datamart_mapping
INSERT INTO table_datamart_mapping VALUES
('tbl_emp', 'dm_hr'),
('tbl_sales', 'dm_sales');

-- Sample data for byod_table_catalog
INSERT INTO byod_table_catalog VALUES
('cat1', 'conn_postgres', 'public', 'customers', 'N'),
('cat2', 'conn_postgres', 'public', 'orders', 'Y');