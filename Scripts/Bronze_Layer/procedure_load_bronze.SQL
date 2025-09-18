/*
=============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================================
Script Puropse:
    This stored procedure loads data in to the 'bronze' schema from external CSV files.
    It performs the following actions:
        - Truncates the bronze tables before loading data.
        - Uses the 'Bulk Insert' command to load data from CSV files to bronze tables.

Parameters:
    None
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC  bronze.load_bronze;

============================================================================
*/

USE AlxDataWareHouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @batch_start_time DATETIME;
    DECLARE @batch_end_time DATETIME;
    DECLARE @start_time DATETIME;
    DECLARE @end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '============================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '============================================================';

        -- Load pw_learner_activity
        PRINT '-------------------------------------------------------------';
        PRINT 'Loading bronze.pw_learner_activity';
        PRINT '-------------------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.pw_learner_activity;

        BULK INSERT bronze.pw_learner_activity
        FROM 'C:\project\alx\Term 2_ Pathway C1 Learners Academic Activity_August 19th - Learners Activity.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FORMAT = 'CSV',
            CODEPAGE = '65001',
            TABLOCK,
            KEEPNULLS,
            FIELDQUOTE = '"'
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (Learner Activity): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load pw_learner_tracking_overview
        PRINT '-------------------------------------------------------------';
        PRINT 'Loading bronze.pw_learner_tracking_overview';
        PRINT '-------------------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.pw_learner_tracking_overview;

        BULK INSERT bronze.pw_learner_tracking_overview
        FROM 'C:\project\alx\Term 2_ Pathway C1 Learners Academic Activity_August 19th - Learners Tracking Overview.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FORMAT = 'CSV',
            CODEPAGE = '65001',
            TABLOCK,
            KEEPNULLS,
            FIELDQUOTE = '"'
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (Learner Tracking Overview): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer Completed';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '============================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================================';
    END CATCH
END;
