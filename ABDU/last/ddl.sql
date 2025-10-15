USE AlxDataWareHouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
    @filePath NVARCHAR(500)  -- Dynamic CSV file path
AS
BEGIN
    DECLARE @batch_start_time DATETIME = GETDATE();
    DECLARE @batch_end_time DATETIME;
    DECLARE @start_time DATETIME;
    DECLARE @end_time DATETIME;
    DECLARE @sql NVARCHAR(MAX);

    BEGIN TRY
        PRINT '============================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '============================================================';

        -- --------------------------
        -- Load bronze.pw_learner_activity
        -- --------------------------
        PRINT '-------------------------------------------------------------';
        PRINT 'Loading bronze.pw_learner_activity';
        PRINT '-------------------------------------------------------------';

        SET @start_time = GETDATE();

        -- Check if table exists before truncating
        IF OBJECT_ID('bronze.pw_learner_activity', 'U') IS NOT NULL
        BEGIN
            PRINT '>> Truncating Table: bronze.pw_learner_activity';
            TRUNCATE TABLE bronze.pw_learner_activity;
        END

        -- Build dynamic SQL for BULK INSERT
        SET @sql = N'
            BULK INSERT bronze.pw_learner_activity
            FROM ''' + @filePath + '''
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                FORMAT = ''CSV'',
                CODEPAGE = ''65001'',
                TABLOCK
            );';

        -- Execute dynamic SQL
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------';

        -- --------------------------
        -- Complete
        -- --------------------------
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
GO

-- Example execution
EXEC bronze.load_bronze 
    @filePath = 'C:\project\alx\aa.csv';
