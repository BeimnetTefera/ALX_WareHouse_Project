/*
=============================================
CREATE Database and Schemas
=============================================
Script purpose:
	This Script creates a new database named 'AlxDataWareHouse'. 
	If it already exists, it is dropped and recreated. 
	It also creates three schemas: bronze, silver, gold.
WARNING:
	This will DROP the 'AlxDataWareHouse' database and delete all its contents.
	Make sure to back up your data!
*/

USE Master;
GO

-- DROP AND RECRREATE THE 'DataWarehouse' DATABASE --
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'AlxDataWareHouse')
BEGIN 
	ALTER DATABASE AlxDataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE AlxDataWareHouse;
END
GO

-- CREATE DATABASE WAREHOUSE --
CREATE DATABASE AlxDataWareHouse;
GO

USE AlxDataWareHouse;
GO

-- DROP SCHEMAS IF THEY ALREADY EXISTS --
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze') EXEC('DROP SCHEMA bronze');
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver') EXEC('DROP SCHEMA silver');
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')   EXEC('DROP SCHEMA gold');
GO

-- CREATE SCHEMAS --
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
