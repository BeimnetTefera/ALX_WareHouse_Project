/*
*******************************************************************************
DDL Script: Create Gold Tables
*******************************************************************************
Script Purpose:
    This script creates table for the Gold layer in the data warehouse.
    The Gold layer Represents the final table

    Each view performs transformations and combines data from the silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - The table can be queried directly for analytics and reporting.
*******************************************************************************
*/

USE AlxDataWareHouse;
GO

-- Drop Gold tables if they exist
IF OBJECT_ID('gold.pw_final', 'U') IS NOT NULL
    DROP TABLE gold.pw_final;

CREATE TABLE pw_final( 
	First_Name NVARCHAR(50),
    Last_Name NVARCHAR(50),
    Email NVARCHAR(150),
    Phone_Number NVARCHAR(13),
    Assignment_Name NVARCHAR(250),
    Is_Assignment_Accessed NVARCHAR(5),
    Is_Assignment_Submitted NVARCHAR(5),
    Is_Assignment_Passed NVARCHAR(5),
    Assignment_Type NVARCHAR(250),
    Is_Assignment_Resubmitted_And_Not_Corrected_Yet NVARCHAR(5),
    Is_Assignment_Corrected_by_an_External_Grader NVARCHAR(5),
    Age INT,
    Lms_Overall_Score FLOAT,
    No_of_Submissions INT,
    Course_Name NVARCHAR(150),
    Country_of_Residence NVARCHAR(50),
    Cohort_Name NVARCHAR(50),
    Is_Program_Withdrawn NVARCHAR(5)
)
