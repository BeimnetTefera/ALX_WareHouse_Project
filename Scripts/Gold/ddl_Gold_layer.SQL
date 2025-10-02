USE AlxDataWareHouse;
GO

-- Drop Gold tables if they exist
IF OBJECT_ID('gold.pw_final', 'U') IS NOT NULL
    DROP TABLE gold.pw_final;

CREATE TABLE pw_final( 
	First_Name,
    Last_Name,
    Email,
    Phone_Number,
    Assignment_Name,
    Is_Assignment_Accessed,
    Is_Assignment_Submitted,
    Is_Assignment_Passed,
    Assignment_Type,
    Is_Assignment_Resubmitted_And_Not_Corrected_Yet,
    Is_Assignment_Corrected_by_an_External_Grader,
    Age,
    Lms_Overall_Score,
    No_of_Submissions,
    Course_Name,
    Country_of_Residence,
    Cohort_Name,
    Is_Program_Withdrawn
)
