USE AlxDataWareHouse;
GO

TRUNCATE TABLE pw_final;

INSERT INTO pw_final (
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
SELECT 
    COALESCE(la.first_name, lto.first_name) AS First_Name,
    COALESCE(la.last_name, lto.last_name) AS Last_Name,
    COALESCE(la.email, lto.email) AS Email,
    COALESCE(la.contact_phone, lto.contact_phone) AS Phone_Number,
    la.assignment_name,
    la.is_assignment_accessed,
    la.is_assignment_submitted,
    la.is_assignment_passed,
    la.assignment_type,
    la.is_assignment_resubmitted_and_not_corrected_yet,
    la.is_assignment_corrected_by_an_external_grader,
    lto.age,
    lto.lms_overall_score,
    lto.no_of_submissions,
    lto.course_name,
    lto.country_of_residence,
    lto.cohort_name,
    lto.is_program_withdrawn
FROM silver.pw_learner_activity AS la
RIGHT JOIN silver.pw_learner_tracking_overview AS lto
    ON LOWER(LTRIM(RTRIM(la.email))) = LOWER(LTRIM(RTRIM(lto.email)));
