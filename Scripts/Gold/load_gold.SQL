USE AlxDataWareHouse;
GO

INSERT INTO pw_final(
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
    la.first_name,
    la.last_name,
    la.email,
    la.contact_phone,
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
    ON la.email = lto.email;
