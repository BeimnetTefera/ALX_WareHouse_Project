-- Step 1: Aggregate assignments by status for each student & course
WITH CoursePerformance AS (
    SELECT
        first_name,
        last_name,
        course_name,
        no_of_submissions,
        lms_overall_score,
        STRING_AGG(CASE WHEN is_assignment_passed = 'Yes' THEN assignment_name END, ', ') AS passed_assignments,
        STRING_AGG(CASE WHEN is_assignment_passed = 'No' THEN assignment_name END, ', ') AS failed_assignments,
        STRING_AGG(assignment_name, ', ') AS all_assignments  -- ✅ Validation column (all assignments)
    FROM bronze.pw_learner_activity
    GROUP BY
        first_name,
        last_name,
        course_name,
        lms_overall_score,
        no_of_submissions
)

-- Step 2: Pivot courses into columns with Passed/Failed prefixes and include the validation list
SELECT
    UPPER(first_name) AS First_Name,
    UPPER(last_name) AS Last_Name,
    lms_overall_score,
    -- ✅ BUS 200
    MAX(CASE WHEN course_name = 'BUS 200: Business Finance' THEN all_assignments END) AS [BUS 200: Business Finance],
    MAX(CASE WHEN course_name = 'BUS 200: Business Finance' THEN passed_assignments END) AS [Passed_BUS 200: Business Finance],
    MAX(CASE WHEN course_name = 'BUS 200: Business Finance' THEN failed_assignments END) AS [Failed_BUS 200: Business Finance],


    -- ✅ ENT 110
    MAX(CASE WHEN course_name = 'ENT 110: Introduction to Venture Creation' THEN all_assignments END) AS [ENT 110: Introduction to Venture Creation],
    MAX(CASE WHEN course_name = 'ENT 110: Introduction to Venture Creation' THEN passed_assignments END) AS [Passed_ENT 110: Introduction to Venture Creation],
    MAX(CASE WHEN course_name = 'ENT 110: Introduction to Venture Creation' THEN failed_assignments END) AS [Failed_ENT 110: Introduction to Venture Creation],
    

    -- ✅ QNT 102
    MAX(CASE WHEN course_name = 'QNT 102: Introduction to Statistics' THEN all_assignments END) AS [QNT 102: Introduction to Statistics],
    MAX(CASE WHEN course_name = 'QNT 102: Introduction to Statistics' THEN passed_assignments END) AS [Passed_QNT 102: Introduction to Statistics],
    MAX(CASE WHEN course_name = 'QNT 102: Introduction to Statistics' THEN failed_assignments END) AS [Failed_QNT 102: Introduction to Statistics],
    

    -- ✅ SE 101
    MAX(CASE WHEN course_name = 'SE 101: Introduction to Computing' THEN all_assignments END) AS [SE 101: Introduction to Computing],
    MAX(CASE WHEN course_name = 'SE 101: Introduction to Computing' THEN passed_assignments END) AS [Passed_SE 101: Introduction to Computing],
    MAX(CASE WHEN course_name = 'SE 101: Introduction to Computing' THEN failed_assignments END) AS [Failed_SE 101: Introduction to Computing],
    
    -- ✅ SE 102
    MAX(CASE WHEN course_name = 'SE 102: Foundations of Linux and Version Control' THEN all_assignments END) AS [SE 102: Foundations of Linux and Version Control],
    MAX(CASE WHEN course_name = 'SE 102: Foundations of Linux and Version Control' THEN passed_assignments END) AS [Passed_SE 102: Foundations of Linux and Version Control],
    MAX(CASE WHEN course_name = 'SE 102: Foundations of Linux and Version Control' THEN failed_assignments END) AS [Failed_SE 102: Foundations of Linux and Version Control]
    

FROM CoursePerformance
GROUP BY
    first_name,
    last_name,
    lms_overall_score;
