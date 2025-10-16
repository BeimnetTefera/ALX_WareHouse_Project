-- Step 1: Aggregate assignments by status for each student & course
WITH CoursePerformance AS (
    SELECT
        first_name,
        last_name,
        course_name,
        STRING_AGG(CASE WHEN is_assignment_passed = 'No' THEN assignment_name END, ', ') AS failed_assignments
    FROM bronze.pw_learner_activity
    GROUP BY
        first_name,
        last_name,
        course_name
)

-- Step 2: Pivot courses into columns and fetch raw data for LMS score, submissions, and passed assignments
SELECT
    UPPER(cp.first_name) AS First_Name,
    UPPER(cp.last_name) AS Last_Name,

    -- Contact info
    MAX(lms.contact_phone) AS Phone_Number,
    MAX(lms.email) AS Email,

    -- BUS 200
    MAX(CASE WHEN cp.course_name = 'BUS 200: Business Finance' THEN lms.no_of_submissions END) AS [BUS 200: Business Finance: # of Assignments Submitted],
    MAX(CASE WHEN cp.course_name = 'BUS 200: Business Finance' THEN lms.no_of_assignments_passed END) AS [BUS 200: Business Finance: # of Assignments Passed],
    MAX(CASE WHEN cp.course_name = 'BUS 200: Business Finance' THEN failed_assignments END) AS [BUS 200: Business Finance: Missed Assignments],
    MAX(CASE WHEN cp.course_name = 'BUS 200: Business Finance' THEN lms.lms_overall_score END) AS [BUS 200: Business Finance: Failed Assignments],

    -- ENT 110
    MAX(CASE WHEN cp.course_name = 'ENT 110: Introduction to Venture Creation' THEN lms.no_of_submissions END) AS [ENT 110: Introduction to Venture Creation: # of Assignments Submitted],
    MAX(CASE WHEN cp.course_name = 'ENT 110: Introduction to Venture Creation' THEN lms.no_of_assignments_passed END) AS [ENT 110: Introduction to Venture Creation: # of Assignments Passed],
    MAX(CASE WHEN cp.course_name = 'ENT 110: Introduction to Venture Creation' THEN failed_assignments END) AS [ENT 110: Introduction to Venture Creation: Missed Assignments],
    MAX(CASE WHEN cp.course_name = 'ENT 110: Introduction to Venture Creation' THEN lms.lms_overall_score END) AS [ENT 110: Introduction to Venture Creation: Failed Assignments],

    -- QNT 102
    MAX(CASE WHEN cp.course_name = 'QNT 102: Introduction to Statistics' THEN lms.no_of_submissions END) AS [QNT 102: Introduction to Statistics: # of Assignments Submitted],
    MAX(CASE WHEN cp.course_name = 'QNT 102: Introduction to Statistics' THEN lms.no_of_assignments_passed END) AS [QNT 102: Introduction to Statistics: # of Assignments Passed],
    MAX(CASE WHEN cp.course_name = 'QNT 102: Introduction to Statistics' THEN failed_assignments END) AS [QNT 102: Introduction to Statistics: Missed Assignments],
    MAX(CASE WHEN cp.course_name = 'QNT 102: Introduction to Statistics' THEN lms.lms_overall_score END) AS [QNT 102: Introduction to Statistics: Failed Assignments],

    -- SE 101
    MAX(CASE WHEN cp.course_name = 'SE 101: Introduction to Computing' THEN lms.no_of_submissions END) AS [SE 101: Introduction to Computing: # of Assignments Submitted],
    MAX(CASE WHEN cp.course_name = 'SE 101: Introduction to Computing' THEN lms.no_of_assignments_passed END) AS [SE 101: Introduction to Computing: # of Assignments Passed],
    MAX(CASE WHEN cp.course_name = 'SE 101: Introduction to Computing' THEN failed_assignments END) AS [SE 101: Introduction to Computing: Missed Assignments],
    MAX(CASE WHEN cp.course_name = 'SE 101: Introduction to Computing' THEN lms.lms_overall_score END) AS [SE 101: Introduction to Computing: Failed Assignments],

    -- SE 102
    MAX(CASE WHEN cp.course_name = 'SE 102: Foundations of Linux and Version Control' THEN lms.no_of_submissions END) AS [SE 102: Foundations of Linux and Version Control: # of Assignments Submitted],
    MAX(CASE WHEN cp.course_name = 'SE 102: Foundations of Linux and Version Control' THEN lms.no_of_assignments_passed END) AS [SE 102: Foundations of Linux and Version Control: # of Assignments Passed],
    MAX(CASE WHEN cp.course_name = 'SE 102: Foundations of Linux and Version Control' THEN failed_assignments END) AS [SE 102: Foundations of Linux and Version Control: Missed Assignments],
    MAX(CASE WHEN cp.course_name = 'SE 102: Foundations of Linux and Version Control' THEN lms.lms_overall_score END) AS [SE 102: Foundations of Linux and Version Control: Failed Assignments]

FROM CoursePerformance cp
LEFT JOIN bronze.pw_learner_activity lms
    ON cp.first_name = lms.first_name
   AND cp.last_name = lms.last_name
   AND cp.course_name = lms.course_name
GROUP BY
    cp.first_name,
    cp.last_name
ORDER BY
    cp.first_name ASC;
