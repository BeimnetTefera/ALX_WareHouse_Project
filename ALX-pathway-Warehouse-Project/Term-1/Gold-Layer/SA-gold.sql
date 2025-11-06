WITH CoursePerformance AS (
    SELECT
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        TRIM(course_name) AS course_name,

        -- ✅ Failed assignments (submitted but not passed)
        STRING_AGG(
            CASE 
                WHEN is_assignment_passed = 'No' THEN TRIM(assignment_name)
            END, ', '
        ) AS failed_assignments,

        -- ✅ Missed assignments (not submitted at all)
        STRING_AGG(
            CASE 
                WHEN is_assignment_submitted = 'No' THEN TRIM(assignment_name)
            END, ', '
        ) AS missed_assignments,

        MAX(no_of_submissions) AS no_of_submissions,
        MAX(no_of_assignments_passed) AS no_of_assignments_passed,
        MAX(lms_overall_score) AS lms_overall_score,
        MAX(TRIM(contact_phone)) AS contact_phone,
        MAX(TRIM(email)) AS email,
        MAX(TRIM(age_range)) AS age_range
    FROM bronze.pw_learner_activity
    WHERE 
        -- ✅ Only include learners who have submitted 1 or more assignments
        TRY_CAST(no_of_submissions AS FLOAT) >= 1
    GROUP BY
        TRIM(first_name),
        TRIM(last_name),
        TRIM(course_name)
)
SELECT
    UPPER(TRIM(cp.first_name)) AS [First_Name],
    UPPER(TRIM(cp.last_name)) AS [Last_Name],

    TRIM(
    CASE 
        WHEN cp.contact_phone LIKE '27%' THEN 
            '+27' + RIGHT(cp.contact_phone, LEN(cp.contact_phone) - 2)
        WHEN cp.contact_phone LIKE '0%' THEN 
            '+27' + RIGHT(cp.contact_phone, LEN(cp.contact_phone) - 1)
        ELSE 
            cp.contact_phone
    END
) AS [Phone_Number],
    TRIM(cp.email) AS [Email],
    TRIM(cp.age_range) AS [Age_Range],

    -- ENT 110
    COALESCE(MAX(CASE WHEN cp.course_name = 'ENT 100: FOUNDATIONS OF ENTREPRENEURSHIP' THEN cp.no_of_submissions END), '0') AS [ENT 110: # Submitted],
    COALESCE(MAX(CASE WHEN cp.course_name = 'ENT 100: FOUNDATIONS OF ENTREPRENEURSHIP' THEN cp.no_of_assignments_passed END), '0') AS [ENT 110: # Passed],
    COALESCE(MAX(CASE WHEN cp.course_name = 'ENT 100: FOUNDATIONS OF ENTREPRENEURSHIP' THEN cp.failed_assignments END), 'All Good ') AS [ENT 110: Failed Assignments],
    COALESCE(MAX(CASE WHEN cp.course_name = 'ENT 100: FOUNDATIONS OF ENTREPRENEURSHIP' THEN cp.missed_assignments END), 'All Good ') AS [ENT 110: Missed Assignments],
    COALESCE(MAX(CASE WHEN cp.course_name = 'ENT 100: FOUNDATIONS OF ENTREPRENEURSHIP' THEN cp.lms_overall_score END), '0') AS [ENT 110: LMS Score],

    -- QNT 101
    COALESCE(MAX(CASE WHEN cp.course_name = 'QNT 101: INTRODUCTION TO COLLEGE ALGEBRA' THEN cp.no_of_submissions END), '0') AS [QNT 101: # Submitted],
    COALESCE(MAX(CASE WHEN cp.course_name = 'QNT 101: INTRODUCTION TO COLLEGE ALGEBRA' THEN cp.no_of_assignments_passed END), '0') AS [QNT 101: # Passed],
    COALESCE(MAX(CASE WHEN cp.course_name = 'QNT 101: INTRODUCTION TO COLLEGE ALGEBRA' THEN cp.failed_assignments END), 'All Good ') AS [QNT 101: Failed Assignments],
    COALESCE(MAX(CASE WHEN cp.course_name = 'QNT 101: INTRODUCTION TO COLLEGE ALGEBRA' THEN cp.missed_assignments END), 'All Good ') AS [QNT 101:: Missed Assignments],
    COALESCE(MAX(CASE WHEN cp.course_name = 'QNT 101: INTRODUCTION TO COLLEGE ALGEBRA' THEN cp.lms_overall_score END), '0') AS [QNT 101: LMS Score],

    CASE
        WHEN MIN(CAST(cp.lms_overall_score AS FLOAT)) >= 70 THEN 'On Track'
        WHEN MIN(CAST(cp.lms_overall_score AS FLOAT)) >= 50 
             AND MAX(CAST(cp.lms_overall_score AS FLOAT)) < 100 THEN 'Needs Improvement'
        ELSE 'Off Track'
    END AS Learner_Status

FROM CoursePerformance cp
GROUP BY cp.first_name, cp.last_name, cp.contact_phone, cp.email, cp.age_range
ORDER BY [First_Name];
