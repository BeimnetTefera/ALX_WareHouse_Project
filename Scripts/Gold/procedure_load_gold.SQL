USE AlxDataWareHouse;
GO

WITH learner_base AS (
    SELECT
        la.first_name,
        la.last_name,
        la.email,
        la.contact_phone,
        lto.age,
        lto.lms_overall_score,
        lto.course_name,

        -- Group failed and missing assignments by course
        STRING_AGG(
            CASE 
                WHEN la.is_assignment_passed = 'No' AND la.is_assignment_submitted = 'Yes' 
                    THEN CAST(la.assignment_name AS NVARCHAR(MAX)) 
            END, ', '
        ) AS failed_assignments,
        STRING_AGG(
            CASE 
                WHEN la.is_assignment_submitted = 'No' 
                    THEN CAST(la.assignment_name AS NVARCHAR(MAX)) 
            END, ', '
        ) AS missing_assignments
    FROM silver.pw_learner_activity la
    JOIN silver.pw_learner_tracking_overview lto
        ON la.email = lto.email
    GROUP BY la.first_name, la.last_name, la.email, la.contact_phone,
             lto.age, lto.lms_overall_score, lto.course_name
)

SELECT
    lb.first_name,
    lb.last_name,
    lb.email,
    lb.contact_phone,

    -- Age Range
    CASE 
        WHEN lb.age BETWEEN 18 AND 20 THEN '18-20'
        WHEN lb.age BETWEEN 21 AND 23 THEN '21-23'
        WHEN lb.age BETWEEN 24 AND 26 THEN '24-26'
        ELSE '27+'
    END AS age_range,

    -- === BUS 200: Business Finance ===
    COALESCE(MAX(CASE WHEN lb.course_name = 'BUS 200: Business Finance' THEN lb.missing_assignments END), 'All Good ✅') AS BUS200_Missed,
    COALESCE(MAX(CASE WHEN lb.course_name = 'BUS 200: Business Finance' THEN lb.failed_assignments END), 'All Good ✅') AS BUS200_Failed,
    MAX(CASE WHEN lb.course_name = 'BUS 200: Business Finance' THEN lb.lms_overall_score END) AS BUS200_Score,

    -- === ENT 110: Introduction to Venture Creation ===
    COALESCE(MAX(CASE WHEN lb.course_name = 'ENT 110: Introduction to Venture Creation' THEN lb.missing_assignments END), 'All Good ✅') AS ENT110_Missed,
    COALESCE(MAX(CASE WHEN lb.course_name = 'ENT 110: Introduction to Venture Creation' THEN lb.failed_assignments END), 'All Good ✅') AS ENT110_Failed,
    MAX(CASE WHEN lb.course_name = 'ENT 110: Introduction to Venture Creation' THEN lb.lms_overall_score END) AS ENT110_Score,

    -- === QNT 102: Introduction to Statistics ===
    COALESCE(MAX(CASE WHEN lb.course_name = 'QNT 102: Introduction to Statistics' THEN lb.missing_assignments END), 'All Good ✅') AS QNT102_Missed,
    COALESCE(MAX(CASE WHEN lb.course_name = 'QNT 102: Introduction to Statistics' THEN lb.failed_assignments END), 'All Good ✅') AS QNT102_Failed,
    MAX(CASE WHEN lb.course_name = 'QNT 102: Introduction to Statistics' THEN lb.lms_overall_score END) AS QNT102_Score,

    -- === SE 101: Introduction to Computing ===
    COALESCE(MAX(CASE WHEN lb.course_name = 'SE 101: Introduction to Computing' THEN lb.missing_assignments END), 'All Good ✅') AS SE101_Missed,
    COALESCE(MAX(CASE WHEN lb.course_name = 'SE 101: Introduction to Computing' THEN lb.failed_assignments END), 'All Good ✅') AS SE101_Failed,
    MAX(CASE WHEN lb.course_name = 'SE 101: Introduction to Computing' THEN lb.lms_overall_score END) AS SE101_Score,

    -- === SE 102: Foundations of Linux and Version Control ===
    COALESCE(MAX(CASE WHEN lb.course_name = 'SE 102: Foundations of Linux and Version Control' THEN lb.missing_assignments END), 'All Good ✅') AS SE102_Missed,
    COALESCE(MAX(CASE WHEN lb.course_name = 'SE 102: Foundations of Linux and Version Control' THEN lb.failed_assignments END), 'All Good ✅') AS SE102_Failed,
    MAX(CASE WHEN lb.course_name = 'SE 102: Foundations of Linux and Version Control' THEN lb.lms_overall_score END) AS SE102_Score,

    -- learner status
    CASE
        WHEN MAX(lb.lms_overall_score) >= 70 THEN 'On Track 🚀'
        WHEN MAX(lb.lms_overall_score) BETWEEN 50 AND 69 THEN 'Needs Attention 👀'
        ELSE 'Off Track 🤦'
    END AS learner_status

FROM learner_base lb
GROUP BY lb.first_name, lb.last_name, lb.email, lb.contact_phone,
         CASE 
            WHEN lb.age BETWEEN 18 AND 20 THEN '18-20'
            WHEN lb.age BETWEEN 21 AND 23 THEN '21-23'
            WHEN lb.age BETWEEN 24 AND 26 THEN '24-26'
            ELSE '27+'
         END;
