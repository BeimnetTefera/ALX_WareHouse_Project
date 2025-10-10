USE AlxDataWareHouse;
GO

WITH learner_base AS (
    SELECT
        f.First_Name,
        f.Last_Name,
        f.Email,
        f.Phone_Number,
        f.Age,
        f.Lms_Overall_Score,
        f.Course_Name,

        -- To list the assignmnets where it is submibmitted but failed
        STRING_AGG(
            CASE 
                 WHEN f.Is_Assignment_Passed = 'No' AND f.Is_Assignment_Submitted = 'Yes' 
                 THEN CAST(f.Assignment_Name AS NVARCHAR(MAX)) 
            END,  ', '
        ) AS failed_assignments,

        -- To list not submitted assignmnets
        STRING_AGG(
            CASE 
                WHEN f.Is_Assignment_Submitted = 'No' 
                THEN CAST(f.Assignment_Name AS NVARCHAR(MAX)) 
            END, ', '
        ) AS missing_assignments,

        --  Count submittted assignmnets wheteher it is passed or not
        COUNT(CASE WHEN f.Is_Assignment_Submitted = 'Yes' THEN 1 END) AS submitted_count,

        -- count submitted and passed assignmnets
        COUNT(CASE WHEN f.Is_Assignment_Passed = 'Yes' AND f.Is_Assignment_Submitted = 'Yes' THEN 1 END) AS passed_count

    FROM .pw_final f

    GROUP BY 
        f.First_Name, f.Last_Name, f.Email, f.Phone_Number,
        f.Age, f.Lms_Overall_Score, f.Course_Name
)

SELECT
    lb.First_Name,
    lb.Last_Name,
    lb.Email,
    lb.Phone_Number,

    -- Age range calculated column
    CASE 
        WHEN lb.Age BETWEEN 18 AND 20 THEN '18-20'
        WHEN lb.Age BETWEEN 21 AND 23 THEN '21-23'
        WHEN lb.Age BETWEEN 24 AND 26 THEN '24-26'
        ELSE '26+'
    END AS Age_Range,

    -- === BUS 200 ===
    MAX(CASE WHEN lb.Course_Name = 'BUS 200: Business Finance' THEN lb.submitted_count END) AS BUS200_#_Assignments_Submitted,
    MAX(CASE WHEN lb.Course_Name = 'BUS 200: Business Finance' THEN lb.passed_count END) AS BUS200_#_Assignments_Passed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'BUS 200: Business Finance' THEN lb.missing_assignments  END), 'All Good ') AS BUS200_Missed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'BUS 200: Business Finance' THEN lb.failed_assignments END), 'All Good ') BUS200_Failed,
    MAX(CASE WHEN lb.Course_Name = 'BUS 200: Business Finance' THEN lb.Lms_Overall_Score END) AS BUS200_Score,

    -- === ENT 110 ===
    MAX(CASE WHEN lb.Course_Name = 'ENT 110: Introduction to Venture Creation' THEN lb.submitted_count END) AS ENT110_#_Assignments_Submitted,
    MAX(CASE WHEN lb.Course_Name = 'ENT 110: Introduction to Venture Creation' THEN lb.passed_count END) AS ENT110_#_Assignments_Passed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'ENT 110: Introduction to Venture Creation' THEN lb.Missing_Assignments END), 'All Good ') AS ENT110_Missed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'ENT 110: Introduction to Venture Creation' THEN lb.Failed_Assignments END), 'All Good ') AS ENT110_Failed,
    MAX(CASE WHEN lb.Course_Name = 'ENT 110: Introduction to Venture Creation' THEN lb.Lms_Overall_Score END) AS ENT110_Score,

    -- === QNT 102 ===
    MAX(CASE WHEN lb.Course_Name = 'QNT 102: Introduction to Statistics' THEN lb.submitted_count END) AS QNT102_#_Assignments_Submitted,
    MAX(CASE WHEN lb.Course_Name = 'QNT 102: Introduction to Statistics' THEN lb.passed_count END) AS QNT102_#_Assignments_Passed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'QNT 102: Introduction to Statistics' THEN lb.Missing_Assignments END), 'All Good ') AS QNT102_Missed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'QNT 102: Introduction to Statistics' THEN lb.Failed_Assignments END), 'All Good ') AS QNT102_Failed,
    MAX(CASE WHEN lb.Course_Name = 'QNT 102: Introduction to Statistics' THEN lb.Lms_Overall_Score END) AS QNT102_Score,

    -- === SE 101 ===
    MAX(CASE WHEN lb.Course_Name = 'SE 101: Introduction to Computing' THEN lb.submitted_count END) AS SE101_#_Assignments_Submitted,
    MAX(CASE WHEN lb.Course_Name = 'SE 101: Introduction to Computing' THEN lb.passed_count END) AS SE101_#_Assignments_Passed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'SE 101: Introduction to Computing' THEN lb.Missing_Assignments END), 'All Good ') AS SE101_Missed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'SE 101: Introduction to Computing' THEN lb.Failed_Assignments END), 'All Good ') AS SE101_Failed,
    MAX(CASE WHEN lb.Course_Name = 'SE 101: Introduction to Computing' THEN lb.Lms_Overall_Score END) AS SE101_Score,

    -- === SE 102 ===
    MAX(CASE WHEN lb.Course_Name = 'SE 102: Foundations of Linux and Version Control' THEN lb.submitted_count END) AS SE102_#_Assignments_Submitted,
    MAX(CASE WHEN lb.Course_Name = 'SE 102: Foundations of Linux and Version Control' THEN lb.passed_count END) AS SE102_#_Assignments_Passed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'SE 102: Foundations of Linux and Version Control' THEN lb.Missing_Assignments END), 'All Good ') AS SE102_Missed,
    COALESCE(MAX(CASE WHEN lb.Course_Name = 'SE 102: Foundations of Linux and Version Control' THEN lb.Failed_Assignments END), 'All Good ') AS SE102_Failed,
    MAX(CASE WHEN lb.Course_Name = 'SE 102: Foundations of Linux and Version Control' THEN lb.Lms_Overall_Score END) AS SE102_Score,

    -- === Learner Status ===
    CASE
        WHEN MAX(lb.Lms_Overall_Score) >= 70 THEN 'On Track '
        WHEN MAX(lb.Lms_Overall_Score) BETWEEN 50 AND 69 THEN 'Needs Improvement '
        ELSE 'Off Track '
    END AS Learner_Status

FROM learner_base lb
GROUP BY 
    lb.First_Name, lb.Last_Name, lb.Email, lb.Phone_Number,
    CASE 
        WHEN lb.Age BETWEEN 18 AND 20 THEN '18-20'
        WHEN lb.Age BETWEEN 21 AND 23 THEN '21-23'
        WHEN lb.Age BETWEEN 24 AND 26 THEN '24-26'
        ELSE '26+'
    END;
