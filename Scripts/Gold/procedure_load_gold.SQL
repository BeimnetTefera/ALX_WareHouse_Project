USE AlxDataWareHouse;
GO

SELECT
    first_name,
    last_name,
    contact_phone,
    email,
    MAX(CASE WHEN course_name = 'BUS 200: Business Finance' THEN lms_overall_score END) AS BUS200_Finance,
    MAX(CASE WHEN course_name = 'ENT 110: Introduction to Venture Creation' THEN lms_overall_score END) AS ENT110_Venture,
    MAX(CASE WHEN course_name = 'QNT 102: Introduction to Statistics' THEN lms_overall_score END) AS QNT102_Stats,
    MAX(CASE WHEN course_name = 'SE 101: Introduction to Computing' THEN lms_overall_score END) AS SE101_Computing,
    MAX(CASE WHEN course_name = 'SE 102: Foundations of Linux and Version Control' THEN lms_overall_score END) AS SE102_Linux,
    -- add more courses if needed

    -- On track / OFF track summary
    CASE 
        WHEN SUM(CASE WHEN lms_overall_score < 70 THEN 1 ELSE 0 END) > 0 THEN 'OFF TRACK'
        ELSE 'ON TRACK'
    END AS learner_status
    
FROM silver.pw_learner_tracking_overview
GROUP BY first_name, last_name, contact_phone, email;

SELECT 
    course_name,
    SUM(CASE WHEN lms_overall_score >= 70 THEN 1 ELSE 0 END) AS OnTrack_Count,
    SUM(CASE WHEN lms_overall_score < 70 THEN 1 ELSE 0 END) AS OffTrack_Count,
    SUM(CASE WHEN lms_overall_score = NULL THEN 1 ELSE 0 END) AS notavailbale
FROM silver.pw_learner_tracking_overview
GROUP BY ROLLUP(course_name)
ORDER BY 
    CASE WHEN course_name IS NULL THEN 1 ELSE 0 END,
    course_name;    
