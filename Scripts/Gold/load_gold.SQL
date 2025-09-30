USE AlxDataWareHouse;
GO

-- 2. Insert data from Silver into Gold
INSERT INTO gold.pw_learner_activity(
  First_Name,
  Last_Name,
  Phone_Number,
	Email,
	Course_Name,
	LMS_Overall_Score,
	LMS_Greater_than_70 ,
	On_Track 
)

SELECT
    first_name,
    last_name,
    contact_phone,
    email,
    course_name,
    lms_overall_score,
    CASE 
        WHEN lms_overall_score >= 70 THEN 'YES'
        ELSE 'NO'
    END AS is_lms_overall_score_higher_than_threshold,
    CASE 
        WHEN lms_overall_score >= 70 THEN 'YES'
        ELSE 'NO'
    END AS is_on_track
FROM silver.pw_learner_activity;
