IF OBJECT_ID('gold.pw_learner_activity', 'U') IS NOT NULL
    DROP TABLE gold.pw_learner_activity;

CREATE TABLE gold.pw_learner_activity(
	First_Name NVARCHAR(50),
	Last_Name NVARCHAR(50),
	Phone_Number NVARCHAR(13),
	Email NVARCHAR(150),
	Course_Name NVARCHAR(MAX),
	LMS_Overall_Score FLOAT,
	LMS_Greater_than_70 NVARCHAR(5),
	On_Track NVARCHAR(5)
)
