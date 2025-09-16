BULK INSERT bronze.pw_learner_activity
FROM 'C:\project\alx\Term 2_ Pathway C1 Learners Academic Activity_August 19th - Learners Activity.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FORMAT = 'CSV',
    CODEPAGE = '65001',
    TABLOCK
);


BULK INSERT bronze.pw_learner_tracking_overview
FROM 'C:\project\alx\Term 2_ Pathway C1 Learners Academic Activity_August 19th - Learners Tracking Overview.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '\n',
    FIELDQUOTE = '"',
    CODEPAGE = '65001',  -- UTF-8
    KEEPNULLS,
    TABLOCK,
    FORMAT = 'CSV'
);
