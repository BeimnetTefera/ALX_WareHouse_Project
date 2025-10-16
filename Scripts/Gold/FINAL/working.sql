CREATE OR ALTER FUNCTION dbo.SplitCSV
(
    @Line NVARCHAR(MAX)
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        value AS ColumnValue,
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ColumnNumber
    FROM STRING_SPLIT(@Line, ',')
);
GO

USE AlxDataWareHouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @csvFilePath NVARCHAR(500) = 'C:\SQLServerData\Student_Activity_1759808710035.csv';

    BEGIN TRY
        PRINT '============================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '============================================================';

        -- Check if file exists
        DECLARE @fileExists INT;
        EXEC master.dbo.xp_fileexist @csvFilePath, @fileExists OUTPUT;

        IF @fileExists = 0
        BEGIN
            PRINT 'ERROR: CSV file does not exist at ' + @csvFilePath;
            RETURN;
        END

        PRINT 'CSV file found. Starting load...';

        -- Truncate table before load
        PRINT '>> Truncating Table: bronze.pw_learner_activity';
        TRUNCATE TABLE bronze.pw_learner_activity;

        -- Load CSV as SINGLE_CLOB
        DECLARE @csv NVARCHAR(MAX);

        SELECT @csv = BulkColumn
        FROM OPENROWSET(
            BULK 'C:\SQLServerData\Student_Activity_1759808710035.csv',
            SINGLE_CLOB
        ) AS t;

        -- Split CSV into lines and skip header
        WITH Lines AS (
            SELECT value AS Line, ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS rn
            FROM STRING_SPLIT(@csv, CHAR(10))
            WHERE TRIM(value) <> ''
        )
        INSERT INTO bronze.pw_learner_activity (
            first_name, last_name, email, contact_phone, age, age_range,
            country_of_residence, category_country_of_residences, city_of_residence,
            day_of_activation, cohort_name, class_name, location_program_cohort,
            course_name, assignment_name, assignment_type, assignment_score,
            is_assignment_accessed, is_assignment_passed, learner_enrolled_into_lms,
            has_logged_into_lms, has_logged_into_eHub, has_shown_up_in_circle,
            has_shown_up_in_circle_during_the_last_two_days, is_engaged_in_circle,
            is_engaged_in_circle_during_the_last_two_days, enrollment_program_activated,
            enrollment_in_previous_withdrawn_class, enrollment_course_activated,
            is_program_withdrawn, program_withdrawal_reason, program_withdrawn_date,
            is_course_deferred, course_deferred_date, on_track_for_on_time_graduation,
            on_track_for_delayed_graduation, is_assignment_submitted,
            is_assignment_resubmitted, is_assignment_resubmitted_and_not_corrected_yet,
            is_assignment_graded, is_assignment_corrected_by_an_external_grader,
            corrected_external_grader_name, corrected_external_grader_email, is_on_track,
            no_of_assignment_submissions, submission_link, no_of_assignmnets,
            no_of_submissions, no_of_assignments_passed, no_of_milestone,
            no_of_milestone_submissions, no_of_milestones_passed, no_of_quizzes,
            no_of_quizzes_submitted, no_of_quizzes_passed, no_of_tests,
            no_of_tests_submissions, no_of_tests_passed, lms_overall_score,
            is_lms_overall_score_higher_than_score_threshold, lms_course_status,
            ehub_course_status, ehub_class_status, is_course_paused,
            course_leave_of_absence_reason
        )
        SELECT
            MAX(CASE WHEN ColumnNumber = 1 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 2 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 3 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 4 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 5 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 6 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 7 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 8 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 9 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 10 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 11 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 12 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 13 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 14 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 15 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 16 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 17 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 18 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 19 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 20 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 21 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 22 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 23 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 24 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 25 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 26 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 27 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 28 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 29 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 30 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 31 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 32 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 33 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 34 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 35 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 36 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 37 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 38 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 39 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 40 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 41 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 42 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 43 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 44 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 45 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 46 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 47 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 48 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 49 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 50 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 51 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 52 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 53 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 54 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 55 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 56 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 57 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 58 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 59 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 60 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 61 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 62 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 63 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 64 THEN ColumnValue END),
            MAX(CASE WHEN ColumnNumber = 65 THEN ColumnValue END)
        FROM Lines l
        CROSS APPLY dbo.SplitCSV(Line)
        WHERE rn > 1  -- skip header
        GROUP BY rn;

        PRINT '>> Bronze layer load completed successfully.';
        PRINT '============================================================';

    END TRY
    BEGIN CATCH
        PRINT '============================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================================';
    END CATCH
END;
GO


EXEC bronze.load_bronze;

select * from  bronze.pw_learner_activity;


-- Remove quotes from all NVARCHAR columns
UPDATE bronze.pw_learner_activity
SET
    first_name = REPLACE(first_name, '"', ''),
    last_name = REPLACE(last_name, '"', ''),
    email = REPLACE(email, '"', ''),
    contact_phone = REPLACE(contact_phone, '"', ''),
    age = REPLACE(age, '"', ''),
    age_range = REPLACE(age_range, '"', ''),
    country_of_residence = REPLACE(country_of_residence, '"', ''),
    category_country_of_residences = REPLACE(category_country_of_residences, '"', ''),
    city_of_residence = REPLACE(city_of_residence, '"', ''),
    day_of_activation = REPLACE(day_of_activation, '"', ''),
    cohort_name = REPLACE(cohort_name, '"', ''),
    class_name = REPLACE(class_name, '"', ''),
    location_program_cohort = REPLACE(location_program_cohort, '"', ''),
    course_name = REPLACE(course_name, '"', ''),
    assignment_name = REPLACE(assignment_name, '"', ''),
    assignment_type = REPLACE(assignment_type, '"', ''),
    assignment_score = REPLACE(assignment_score, '"', ''),
    is_assignment_accessed = REPLACE(is_assignment_accessed, '"', ''),
    is_assignment_passed = REPLACE(is_assignment_passed, '"', ''),
    learner_enrolled_into_lms = REPLACE(learner_enrolled_into_lms, '"', ''),
    has_logged_into_lms = REPLACE(has_logged_into_lms, '"', ''),
    has_logged_into_eHub = REPLACE(has_logged_into_eHub, '"', ''),
    has_shown_up_in_circle = REPLACE(has_shown_up_in_circle, '"', ''),
    has_shown_up_in_circle_during_the_last_two_days = REPLACE(has_shown_up_in_circle_during_the_last_two_days, '"', ''),
    is_engaged_in_circle = REPLACE(is_engaged_in_circle, '"', ''),
    is_engaged_in_circle_during_the_last_two_days = REPLACE(is_engaged_in_circle_during_the_last_two_days, '"', ''),
    enrollment_program_activated = REPLACE(enrollment_program_activated, '"', ''),
    enrollment_in_previous_withdrawn_class = REPLACE(enrollment_in_previous_withdrawn_class, '"', ''),
    enrollment_course_activated = REPLACE(enrollment_course_activated, '"', ''),
    is_program_withdrawn = REPLACE(is_program_withdrawn, '"', ''),
    program_withdrawal_reason = REPLACE(program_withdrawal_reason, '"', ''),
    program_withdrawn_date = REPLACE(program_withdrawn_date, '"', ''),
    is_course_deferred = REPLACE(is_course_deferred, '"', ''),
    course_deferred_date = REPLACE(course_deferred_date, '"', ''),
    on_track_for_on_time_graduation = REPLACE(on_track_for_on_time_graduation, '"', ''),
    on_track_for_delayed_graduation = REPLACE(on_track_for_delayed_graduation, '"', ''),
    is_assignment_submitted = REPLACE(is_assignment_submitted, '"', ''),
    is_assignment_resubmitted = REPLACE(is_assignment_resubmitted, '"', ''),
    is_assignment_resubmitted_and_not_corrected_yet = REPLACE(is_assignment_resubmitted_and_not_corrected_yet, '"', ''),
    is_assignment_graded = REPLACE(is_assignment_graded, '"', ''),
    is_assignment_corrected_by_an_external_grader = REPLACE(is_assignment_corrected_by_an_external_grader, '"', ''),
    corrected_external_grader_name = REPLACE(corrected_external_grader_name, '"', ''),
    corrected_external_grader_email = REPLACE(corrected_external_grader_email, '"', ''),
    is_on_track = REPLACE(is_on_track, '"', ''),
    no_of_assignment_submissions = REPLACE(no_of_assignment_submissions, '"', ''),
    submission_link = REPLACE(submission_link, '"', ''),
    no_of_assignmnets = REPLACE(no_of_assignmnets, '"', ''),
    no_of_submissions = REPLACE(no_of_submissions, '"', ''),
    no_of_assignments_passed = REPLACE(no_of_assignments_passed, '"', ''),
    no_of_milestone = REPLACE(no_of_milestone, '"', ''),
    no_of_milestone_submissions = REPLACE(no_of_milestone_submissions, '"', ''),
    no_of_milestones_passed = REPLACE(no_of_milestones_passed, '"', ''),
    no_of_quizzes = REPLACE(no_of_quizzes, '"', ''),
    no_of_quizzes_submitted = REPLACE(no_of_quizzes_submitted, '"', ''),
    no_of_quizzes_passed = REPLACE(no_of_quizzes_passed, '"', ''),
    no_of_tests = REPLACE(no_of_tests, '"', ''),
    no_of_tests_submissions = REPLACE(no_of_tests_submissions, '"', ''),
    no_of_tests_passed = REPLACE(no_of_tests_passed, '"', ''),
    lms_overall_score = REPLACE(lms_overall_score, '"', ''),
    is_lms_overall_score_higher_than_score_threshold = REPLACE(is_lms_overall_score_higher_than_score_threshold, '"', ''),
    lms_course_status = REPLACE(lms_course_status, '"', ''),
    ehub_course_status = REPLACE(ehub_course_status, '"', ''),
    ehub_class_status = REPLACE(ehub_class_status, '"', ''),
    is_course_paused = REPLACE(is_course_paused, '"', ''),
    course_leave_of_absence_reason = REPLACE(course_leave_of_absence_reason, '"', '');
