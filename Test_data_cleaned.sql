-- =====================================================================
-- NBFC Vehicle Loan – MySQL Cleaning & EDA Toolkit (MySQL 8 Compatible)
-- Author: <your name>
-- =====================================================================

USE test_data_cleaned;  -- change to your DB name

-- 1) Backup -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS loan_raw_data_backup LIKE loan_raw_data_staging1;
INSERT IGNORE INTO loan_raw_data_backup SELECT * FROM loan_raw_data_staging1;

-- 2) Clean canonical view ------------------------------------------------
DROP VIEW IF EXISTS loan_clean_v;
CREATE VIEW loan_clean_v AS
SELECT
    ID,
    NULLIF(Client_Income, -1) AS Client_Income,
    NULLIF(Credit_Amount, -1) AS Credit_Amount,
    NULLIF(Loan_Annuity, -1) AS Loan_Annuity,
    NULLIF(Child_Count, -1) AS Child_Count,
    NULLIF(Client_Family_Members, -1) AS Client_Family_Members,
    NULLIF(Cleint_City_Rating, -1) AS Client_City_Rating,

    CASE WHEN Age_Days IS NULL OR Age_Days = -1 THEN NULL ELSE ROUND(ABS(Age_Days)/365.25,1) END AS Age_Years,
    CASE WHEN Employed_Days IS NULL OR Employed_Days = -1 THEN NULL ELSE ROUND(ABS(Employed_Days)/365.25,1) END AS Employed_Years,

    CASE WHEN UPPER(TRIM(Car_Owned)) IN ('Y','YES','1') THEN 1 WHEN UPPER(TRIM(Car_Owned)) IN ('N','NO','0') THEN 0 ELSE NULL END AS Car_Owned,
    CASE WHEN UPPER(TRIM(Bike_Owned)) IN ('Y','YES','1') THEN 1 WHEN UPPER(TRIM(Bike_Owned)) IN ('N','NO','0') THEN 0 ELSE NULL END AS Bike_Owned,
    CASE WHEN UPPER(TRIM(Active_Loan)) IN ('Y','YES','1') THEN 1 WHEN UPPER(TRIM(Active_Loan)) IN ('N','NO','0') THEN 0 ELSE NULL END AS Active_Loan,
    CASE WHEN UPPER(TRIM(House_Own)) IN ('Y','YES','1') THEN 1 WHEN UPPER(TRIM(House_Own)) IN ('N','NO','0') THEN 0 ELSE NULL END AS House_Own,

    NULLIF(TRIM(Client_Income_Type), '') AS Client_Income_Type,
    NULLIF(TRIM(Client_Education), '') AS Client_Education,
    NULLIF(TRIM(Client_Marital_Status), '') AS Client_Marital_Status,
    NULLIF(TRIM(Client_Gender), '') AS Client_Gender,
    NULLIF(TRIM(Type_Organization), '') AS Type_Organization,

    CASE
      WHEN ABS(Age_Days)/365.25 < 25 THEN '<25'
      WHEN ABS(Age_Days)/365.25 < 35 THEN '25–35'
      WHEN ABS(Age_Days)/365.25 < 50 THEN '35–50'
      ELSE '50+'
    END AS Age_Bucket
FROM loan_raw_data_staging1;

-- 3) Missingness Audit --------------------------------------------------
DROP PROCEDURE IF EXISTS sp_profile_missingness;
DELIMITER $$
CREATE PROCEDURE sp_profile_missingness()
BEGIN
  SELECT 'Client_Income' AS col, SUM(Client_Income IS NULL) AS nulls FROM loan_clean_v
  UNION ALL SELECT 'Credit_Amount', SUM(Credit_Amount IS NULL) FROM loan_clean_v
  UNION ALL SELECT 'Loan_Annuity', SUM(Loan_Annuity IS NULL) FROM loan_clean_v
  UNION ALL SELECT 'Client_Gender', SUM(Client_Gender IS NULL) FROM loan_clean_v
  UNION ALL SELECT 'Age_Years', SUM(Age_Years IS NULL) FROM loan_clean_v;
END $$
DELIMITER ;
-- Run: CALL sp_profile_missingness();

-- 4) Outlier Detection using NTILE --------------------------------------
DROP VIEW IF EXISTS outliers_income_v;
CREATE VIEW outliers_income_v AS
WITH q AS (
  SELECT Client_Income,
         NTILE(4) OVER (ORDER BY Client_Income) AS quartile
  FROM loan_clean_v WHERE Client_Income IS NOT NULL
)
SELECT
  (SELECT MAX(Client_Income) FROM q WHERE quartile=1) AS q1,
  (SELECT MAX(Client_Income) FROM q WHERE quartile=2) AS q2,
  (SELECT MAX(Client_Income) FROM q WHERE quartile=3) AS q3,
  (SELECT MAX(Client_Income) FROM q WHERE quartile=4) AS q4;

-- 5) Vehicle vs Loan ----------------------------------------------------
DROP VIEW IF EXISTS vehicle_vs_loan_v;
CREATE VIEW vehicle_vs_loan_v AS
SELECT Car_Owned, Bike_Owned,
       COUNT(*) AS applicants,
       AVG(Credit_Amount) AS avg_loan
FROM loan_clean_v
GROUP BY Car_Owned, Bike_Owned;

-- 6) Employment vs Loan -------------------------------------------------
DROP VIEW IF EXISTS employment_vs_loan_v;
CREATE VIEW employment_vs_loan_v AS
SELECT Client_Income_Type,
       COUNT(*) AS applicants,
       AVG(Credit_Amount) AS avg_loan
FROM loan_clean_v
GROUP BY Client_Income_Type;

-- 7) Age vs Loan --------------------------------------------------------
DROP VIEW IF EXISTS agebucket_vs_loan_v;
CREATE VIEW agebucket_vs_loan_v AS
SELECT Age_Bucket,
       COUNT(*) AS applicants,
       AVG(Credit_Amount) AS avg_loan
FROM loan_clean_v
GROUP BY Age_Bucket
ORDER BY FIELD(Age_Bucket,'<25','25–35','35–50','50+');

-- 8) Gender vs Loan -----------------------------------------------------
DROP VIEW IF EXISTS gender_vs_loan_v;
CREATE VIEW gender_vs_loan_v AS
SELECT Client_Gender,
       COUNT(*) AS applicants,
       AVG(Credit_Amount) AS avg_loan
FROM loan_clean_v
GROUP BY Client_Gender;

-- 9) Region/City Rating vs Loan ----------------------------------------
DROP VIEW IF EXISTS region_vs_loan_v;
CREATE VIEW region_vs_loan_v AS
SELECT CASE
         WHEN Client_City_Rating <= 2 THEN 'Low Rating (<=2)'
         WHEN Client_City_Rating <= 4 THEN 'Mid Rating (<=4)'
         ELSE 'High Rating (>4)'
       END AS city_segment,
       COUNT(*) AS applicants,
       AVG(Credit_Amount) AS avg_loan
FROM loan_clean_v
GROUP BY city_segment;

-- 10) Fraud Flags -------------------------------------------------------
DROP VIEW IF EXISTS fraud_flags_v;
CREATE VIEW fraud_flags_v AS
SELECT ID,
       CASE WHEN Credit_Amount > 8*Client_Income THEN 1 ELSE 0 END AS flag_income_mismatch,
       CASE WHEN Employed_Years IS NOT NULL AND Employed_Years < 0.5 THEN 1 ELSE 0 END AS flag_short_employment
FROM loan_clean_v;

-- 11) Fraud Flags Summary -----------------------------------------------
DROP VIEW IF EXISTS fraud_flags_summary_v;
CREATE VIEW fraud_flags_summary_v AS
SELECT SUM(flag_income_mismatch) AS income_mismatch,
       SUM(flag_short_employment) AS short_employment,
       COUNT(*) AS total
FROM fraud_flags_v;

-- 12) Indexes (manual creation only) -----------------------------------
-- Run this after checking existing indexes:
-- SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS 
-- WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'loan_raw_data_staging1';

-- CREATE INDEX idx_vehicle_loan ON loan_raw_data_staging1 (Car_Owned, Bike_Owned);
-- CREATE INDEX idx_income_type ON loan_raw_data_staging1 (Client_Income_Type);
-- CREATE INDEX idx_gender ON loan_raw_data_staging1 (Client_Gender);
-- CREATE INDEX idx_cityrating ON loan_raw_data_staging1 (Cleint_City_Rating);

-- =====================================================================
-- END OF TOOLKIT (MySQL 8 Compatible)
-- =====================================================================

CALL sp_profile_missingness();

SELECT * FROM fraud_flags_summary_v;


