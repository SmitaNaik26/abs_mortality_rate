-- load the DATA to BQ
CREATE TABLE smitanaik.abs.cause_of_death_2015(
    cause_of_death STRING,
    number_males INT,
    number_females INT,
    number_persons INT,
    rate_males FLOAT64,
    rate_females FLOAT64,
    rate_persons FLOAT64,
    age_group STRING
);
CREATE TABLE smitanaik.abs.cause_of_death_2016(
    cause_of_death STRING,
    number_males INT,
    number_females INT,
    number_persons INT,
    rate_males FLOAT64,
    rate_females FLOAT64,
    rate_persons FLOAT64,
    age_group STRING
);
CREATE TABLE smitanaik.abs.cause_of_death_2017(
    cause_of_death STRING,
    number_males INT,
    number_females INT,
    number_persons INT,
    rate_males FLOAT64,
    rate_females FLOAT64,
    rate_persons FLOAT64,
    age_group STRING
);
CREATE TABLE smitanaik.abs.household_income_and_wealth (
    item_type STRING,
    item STRING,
    age_15_24 FLOAT64,
    age_25_34 FLOAT64,
    age_35_44 FLOAT64,
    age_45_54 FLOAT64,
    age_55_64 FLOAT64,
    age_65_74 FLOAT64,
    age_75_and_over FLOAT64

);
Create table smitanaik.abs.safe_user(
    user_id STRING, 
    date_of_birth DATE, 
    region STRING
);

CREATE TABLE smitanaik.abs.dim_household_income_and_wealth (
    item_type STRING,
    item STRING,
    age_15_24 FLOAT64,
    age_25_34 FLOAT64,
    age_35_44 FLOAT64,
    age_45_54 FLOAT64,
    age_55_64 FLOAT64,
    age_65_74 FLOAT64,
    age_75_and_over FLOAT64

);
CREATE TABLE IF NOT EXISTS smitanaik.abs.dim_safe_user (
    user_id STRING NOT NULL,
    date_of_birth DATE NOT NULL,
    region STRING,
    age_group STRING,
    
);

--MERGE the data to safe_user
MERGE smitanaik.abs.dim_safe_user AS target
USING (SELECT * FROM `smitanaik.abs.safe_user`) AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
    UPDATE SET
        date_of_birth = COALESCE(source.date_of_birth, DATE '1900-01-01'),
        region = source.region,
        age_group = CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 15 AND 24 THEN 'age_15_24'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 25 AND 34 THEN 'age_25_34'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 35 AND 44 THEN 'age_35_44'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 45 AND 54 THEN 'age_45_54'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 55 AND 64 THEN 'age_55_64'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 65 AND 74 THEN 'age_65_74'
            ELSE 'age_75_and_over'
        END 
WHEN NOT MATCHED THEN
    INSERT ( user_id, date_of_birth, region, age_group)
    VALUES (  source.user_id, COALESCE(source.date_of_birth, DATE '1900-01-01'), source.region, 
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 15 AND 24 THEN 'age_15_24'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 25 AND 34 THEN 'age_25_34'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 35 AND 44 THEN 'age_35_44'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 45 AND 54 THEN 'age_45_54'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 55 AND 64 THEN 'age_55_64'
            WHEN DATE_DIFF(CURRENT_DATE(), source.date_of_birth, YEAR) BETWEEN 65 AND 74 THEN 'age_65_74'
            ELSE 'age_75_and_over'
        END);

-- Merge data into dim_wealth
MERGE INTO smitanaik.abs.dim_household_income_and_wealth AS target
USING `smitanaik.abs.household_income_and_wealth` AS source
ON target.item_type = source.item_type AND target.item = source.item
WHEN MATCHED THEN
    UPDATE SET
        target.age_15_24 = source.age_15_24,
        target.age_25_34 = source.age_25_34,
        target.age_35_44 = source.age_35_44,
        target.age_45_54 = source.age_45_54,
        target.age_55_64 = source.age_55_64,
        target.age_65_74 = source.age_65_74,
        target.age_75_and_over = source.age_75_and_over
WHEN NOT MATCHED THEN
    INSERT (item_type, item, age_15_24, age_25_34, age_35_44, age_45_54, age_55_64, age_65_74, age_75_and_over)
    VALUES (source.item_type, source.item, source.age_15_24, source.age_25_34, source.age_35_44, source.age_45_54, source.age_55_64, source.age_65_74, source.age_75_and_over);

--merge the data with some calculation to fact_mortality_by_age_group
CREATE TABLE smitanaik.abs.fact_age_group_mortality_rates AS
SELECT
    CASE
        WHEN age_group = 'Under 1 year' THEN 'age_under_1'
        WHEN age_group = '1-14 years' THEN 'age_1_14'
        WHEN age_group = '15-24 years' THEN 'age_15_24'
        WHEN age_group = '25-34 years' THEN 'age_25_34'
        WHEN age_group = '35-44 years' THEN 'age_35_44'
        WHEN age_group = '45-54 years' THEN 'age_45_54'
        WHEN age_group = '55-64 years' THEN 'age_55_64'
        WHEN age_group = '65-74 years' THEN 'age_65_74'
        WHEN age_group = '75-84 years' THEN 'age_75_and_over'
        WHEN age_group = '85-94 years' THEN 'age_75_and_over'
        WHEN age_group = '95 years and over' THEN 'age_75_and_over'
        ELSE 'unknown_age_group'
    END AS age_group,
    STRING_AGG(cause_of_death, ', ') AS causes_of_death,
    ROUND(SUM(CASE WHEN year = 2015 THEN rate_males ELSE 0 END), 2) AS rate_males_2015,
    ROUND(SUM(CASE WHEN year = 2016 THEN rate_males ELSE 0 END), 2) AS rate_males_2016,
    ROUND(SUM(CASE WHEN year = 2017 THEN rate_males ELSE 0 END), 2) AS rate_males_2017,
    ROUND(SUM(CASE WHEN year = 2015 THEN rate_females ELSE 0 END), 2) AS rate_females_2015,
    ROUND(SUM(CASE WHEN year = 2016 THEN rate_females ELSE 0 END), 2) AS rate_females_2016,
    ROUND(SUM(CASE WHEN year = 2017 THEN rate_females ELSE 0 END), 2) AS rate_females_2017,
    ROUND(SUM(CASE WHEN year = 2015 THEN rate_persons ELSE 0 END), 2) AS rate_person_2015,
    ROUND(SUM(CASE WHEN year = 2016 THEN rate_persons ELSE 0 END), 2) AS rate_person_2016,
    ROUND(SUM(CASE WHEN year = 2017 THEN rate_persons ELSE 0 END), 2) AS rate_person_2017
FROM (
    SELECT
        cause_of_death,
        age_group,
        rate_males,
        rate_females,
        rate_persons,
        2015 AS year
    FROM smitanaik.abs.cause_of_death_2015
    UNION ALL
    SELECT
        cause_of_death,
        age_group,
        rate_males,
        rate_females,
        rate_persons,
        2016 AS year
    FROM smitanaik.abs.cause_of_death_2016
    UNION ALL
    SELECT
        cause_of_death,
        age_group,
        rate_males,
        rate_females,
        rate_persons,
        2017 AS year
    FROM smitanaik.abs.cause_of_death_2017
) AS combined_data
GROUP BY
    CASE
        WHEN age_group = 'Under 1 year' THEN 'age_under_1'
        WHEN age_group = '1-14 years' THEN 'age_1_14'
        WHEN age_group = '15-24 years' THEN 'age_15_24'
        WHEN age_group = '25-34 years' THEN 'age_25_34'
        WHEN age_group = '35-44 years' THEN 'age_35_44'
        WHEN age_group = '45-54 years' THEN 'age_45_54'
        WHEN age_group = '55-64 years' THEN 'age_55_64'
        WHEN age_group = '65-74 years' THEN 'age_65_74'
        WHEN age_group = '75-84 years' THEN 'age_75_and_over'
        WHEN age_group = '85-94 years' THEN 'age_75_and_over'
        WHEN age_group = '95 years and over' THEN 'age_75_and_over'
        ELSE 'unknown_age_group'
    END;

--Load the data to fact_age_group_across_net_worth
CREATE OR REPLACE TABLE smitanaik.abs.fact_age_group_across_net_worth AS
WITH unpivoted_data AS (
    -- Unpivot the age group data for household income and wealth
    SELECT 
        item_type,
        age_group,
        age_value
    FROM smitanaik.abs.dim_household_income_and_wealth,
    UNNEST([
        STRUCT('age_15_24' AS age_group, age_15_24 AS age_value),
        STRUCT('age_25_34' AS age_group, age_25_34 AS age_value),
        STRUCT('age_35_44' AS age_group, age_35_44 AS age_value),
        STRUCT('age_45_54' AS age_group, age_45_54 AS age_value),
        STRUCT('age_55_64' AS age_group, age_55_64 AS age_value),
        STRUCT('age_65_74' AS age_group, age_65_74 AS age_value),
        STRUCT('age_75_and_over' AS age_group, age_75_and_over AS age_value)
    ]) 
),
net_worths AS (
    -- Calculate the net worth for each age group
    SELECT
        age_group,
        SUM(CASE WHEN item_type IN ('Financial assets', 'Non-financial assets') THEN age_value ELSE 0 END) -
        SUM(CASE WHEN item_type IN ('Property loans', 'Other liabilities') THEN age_value ELSE 0 END) AS net_worth
    FROM unpivoted_data
    GROUP BY age_group
),
final_data AS (
    -- Select the user details, net worth, and causes of death
    SELECT
        du.user_id,
        du.age_group,
    round(nw.net_worth, 2) as net_worth
    FROM
        smitanaik.abs.dim_safe_user du
    JOIN
        net_worths nw ON du.age_group = nw.age_group
    LEFT JOIN
        smitanaik.abs.fact_age_group_mortality_rates fr
    ON
        du.age_group = fr.age_group
)
SELECT
    ROW_NUMBER() OVER() AS age_group_across_net_worth_id,
    user_id,
    age_group,
    net_worth
FROM final_data
