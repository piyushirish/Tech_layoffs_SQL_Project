-- SQL Project - Data Cleaning


use Tech_layoffs;

SELECT * 
FROM Tech_layoffs.layoffs;



-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE Tech_layoffs.layoffs_staging 
LIKE Tech_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM Tech_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM Tech_layoffs.layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates



SELECT *
FROM Tech_layoffs.layoffs_staging;


SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		Tech_layoffs.layoffs_staging;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		Tech_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM Tech_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised
			) AS row_num
	FROM 
		Tech_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- now you may want to write it like this:

SET SQL_SAFE_UPDATES = 0;


WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised
			) AS row_num
	FROM 
		Tech_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;

select * 
from Tech_layoffs.layoffs_staging;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
	FROM Tech_layoffs.layoffs_staging
)
DELETE FROM Tech_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

ALTER TABLE Tech_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM Tech_layoffs.layoffs_staging
;

UPDATE Tech_layoffs.layoffs_staging
SET total_laid_off = NULL
WHERE total_laid_off = '';


CREATE TABLE `Tech_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`source` text,
`stage`text,
`country` text,
`funds_raised` text,
`date_added` text,
row_num INT
);

INSERT INTO `Tech_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`source` ,
`stage`,
`country`,
`funds_raised`,
`date_added` ,
`row_num`)
SELECT 
  `company`,
  `location`,
  `industry`,
  `total_laid_off`,
  `percentage_laid_off`,
  `date`,
  `source`,
  `stage`,
  `country`,

  -- Store cleaned funds_raised (remove $ and allow empty)
  REPLACE(`funds_raised`, '$', '') AS funds_raised,

  `date_added`,

  -- Apply row_number using cleaned funds_raised
  ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off,
                 percentage_laid_off, `date`, stage, country,
                 REPLACE(funds_raised, '$', '')
  ) AS row_num
FROM 
  Tech_layoffs.layoffs_staging;

select *
from Tech_layoffs.layoffs_staging2;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM Tech_layoffs.layoffs_staging2
WHERE row_num >= 2;

select *
from Tech_layoffs.layoffs_staging2;





-- 2. Standardize Data

SELECT * 
FROM Tech_layoffs.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM Tech_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM Tech_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM Tech_layoffs.layoffs_staging2
WHERE company LIKE 'Appsm%';


-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE Tech_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM Tech_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Appsmith's was the only one without a populated row to populate this null values
SELECT *
FROM Tech_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

SELECT DISTINCT industry
FROM Tech_layoffs.layoffs_staging2
ORDER BY industry;

-- I have also noticed that some conpany's have other as a industry there is no specific industry name for those companys's

select *
from Tech_layoffs.layoffs_staging2
where industry = 'Other';

-- now that's taken care of:
SELECT DISTINCT industry
FROM Tech_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM Tech_layoffs.layoffs_staging2;

-- lets look at the country column also

SELECT DISTINCT country
FROM Tech_layoffs.layoffs_staging2
ORDER BY country;

-- It looks like everything is good in this column

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM Tech_layoffs.layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM Tech_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = CAST(`date` AS DATE);

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SHOW COLUMNS FROM layoffs_staging2;


SELECT *
FROM Tech_layoffs.layoffs_staging2;


-- now we look on the percentage_laid_off column we can see there are some empty values so convert them to null values first

select *
from Tech_layoffs.layoffs_staging2
where percentage_laid_off = '';

update Tech_layoffs.layoffs_staging2
set percentage_laid_off = null
where percentage_laid_off = '';

SELECT *
FROM Tech_layoffs.layoffs_staging2;


-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised all look normal. I don't think I want to change that
-- we can also remove them where the null values 
-- so there isn't anything I want to change with the null values




-- 4. remove any columns and rows we need to

SELECT *
FROM Tech_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM Tech_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM Tech_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM Tech_layoffs.layoffs_staging2;

-- now we can remove row_num column as it is not usefull anymore

ALTER TABLE Tech_layoffs.layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM Tech_layoffs.layoffs_staging2;

-- now look at the funds_raised column as we have some empty values in that column also

select *
from Tech_layoffs.layoffs_staging2
where funds_raised = '';

-- set to null

update Tech_layoffs.layoffs_staging2
set funds_raised = null
where funds_raised = '';

SELECT * 
FROM Tech_layoffs.layoffs_staging2;