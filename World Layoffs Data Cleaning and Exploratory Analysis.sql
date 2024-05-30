# World Layoff Data Cleaning and Exploratory Analysis 
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

#--------------------------------------------------------------------------- Data Cleaning -----------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT * 
FROM world_layoffs.layoffs;

#Create a staging table. This is the one we will work in and clean the data.
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


# 1. Check for duplicates and remove any ----------------------------------------------------------------

##Checking for duplicates 
SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
-- looks like there are duplicates. 
    
##Looking into specifics to double check
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- Oda has legit entries! We need to be more specific and look into all rows for accuracy. 

##Checking the real/accurate duplicates
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
-- We need to delete the ones with row numbers above 1! 


##Add new column with row numbers in and then delete the 1+, so they are just 1. 
ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
        
##Now actually delete! 
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;



# 2. Standardize data -------------------------------------------------------------------------

SELECT * 
FROM world_layoffs.layoffs_staging2;

##Industry has nulls/empty space
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


##Checking where we saw nulls 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';
-- airbnb is travel industry, the null just was not populated. Need to update this to populate the field. Join to itself and then update to fill correct industry. 


#Joining the tables to set the correct industries for those that can! 
SELECT * 
FROM world_layoffs.layoffs_staging2 t1 
JOIN world_layoffs.layoffs_staging2 t2 
    ON t1.company = t2.company 
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;
-- this is not working, so we will set them to nulls and see if we can figure it out from there 


##Setting these to nulls and then rerunning the above section! 
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Bally's was the only one left, lets check into it. 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- all good in this part -- there is only one, so this cannot be updated as we cannot update in the same way. 


#Double checking work to make sure bally's is only one. 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
    OR industry = ''
ORDER BY industry;


##Fixing the spacing 
SELECT company, TRIM(company)
FROM world_layoffs.layoffs_staging2;

Update world_layoffs.layoffs_staging2
SET company = TRIM(company);

##Crypto can be consolidated/variations 
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');
-- checking again

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;


##Checking 
SELECT *
FROM world_layoffs.layoffs_staging2;
-- everything but punctuation in United States 

##Fixing the US
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);
-- now if we run this again it is fixed

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

##Fix the date columns
SELECT *
FROM world_layoffs.layoffs_staging2;

##Str to date to update
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

##Convert date data type 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2;



#3. Look at null values and blanks -----------------------------------------------------------------

## Nothing in this section to change. 
## Null values in: total_laid_off, percentage_laid_off, and funds_raised_millions 
## Keep them as NULLS so calculations are easier. 



#4. Remove any unneccesary -----------------------------------------------------------------------
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

##Delete "useless" data that we cannot use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


##Double Checking and seeing what more 
SELECT * 
FROM world_layoffs.layoffs_staging2;
-- no longer need the row_num

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;


#------------------------------------------------------------------------- Exploratory Analysis ------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT * 
FROM world_layoffs.layoffs_staging2;


##Basic exploration of layoffs 

## What was the biggest layoff? 
SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;
-- 12,000 people being laid off in one day!

## How much of the company was laid off? 
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;
-- Looking to see how big or small these layoffs were 

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;
-- 100% of company was laid off

## Which companies had 100% layoffs? 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- mostly startups that had to shut down. 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY total_laid_off DESC; 
-- Katerra (construction) had the highest layoff when it shutdown.

## How big are these companies? 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- some very big companies that went under (raising like $2billion)

## What companies had the biggest single layoff? 
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- Google (12,000), Meta (11,000), and Amazon (10,000) had the highest layoffs in one go. 

## What companies had the most total layoffs? 
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;
-- Amazon (18,150), Google (12,000), and Meta (11,000) had the most total layoffs. 

## When are these layoffs happening? 
SELECT MIN(`date`), MAX(`date`)
FROM world_layoffs.layoffs_staging2;
-- these layoffs are happening during March 3rd 2020 to March 6th of 2023 (covid 19 era, so wonder if data would be different otherwise). 

# How many people are being laid off each year? 
SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;
-- 2022 had the highest layoff numbers (for a full year) with 160,661. With only 3 months of data in 2023, there are 125,677 people laid off so far.

## What are the global layoff numbers? 
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
-- United states by far had the most layoffs reported in this dataset. 

## Where are these layoffs happening specifically? 
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;
-- Mostly in the United States, San Fransicso Bay area (125,631), Seattle (34,743) and New York City (29,364) were top three highest places for layoffs. 

## What industry are people getting laid off in? 
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
-- Consumer, retail, "other", and transportation had the most layoffs. 

## What stage of buisness are people getting laid off in? 
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;
-- Post-IPO had the most layoffs (think bigger companies, amazon, google, etc). 


## Rolling Layoffs by month

SELECT SUBSTRING(`date`,1,7) as `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- creating CTE to then make queries off of it. 
WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`,1,7) as `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER (ORDER BY `MONTH` ) as rolling_total_layoffs
FROM Rolling_Total
;
-- Can see the amount of people getting laid off each month and also how it adds up throughout the months/years. 
-- By end of 2021 there were only 96,821 lay offs all year (not as bad as anticpated!). End of year 2022 and beginning of 2023 layoffs starts ramping up dramatically.


## Companies layoff each year (who laid off the most people per year?)

  SELECT company, YEAR(`date`), SUM(total_laid_off) 
  FROM layoffs_staging2
  GROUP BY company, YEAR(`date`)
  ORDER BY 3 DESC;
  
  #Create CTE 
WITH Company_Year AS 
(
  SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(`date`)
  ORDER BY company ASC
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;
-- can now see what companies laid off the most people each year (and their ranking in top three). 




