#DATA CLEANING

#AGENDA 
-- 1. REMOVING DUPLICATES
-- 2. STANDARDISING THE DATA
-- 3. NULL AND BLANK VALUES

select * from layoffs;

#----------------------------------------------------------------------------------

-- CREATING A DUPLICATE TABLE SO THAT WE CAN MAKE ANY NO OF CHANGES 
-- WITHOUT DISTURBING THE ORIGINAL DATA

CREATE TABLE layoffs_staging LIKE layoffs;    -- creates an empty table with columns like layoffs

SELECT *
FROM layoffs_staging;

-- Inserting the values
insert layoffs_staging
SELECT *
FROM layoffs;  

#----------------------------------------------------------------------------------

-- since our data has no row numbers lets assign them

select *, 
row_number() 
over(partition by company,location,industry,         -- assigning the row_numbers to each
		total_laid_off, percentage_laid_off, `date`, -- unique row if there are duplicates
		stage,country,funds_raised_millions) as row_num -- we will get 2,3.. row number
from layoffs_staging;

#----------------------------------------------------------------------------------

#CHECKING THE DUPLICATES
with duplicate_cte as (select *, 
row_number() 
over(partition by company,location,industry,         
		total_laid_off, percentage_laid_off, `date`, 
		stage,country,funds_raised_millions) as row_num 
		from layoffs_staging)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


select * from layoffs_staging
where company = 'casper';

-- let's make another table and delete duplicates from that
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert layoffs_staging2
select *, 
row_number() 
over(partition by company,location,industry,         
		total_laid_off, percentage_laid_off, `date`, 
		stage,country,funds_raised_millions) as row_num 
		from layoffs_staging;
        
DELETE
from layoffs_staging2
where row_num > 1;

SELECT COUNT(*)
from layoffs_staging2
;                     -- layoffs_staging2 now has no duplicates


-- as we deleted the duplicate rows we don't need row_num
alter table layoffs_staging2
drop column row_num;

#----------------------------------------------------------------------------------
#STANDARDISING DATA : finding issues and fixing them

-- let's remove the extra spaces

select company, trim(company)
from layoffs_staging2;       -- checking to see the extra spaces

UPDATE layoffs_staging2
set company = trim(company);   -- updating the trimmed values

-- CHECKING FOR THE DISTINCT INDUSTRIES
-- if there are any duplicates or same industries let's update them

select distinct industry from layoffs_staging2; -- we have crypto and crypto currency lets change that

select * from layoffs_staging2
where industry like 'Crypto';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';  -- it's updated now 

select count(*) from layoffs_staging2 
where industry = 'Crypto Currency';

-- let's check other columns
select distinct location from layoffs_staging2
order by 1;  -- these look fine

select distinct country from layoffs_staging2
order by 1;  -- we got two US : United States , United States.

-- let's fix that using trim trailing method
select distinct country, trim(trailing '.' from country)
from layoffs_staging2 order by 1;

-- let's update
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select distinct country
from layoffs_staging2
order by 1;

-- our date is in text format let's change that

select `date`
from layoffs_staging2;            -- this changes the dates like this 2022-12-16

update layoffs_staging2
set `date` = str_to_date(`date` , '%m/%d/%Y') ;
 
-- when we look at the data type its still the text so let's fix that

alter table layoffs_staging2
modify column `date` DATE;         -- this will modify the text col to date col

#----------------------------------------------------------------------------------

-- NULL AND BLANK VALUES

-- let's go step by step now

select * from layoffs_staging2
where industry is null or industry = '';   -- we got 4 companies

-- let's replace the empty spaces with null
update layoffs_staging2
set industry = null
where industry = '';           

-- all this code does is checks that if there are same companies
-- if they are available we can assign their industries to the nulls

select t1.company, t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;   

-- we got the industries let's update

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- let's check for any other industry null values
select * from layoffs_staging2       -- we got a single company which has null
where industry is null;


-- when we look up the data we see that
-- some of total_laid and percentage laid are null and they are not 
-- useful and we can't add values to them like above so lets check and delete

select * from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

-- let's delete them

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2;












