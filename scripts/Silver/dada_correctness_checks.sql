----------------------------------------------
-->>For crm_cust_info
--Check for NULLs or Duplicates in Primary key
--Expectation no result
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; 

--Check for unwanted spaces
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

--Data Standerdization and Consistency
SELECT DISTINCT(cst_gndr)
FROM silver.crm_cust_info;

SELECT DISTINCT(cst_marital_status)
FROM silver.crm_cust_info;
----------------------------------------------









----------------------------------------------
-->>For crm_prd_info

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')
;

--Check for NULLs or Duplicates in Primary key
--Expectation no result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; 

--Check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--Data Standerdization and Consistency
SELECT DISTINCT(prd_line)
FROM silver.crm_prd_info;

SELECT DISTINCT(cst_marital_status)
FROM silver.crm_cust_info;

--Check for nulls and negative numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL  ;

--Check for invalid Dates
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt  ;

----------------------------------------------








----------------------------------------------
-->>For crm_sales_details

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

--Check whether all sls_prd_key are available in silver_crm_prd_info
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
SELECT prd_key
FROM silver.crm_prd_info
);

--Check whether all sls_cust_id are available in silver.crm_cust_info
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
SELECT cst_id
FROM silver.crm_cust_info
);

--Check for invalid Dates
SELECT 
NULLIF(sls_due_dt,0) AS sls_due_dt
--TRY_CAST(CAST(sls_order_dt AS VARCHAR)AS DATE) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE 
sls_due_dt <= 0 
OR LEN(sls_due_dt)!= 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101;

--Check for invalid Dates orders
SELECT 
*
FROM silver.crm_sales_details
WHERE 
sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

--Check Data consistency b/w sales, quantity and price
--> Sales = Quantity * Price
--> Values must not be NULL , zero or negative
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_price AS old_sls_price,
sls_quantity,

CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE 
	WHEN sls_price IS NULL OR sls_price <= 0 
	THEN ABS(sls_sales)/NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_sales IS NULL
OR sls_quantity <= 0 OR sls_quantity IS NULL
OR sls_price <= 0 OR sls_price IS NULL
ORDER BY sls_sales,sls_quantity , sls_price;

--Check Data consistency b/w sales, quantity and price
--> Sales = Quantity * Price
--> Values must not be NULL , zero or negative
SELECT DISTINCT
sls_sales ,
sls_price ,
sls_quantity
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_sales IS NULL
OR sls_quantity <= 0 OR sls_quantity IS NULL
OR sls_price <= 0 OR sls_price IS NULL
ORDER BY sls_sales,sls_quantity , sls_price;

--Check for unwanted spaces
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

----------------------------------------------













----------------------------------------------
-->>For erp_cust_az12

--Identify out of range dates
SELECT 
*,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

SELECT * FROM silver.erp_cust_az12 WHERE bdate IS NULL;

--Data Standerdization and consistency
SELECT DISTINCT 
gen 
FROM silver.erp_cust_az12;
----------------------------------------------












----------------------------------------------
-->>For erp_loc_a101

--Data standerdization and consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;
----------------------------------------------












---------------------------------
-->>For erp_px_cat_g1v2
--Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(CAT) OR subcat!= TRIM(subcat) OR maintenence!= TRIM(maintenence) ;

--Data standerdization and consistency
SELECT DISTINCT
maintenence
FROM bronze.erp_px_cat_g1v2;
----------------------------------------------
