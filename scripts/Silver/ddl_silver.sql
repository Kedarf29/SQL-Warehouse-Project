IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_latname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

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

IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenence NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);






















--Check for NULLs or Duplicates in Primary key
--Expectation no result

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; 



















SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,

CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'FEMALE'
	 WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'MALE'
	 ELSE 'n/a'
END cst_gndr,	 
	 
cst_create_date
FROM
(
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL  
)t
WHERE flag_last = 1 ;

--WHERE cst_id = 29466;


--SELECT * FROM bronze.crm_cust_info;


--EXEC sp_rename 'bronze.crm_cust_info.cst_material_status', 'cst_marital_status', 'COLUMN';























--Check for unwanted spaces

SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);

--Data Standerdization and Consistency
SELECT DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;

SELECT DISTINCT(cst_marital_status)
FROM bronze.crm_cust_info;
