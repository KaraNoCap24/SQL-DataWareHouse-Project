/*	
This Script is responsible for loading data from the bronze layer to the silver layer in a data warehouse. It performs the following steps:
	1) Truncates the existing data in the silver layer tables to ensure a clean slate for loading new data.
	2) Inserts data into the silver layer tables from the corresponding bronze layer tables,
		applying necessary transformations and data cleansing operations.
	3) Measures and prints the time taken for each table to load, as well as the total time taken to load the entire silver layer.
	4) Implements error handling to catch and report any issues that occur during the loading process, ensuring that any errors are 
		logged with relevant details for troubleshooting.
	5) The procedure is designed to be executed on a scheduled basis to keep the silver layer up-to-date with the latest data from the bronze layer, 
		allowing for efficient data processing and analysis in the data warehouse environment.
Warnings: 
	1) Ensure that the bronze layer tables contain valid and clean data before executing this procedure, as any issues in the source 
		data may affect the integrity of the silver layer.
	2) The truncation of tables in the silver layer will result in the loss of existing data, so it is crucial to have a backup or
		ensure that the data can be reloaded if necessary.
	3) Monitor the execution time of the procedure, especially for larger datasets, as it may take longer to load data into the silver layer, 
		and consider optimizing queries if performance issues arise.
How to Execute: 
	1) Ensure that you have the necessary permissions to execute stored procedures and access the relevant tables in the database.
	2) Execute the stored procedure using the following command:
		EXEC silver.load_silver;
	3) Monitor the output messages for any errors or warnings that may occur during the execution, and review the time taken for each table to 
		load for performance insights.
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================================================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '======================================================================================================================';
	
		PRINT '---------------------------------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------------------------------------------------------------------';
/*=======================================================================================================================*/
SET @start_time = GETDATE();
	PRINT '>> Truncating Data From: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
		cst_id, 
		cst_key, 
		cst_firstname,
		cst_lastname, 
		cst_material_status, 
		cst_gndr, 
		cst_create_date)
	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_material_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM(
		SELECT 
		* ,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
	)t WHERE flag_last = 1
SET @end_time = GETDATE();
PRINT '>> Time taken to load silver.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------------------------------------------------------------------------------------------------';
--=====================================================================================================================--
SET @start_time = GETDATE();
	PRINT '>> Truncating Data From: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
	prd_id, 
	cat_id,
	prd_key, 
	prd_nm, 
	prd_cost,
	prd_line, 
	prd_start_dt,
	prd_end_dt
	)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,   
		SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,           
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,                 
		CASE UPPER(TRIM(prd_line))                      
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,										
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS DATE) AS prd_end_dt  
	FROM bronze.crm_prd_info
SET @end_time = GETDATE();
PRINT '>> Time taken to load silver.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------------------------------------------------------------------------------------------------';
--=====================================================================================================================--
SET @start_time = GETDATE();
	PRINT '>> Truncating Data From: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_date,												
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,													
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
		sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0) 
		ELSE sls_price
	END AS sls_price												
	FROM bronze.crm_sales_details
SET @end_time = GETDATE();
PRINT '>> Time taken to load silver.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------------------------------------------------------------------------------------------------';
	--==============================================================================================================--
	SET @start_time = GETDATE();
	PRINT '---------------------------------------------------------------------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '---------------------------------------------------------------------------------------------------------------';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Truncating Data From: silver.erp_cust_az12';
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE () THEN NULL
		ELSE bdate
	END AS bdate,													
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen														
	FROM bronze.erp_cust_az12
	SET @end_time = GETDATE();
	PRINT '>> Time taken to load silver.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------------------------------------------------';
	--==============================================================================================================--
	SET @start_time = GETDATE();
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Truncating Data From: silver.erp_loc_a101';
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	SELECT
	REPLACE(cid, '-','') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry                                                
	FROM bronze.erp_loc_a101
	SET @end_time = GETDATE();
	PRINT '>> Time taken to load silver.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------------------------------------------------';
	--==============================================================================================================--
	SET @start_time = GETDATE();
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Truncating Data From: silver.erp_px_cat_g1v2';
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (
		id, 
		cat,
		subcat, 
		maintenance
	)
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
	PRINT '>> Time taken to load silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------------------------------------------------';
	--==============================================================================================================--
	SET @batch_end_time = GETDATE();
	PRINT '======================================================================================================================';
	PRINT 'Total Time taken to load Silver Layer: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '======================================================================================================================';
	END TRY
	BEGIN CATCH
		PRINT '============================================================='
		PRINT 'Error occurred while loading data into silver layer:'
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message' +CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================================='
	END CATCH
END
	
EXEC silver.load_silver;
