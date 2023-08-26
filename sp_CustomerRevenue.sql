USE [WideWorldImportersDW-Standard]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_CustomerRevenue]
	  @FromYear		INT = -1,
	  @ToYear		INT = -1,
	  @Period		NVARCHAR(7) = 'Y',
	  @CustomerID	INT	= -1
AS BEGIN
	/************************************************************************************************************************************************************
	sp_CustomerRevenue
	-> This Stored Procedure is used to publish EBP prices from "EBP ZLST A800" PM worksheet when overnight publish has failed

	EXEC	[WideWorldImportersDW-Standard].[dbo].[sp_CustomerRevenue] @FromYear = 2015, @ToYear = 2016, @Period = 'Q', @CustomerID = 99
	SELECT	TOP 500 * FROM [WideWorldImportersDW-Standard].[dbo].[ErrorLog] ORDER BY CreatedAt DESC

	Modifications
	User				Date		Comments
	Luis Gutierrez		08/25/2023	Creating this Stored Procedure to solve Use Case #32 Generative-AI-Initiative
	**********************************************************************************************************************************************************/
	SET NOCOUNT ON
	BEGIN TRY

		/* SETTING VARIABLES */

		--	If input parameter is not passed, the earliest available year in the data set should be used by default
		IF	@FromYear = -1
			BEGIN
				SELECT	@FromYear = MIN(DATEPART(YEAR, [Invoice Date Key]))
				FROM	[Fact].[Sale]
			END

		--	 If input parameter is not passed, the latest available year in the data set should be used by default
		IF	@ToYear = -1
			BEGIN
				SELECT	@ToYear = MAX(DATEPART(YEAR, [Invoice Date Key]))
				FROM	[Fact].[Sale]
			END

		--	Abort the execution in case @Period is not an acceptable value
		IF	@Period	NOT IN ('Month', 'M', 'Quarter', 'Q', 'Year', 'Y')
			BEGIN
				RAISERROR('Period is not a valid option', 18, 1)
			END

		--	Helper variables
		DECLARE	@TableName NVARCHAR(100)
		DECLARE @CustomerName NVARCHAR(100)
		DECLARE @CustomerFilter NVARCHAR(100)

		--	If stored procedure was executed for all customers, table name should contain prefix ‘All’, start year, end year and period identifier
		--	IFF function is used to determine if the query will use only one year
		IF	@CustomerID <> -1
			BEGIN
				
				SELECT	TOP 1 
						@CustomerName = Customer
				FROM	[Dimension].[Customer]	
				WHERE	[WWI Customer ID] = @CustomerID

				SET	@TableName =	CAST(@CustomerID AS NVARCHAR(10)) + '_' + @CustomerName + '_' + IIF(@ToYear = @FromYear, CAST(@ToYear AS NVARCHAR(4)), CAST(@FromYear AS NVARCHAR(4))
									+ '_' + CAST(@ToYear AS NVARCHAR(4))) + '_' + UPPER(LEFT(@Period, 1))

				SET	@CustomerFilter = 'AND	c.[WWI Customer ID]	= '+CAST(@CustomerID AS nvarchar(10))

			END
		ELSE
			BEGIN
				SET	@TableName =	'All_' + IIF(@ToYear = @FromYear, CAST(@ToYear AS NVARCHAR(4)), CAST(@FromYear AS NVARCHAR(4))
									+ '_' + CAST(@ToYear AS NVARCHAR(4))) + '_' + UPPER(LEFT(@Period, 1))

				SET	@CustomerFilter = ' '
			END

		/* CREATING TABLES */

		--	Creating output table each time the stored procedure runs
		DECLARE	@DM1_SQL		NVARCHAR(MAX)
		SET		@DM1_SQL = '
		IF OBJECT_ID(''[dbo].['+@TableName+']'', ''U'') IS NOT NULL BEGIN	DROP TABLE [dbo].['+@TableName+']
		END CREATE TABLE [dbo].['+@TableName+'] ([CustomerID] int, [CustomerName] nvarchar(50),
		[Period] nvarchar(8), [Revenue] numeric(19,2))'
		EXEC(@DM1_SQL)

		--	Creating ErrorLog table only the first time the stored procedure runs
		IF	OBJECT_ID('[dbo].[ErrorLog]', 'U') IS NULL 
			BEGIN
				CREATE TABLE	[dbo].[ErrorLog] 
				([ErrorID] int IDENTITY(1,1), 
				[ErrorNumber] int, 
				[ErrorSeverity] int, 
				[ErrorMessage] varchar(255),
				[CustomerID] int, 
				[Period] varchar(8), 
				[CreatedAt] datetime)
			END
		
		--/* INSERTING OUTPUT DATASET */

		----	Defining variable that is going to be used to aggregate the data
		DECLARE	@PeriodString NVARCHAR(160)
		IF	UPPER(LEFT(@Period, 1)) = 'Q'
			BEGIN
				SELECT	@PeriodString	= '''Q'' + CAST(DATENAME(Q, s.[Invoice Date Key]) AS nvarchar(1)) + '' '' + CAST(DATEPART(YEAR, s.[Invoice Date Key]) AS nvarchar(4))'
			END
		IF	UPPER(LEFT(@Period, 1)) = 'M'
			BEGIN
				SELECT	@PeriodString	= 'CAST(DATENAME(M, s.[Invoice Date Key]) AS NVARCHAR(3))+'' ''+CAST(DATEPART(YEAR, s.[Invoice Date Key]) AS NVARCHAR(4))'
			END
		IF	UPPER(LEFT(@Period, 1)) = 'Y'
			BEGIN
				SELECT	@PeriodString	= 'DATEPART(YEAR, s.[Invoice Date Key])'
			END

		--	Inserting aggregated data into output table
		--	If there are no results, it inserts one record where revenue is set to 0
		DECLARE	@DM2_SQL		NVARCHAR(MAX)
		SET		@DM2_SQL = '
		INSERT INTO [dbo].['+@TableName+'] ([CustomerID], [CustomerName], [Period], [Revenue])
		SELECT	[CustomerID]	= c.[WWI Customer ID]
				,[CustomerName]	= c.Customer
				,[Period]		= '+@PeriodString+'
				,[Revenue]		= SUM(s.Quantity*s.[Unit Price])
		FROM	[Fact].[Sale]							s
		JOIN	[Dimension].[Customer]					c
			ON	s.[Customer Key]	= c.[Customer Key]
		WHERE	DATEPART(YEAR, s.[Invoice Date Key]) >= '+CAST(@FromYear AS nvarchar(4))+'
			AND	DATEPART(YEAR, s.[Invoice Date Key]) <= '+CAST(@ToYear AS nvarchar(4))+' '+@CustomerFilter+'
		GROUP BY	c.[WWI Customer ID], c.Customer, +'+@PeriodString+'
		
		IF	@@ROWCOUNT = 0 INSERT INTO [dbo].['+@TableName+'] ([CustomerID], [CustomerName], [Period], [Revenue])
		SELECT	'+CAST(@CustomerID AS nvarchar(10))+','''+@CustomerName+''','''+@Period+''',0
		'
		EXEC(@DM2_SQL)
		
	END TRY

	BEGIN CATCH
		
		--	Inserting error data in case the stored procedure fails
		INSERT INTO [dbo].ErrorLog
			([ErrorNumber], 
			[ErrorSeverity], 
			[ErrorMessage],
			[CustomerID], 
			[Period], 
			[CreatedAt])
		SELECT
			ERROR_NUMBER(),
			ERROR_SEVERITY(),
			ERROR_MESSAGE(),
			@CustomerID,
			@Period,
			GETDATE()

		--	Printing variable to debug
		PRINT(@CustomerFilter)
		PRINT(@TableName)
		PRINT(@DM1_SQL)
		PRINT(@PeriodString)
		PRINT(@DM2_SQL)
		
	END CATCH

	SET NOCOUNT OFF

END
GO