Master.bat file calls site bat files. 

Site .bat files call site R scripts. 

Site R Script:

	1. Checks whether or not it's on the production server (due to OneDrive), points to different master script files depending on test or dev environment.  - Not Needed due to Git and Sharepoint
	2. Sets Site Variables (hard coded): (John – now in Sharepoint)
		a. server type
		b. server address
		c. server name
		d. CO trigger parameter
		e. Run Machine Level Analyisis (yes/no)
		f. Run First Stop After Co Analysis (yes/ no)
		g. Run multi-constraint analysis (yes/no)
	3. Creates Dataframe object with:  (John – Now in Sharepoint)
		a. System (hard coded)
		b. Line  Name (hard coded)
		c. MES_Constraint Machine String (hard coded)
	4. Calls the Overall Orchestrator in context of site, so all Site Variables carry into execution of Overall Orchestrator… (John – Overall Orchestrator is now the Primary script. #2 and 3 are now code in the main body of the program to retrieve data from sharepoint and destination database, then pass parameters into overall orchestrator to then pass down into the more specific functions) 


Overall Orchestrator 
<Function> Replace Datatypes in master data-frames
	What are Master DataFrames?0
<Function> Reduce decimals in each column of dataframe
<Function> Append Data to existing SQL Tables

	1. Check if site is using non-latin alphabet (Special Library called DBI in r), hard coded
	2. Connect to the SQL datamodel
	3. Set up SQL Queries - how long to look back, start time, end time
	4. Detect whether it's a MAPLE or Proficy server and run the appropriate orchestrator (based on Site Variables)
		a. MAPLE Orchestrator (John – Skipping this section as MDC data follows proficy data model)
			i. Connect to MAPLE SQL server using Site Variables 
			ii. Pull specific columns from LINE_DOWNTIME -> fulldata
			iii. Create Planned Stop Column
			iv. Create Idle Check Column
			v. Create Line_Downtime_Full dataframe from fulldata
			vi. Filter out IdleCheck = true
			vii. Create Line Downtime from fulldata
			viii. Pull Machine Data if machine data extraction Site Variable is true
			ix. Pull the Production Log -> fulldata
			x. Aggregate sum of runtime per production day by LINE -> runtime-per-day-data (dataframe)
			xi. Detect what time production day starts and push to Day_StartTime_Per_Line Dataframe
			xii. Extract Brandcode Data into BRANDCODE_Data dataframe
			xiii. Run the RCO MES ETL
		b. Proficy Orchestrator – Green here means implemented already in python code. 
			i. Pull GetLines API and create a table of lines
			ii. Check if it's an SUD specific case (Site Variables)
			iii. Get DowntimeUptime Raw Data 
			iv. Change all of the column names to match with MAPLE
			v. Create Is_Excluded = true if not containing "PR In"
			vi. Create Machine Downtime Raw data
			vii. Filter only Constraint Stops
			viii. Do Multi Constraint Logic if applicable
			ix. Filter out isStarved, isBlocked
			x. Create PlannedStopCheck column, 
			xi. Create Line_Downtime_Full with all events
			xii. Create Line_Downtime without Excluded Events
			xiii. Check whether to extract machine level data
			xiv. Check SUD machine Stops after CO analysis, run machine downtime extraction if needed
			xv. Extract Production Log
			xvi. Create Production log tables, make sure Line and Brandcode tables are strings
			xvii. Pull Scheduled Time Per Day KPI 
			xviii. Get Day Start Time per line from KPI
			xix. Generate BRANDCODE table from Production Log- summarize by Brandcode, prodDesc, ProdFam, ProdGroup, FirstPackCount
			xx. Add [SIZE] Column to brandcode *HARDCODED BY SITE*
			xxi. Run RCO MES ETL
				1) RCO MES ETL (still in context of site, overall orchestrator, specific orchestrator)
					a) Check if SUD Specific RCO Script
					b) Filter out CO Events (Hardcoded BY SERVER W/IN SITE), toss all data found this way into CO_Event_Log table <- (John – Left off here, but double check the previous work against the R scripts.)
					c) If no changeovers were found (no rows in CO_Event_Log), set No_Co_Flag =1  else 0 and skip the rest of this script
					d) CO Trigger column - paste together Cause Level 1,2,3 into single string
					e) Order all CO Events by Line by StartTime, get rid of any without start time
					f) Add column with seconds of downtime
					g) Add previous rows' relevant data to current row, and calculate minutes difference between end-time of previous CO event and start of current.
					h) Add column CO_Trigger with logic to determine if next event in list is part of one changeover or is considered a new changeover
					i) Optionally, if Cause Model  becomes different, split by Cause Model.
					j) Lima SUD Hard-Code - don't split CO events is cause model includes Changeover Failure and diff less than 2 hours
					k) Add CO_Identifier column and fill CO Identifier for all that are part of same CO
					l) Summarize CO into CO Aggregated data by CO Identifier
					m) Fix datatypes, add 'Downtime PK' to each event
					n) Determine current and next brandcode per Changeover
					o) Indicate whether brandcode was changed during changeover
					p) Clean up columns
					q) Replace problem characters for SQL and CSV writing from Operator Comments
					r) Run SubETL Gantt Data Generator Script if needed for site/server
					s) Run SubETL First Stop After Changeover Script if needed for site/server
					t) Run SUD Specific RCO Scripts if needed for site/server
					u) BACK TO Overall Orchestrator
	5. If Scripts resulted in Changeovers (No_CO_Flag =0), clean up Operator Comments some more in preparation for reading/writing to SQL
	6. Round Runtime_Per_Day data to the nearest 1
	7. Append data to database per Line in Line Data table
		a. Check if Line is already in the database, else add it to Script_Data
		b. Append CO data only if there's data to append
		c. Delete from database anything which happened after StartTime of new data
		d. Do same for machine level analysis, first stop after CO analysis, etc. 
	8. Append new data to Runtime-Per-Day
	9. Update Script Data
	10. Save over Brandcode Data 


REPEAT FOR EACH SITE AND SERVER