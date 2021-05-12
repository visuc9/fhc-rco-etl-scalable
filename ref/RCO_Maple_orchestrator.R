library(lubridate)
library(tidyr)
library(dplyr)
library(RODBC)

#change datetime field for Maple settings
temp <- StartTime_Analysis
StartTime_Analysis_Maple <- paste0(substr(temp,1,5),"-",substr(temp,6,7),"-",substr(temp,8,9)," ",substr(temp,11,20))
temp <- EndTime_Analysis
EndTime_Analysis_Maple <- paste0(substr(temp,1,5),"-",substr(temp,6,7),"-",substr(temp,8,9)," ",substr(temp,11,20))

#set up Maple SQL connection string via Windows authentication (only exception in StLouis uses local SQL account)
if(Server_Name=="StLouis Maple"){
  connStr_Maple <- paste(
    paste0("Server=",Server_Address),
    paste0("Database=",Database),
    "uid=",
    "pwd=",
    "Driver={ODBC Driver 13 for SQL Server}",
    sep=";"
  )
} else {
  connStr_Maple <- paste(
    paste0("Server=",Server_Address),
    paste0("Database=",Database),
    "Trusted_Connection=yes",
    "Driver={ODBC Driver 13 for SQL Server}",
    sep=";"
  )
}
#connect to Maple SQL
conn <- odbcDriverConnect(connStr_Maple)


#LINE DOWNTIME extract
Query1 <- paste0("
                 SELECT DOWNTIME_PK, NEXT_DOWNTIME_FK, START_TIME, UPTIME, DOWNTIME, BRANDCODE, LINE, LINE_STATE, IS_EXCLUDED, PROD_LOG_IS_EXCLUDED, LINE_SUBSTATE, MACHINE, CAUSE_LEVELS_1_NAME, CAUSE_LEVELS_2_NAME, CAUSE_LEVELS_3_NAME, CAUSE_LEVELS_4_NAME, TEAM, SHIFT, OPERATOR_COMMENT, PRODUCTION_LOG, CONTINUATION_OF_DOWNTIME_FK, PLC_CODE
                 FROM LINE_DOWNTIME
                 WHERE START_TIME > ",StartTime_Analysis_Maple,"
                 AND START_TIME < ",EndTime_Analysis_Maple,";
                 ")
fulldata <- sqlQuery(conn, Query1)
fulldata$START_TIME <- ymd_hms(as.character(fulldata$START_TIME))
fulldata$BRANDCODE <- as.character(fulldata$BRANDCODE)
fulldata$LINE_STATE <- as.character(fulldata$LINE_STATE)
fulldata$CAUSE_LEVELS_1_NAME <- as.character(fulldata$CAUSE_LEVELS_1_NAME)
fulldata$CAUSE_LEVELS_2_NAME <- as.character(fulldata$CAUSE_LEVELS_2_NAME)
fulldata$LINE_SUBSTATE <- as.character(fulldata$LINE_SUBSTATE)
fulldata$LINE <- as.character(fulldata$LINE)
fulldata$MACHINE <- as.character(fulldata$MACHINE)
fulldata$EndTime <- fulldata$START_TIME + fulldata$DOWNTIME * 60
fulldata <- fulldata[!is.na(fulldata$START_TIME),] # [NIK]: We can add AND START TIME IS NOT NULL to the query - [ONUR]: yes, i just only introduced it after as this is super uncommon exception handling for problems is Maple database.

#determine if the special logic for Multi-Line Constraint should be run (defined by whether the paramaeter is established AND as "yes"), and if yes, eliminate the multiple entries in downtime log if the line has multi-constraint setup.
if(exists("Run_Multi_Constraint_Data_Line_Script")){
  if(Run_Multi_Constraint_Data_Line_Script=="yes"){
    fulldata$temp <- paste0(fulldata$START_TIME,fulldata$LINE)
    fulldata <- fulldata[!duplicated(fulldata$temp),]
    fulldata <- fulldata[,-which(colnames(fulldata)=="temp")]
  }
} else {
  Run_Multi_Constraint_Data_Line_Script <- "no"
}



#generate version of LINE_DOWNTIME to be used in Current/Next Brandcode definition & Gantt data generation & first stop after CO analysis. (It does not exclude idle events, and adds indications whether Idle or Planned Downtime.)
fulldata$Planned_Stop_Check <- ifelse(grepl("Planned Downtime",fulldata$LINE_STATE),1,0)
fulldata$Idle_Check <- ifelse(fulldata$IS_EXCLUDED==1 | fulldata$PROD_LOG_IS_EXCLUDED==1 | fulldata$LINE_STATE=="Idle" | fulldata$CAUSE_LEVELS_1_NAME=="Idle",1,0)
#LINE_DOWNTIME_for_Uptime_after_CO_Analysis <- fulldata
#LINE_DOWNTIME_for_Gantt_charting <- fulldata
LINE_DOWNTIME_full <- fulldata


#generate version of LINE_DOWNTIME to be used in filtering CO events, (It already filters out any events excluded from production.)
fulldata <- fulldata[fulldata$IS_EXCLUDED==0 & fulldata$PROD_LOG_IS_EXCLUDED==0 & fulldata$LINE_STATE!="Idle" & fulldata$CAUSE_LEVELS_1_NAME!="Idle",]
fulldata <- fulldata[!is.na(fulldata$START_TIME),]
LINE_DOWNTIME <- fulldata


#determine number of constraints by looking at unique Machine count per line (if Multi-Constraint line logic is active)
if(Run_Multi_Constraint_Data_Line_Script=="yes"){
  Number_of_Constraints_data <- LINE_DOWNTIME %>% group_by(LINE,MACHINE) %>% summarize(UPTIME=sum(UPTIME))
  Number_of_Constraints_data <- Number_of_Constraints_data %>% group_by(LINE) %>% summarize(Number_of_Constraints = n())
}


#check whether to extract Machine Downtime data. It is determined based on whether any Machine Downtime-related data modeling is needed. Gantt chart data generation and SUD's Machine Stops After CO generation sub-scripts use Machine Level data.
trigger_for_Machine_Downtime_extraction <- FALSE
if(exists("Run_Machine_Level_analysis")){
  if(Run_Machine_Level_analysis=="yes"){
    trigger_for_Machine_Downtime_extraction <- TRUE
  }
}
if(exists("SUD_Run_Machine_Stops_after_CO_analysis")){
  if(SUD_Run_Machine_Stops_after_CO_analysis=="yes"){
    trigger_for_Machine_Downtime_extraction <- TRUE
  }
}
if(trigger_for_Machine_Downtime_extraction){
  #MACHINE DOWNTIME extract
  Query1 <- paste0("
                 SELECT DOWNTIME_PK, NEXT_DOWNTIME_FK, START_TIME, UPTIME, DOWNTIME, BRANDCODE, LINE, LINE_STATE, IS_EXCLUDED, IS_CONSTRAINT, PROD_LOG_IS_EXCLUDED, LINE_SUBSTATE, MACHINE, CAUSE_LEVELS_1_NAME, CAUSE_LEVELS_2_NAME, CAUSE_LEVELS_3_NAME, CAUSE_LEVELS_4_NAME, TEAM, SHIFT, OPERATOR_COMMENT, PRODUCTION_LOG, CONTINUATION_OF_DOWNTIME_FK, PLC_CODE, IS_STOP
                 FROM MACHINE_DOWNTIME
                 WHERE START_TIME > ",StartTime_Analysis_Maple,"
                 AND START_TIME < ",EndTime_Analysis_Maple,";
                 ")
  fulldata <- sqlQuery(conn, Query1)
  fulldata$START_TIME <- ymd_hms(as.character(fulldata$START_TIME))
  fulldata$BRANDCODE <- as.character(fulldata$BRANDCODE)
  fulldata$EndTime <- fulldata$START_TIME + fulldata$DOWNTIME * 60
  
  #generate version used in SUD's machine stops after CO analysis. Includes both constraint and non-constraint machines, excluding Idle events.
  MACHINE_DOWNTIME_full <- fulldata[fulldata$IS_EXCLUDED==0,]
  
  #generate version used in machine-level Gantt data generation. Includes only non-constraint machines if the line not multi-constraint. Excludes Idle events.
  if(Run_Multi_Constraint_Data_Line_Script=="no"){
    fulldata <- fulldata[fulldata$IS_CONSTRAINT==0,]
  }
  MACHINE_DOWNTIME <- fulldata
}




#PRODUCTION LOG extract
temp <- ymd_hms(StartTime_Analysis_Maple)
temp <- date(temp) - 1
temp <- paste0("'", temp, " 06:00:00'")
StartTime_Analysis_Maple <- temp
Query1 <- paste0("
                 SELECT PRODUCTION_LOG, NEXT_PRODUCTION_LOG_FK, START_TIME, BRANDCODE, IS_EXCLUDED, DURATION, LINE_STATE, LINE
                 FROM PRODUCTION_LOG
                 WHERE START_TIME > ",StartTime_Analysis_Maple,"
                 AND START_TIME < ",EndTime_Analysis_Maple,";
                 ")
fulldata <- sqlQuery(conn, Query1)
fulldata$START_TIME <- ymd_hms(as.character(fulldata$START_TIME))
fulldata$END_TIME <- fulldata$START_TIME + fulldata$DURATION*60

PRODUCTION_LOG <- fulldata
PRODUCTION_LOG$BRANDCODE <- as.character(PRODUCTION_LOG$BRANDCODE)
PRODUCTION_LOG$LINE <- as.character(PRODUCTION_LOG$LINE)



#RUNTIME PER DAY data generation
temp <- ymd_hms(StartTime_Analysis_Maple)
temp <- date(temp)-1
temp <- paste0("'", temp, " 06:00:00'")
StartTime_Analysis_Maple <- temp

Query1 <- paste0("
                 SELECT LINE, PRODUCTION_DAY, SCHEDULED_TIME_REPORTED
                 FROM SHIFT_SUMMARY
                 WHERE START_TIME > ",StartTime_Analysis_Maple,"
                 AND START_TIME < ",EndTime_Analysis_Maple,";
                 ")
fulldata <- sqlQuery(conn, Query1)
fulldata$LINE <- as.character(fulldata$LINE)
fulldata$SCHEDULED_TIME_REPORTED <- as.numeric(as.character(fulldata$SCHEDULED_TIME_REPORTED))
fulldata <- fulldata %>% group_by(LINE, PRODUCTION_DAY) %>% summarize(Runtime = sum(SCHEDULED_TIME_REPORTED))
# [NIK]: Can change the query to be - [ONUR]: Yes! I think i initially built it as when troubleshooting (when for a given day I get scheduled time higher than calendar time), seeing shift level data was giving me that better visibility.
# SELECT LINE
#      , PRODUCTION_DAY
#      , SUM(SCHEDULED_TIME_REPORTED) AS Runtime
# FROM SHIFT_SUMMARY
# WHERE WHERE START_TIME > ",StartTime_Analysis_Maple,"
# AND START_TIME < ",EndTime_Analysis_Maple,"
# GROUP BY LINE
#     , PRODUCTION_DAY

names(fulldata)[which(colnames(fulldata)=="PRODUCTION_DAY")] <- "Date"
Runtime_per_Day_data <- as.data.frame(fulldata)
Runtime_per_Day_data$Date <- as.Date(as.character(Runtime_per_Day_data$Date))
Runtime_per_Day_data$LINE <- as.character(Runtime_per_Day_data$LINE)

#eliminate first day due to risk of not having extracted full day's data
temp <- min(Runtime_per_Day_data$Date)
Runtime_per_Day_data <- Runtime_per_Day_data[Runtime_per_Day_data$Date>temp,]

#if the scheduled time is incorrectly reported as higher than calendar time (only ok if the line is multi-constraint), then correct it to max 1440 minutes.
if(Run_Multi_Constraint_Data_Line_Script=="no"){
  Runtime_per_Day_data$Runtime <- ifelse(Runtime_per_Day_data$Runtime>1440,1440,Runtime_per_Day_data$Runtime)
}



#get Day startTime per Line - this is needed to automatically detect per line, at what hour the production day starts. this info is later used in PowerBI when grouping COs in production days.
temp <- ymd_hms(StartTime_Analysis_Maple)
temp <- date(temp)
temp <- paste0("'", temp, " 06:00:00'")
StartTime_Analysis_Maple <- temp
Query1 <- paste0("
                 SELECT LINE, PRODUCTION_DAY, START_TIME
                 FROM SHIFT_SUMMARY
                 WHERE START_TIME > ",StartTime_Analysis_Maple,"
                 AND START_TIME < ",EndTime_Analysis_Maple,";
                 ")
fulldata <- sqlQuery(conn, Query1)

fulldata$LINE <- as.character(fulldata$LINE)
fulldata$Shift_Start_hours <- as.numeric(difftime(fulldata$START_TIME,fulldata$PRODUCTION_DAY,units="hours"))
fulldata <- fulldata %>% group_by(LINE,Shift_Start_hours) %>% summarize(tally=n())
temp <- fulldata %>% group_by(LINE) %>% summarize(max_tally=max(tally))
fulldata <- merge(x=fulldata,y=temp,by="LINE")
fulldata <- fulldata[fulldata$tally>fulldata$max_tally/2,]
Day_StartTime_per_Line <- fulldata %>% group_by(LINE) %>% summarize(Day_Start_hours=min(Shift_Start_hours))



#BRANDCODE extract
Query1 <- paste0("
                 SELECT BRANDCODE, BRANDNAME, SIZE, PRODUCTION_UNIT_OF_MEASURE, STAT_CASE_CONVERSION, COUNTRY, UNITS_PER_CASE, NUM_SIZE, BRANDGROUP, FORMULA, BRANDFAMILY, DEPARTMENT_BC
                 FROM BRANDCODE;
                 ")
BRANDCODE_data <- sqlQuery(conn, Query1)
BRANDCODE_data$Server <- Server_Name



#RUN RCO ETL
script_name <- paste0(Root_folder_Master_Scripts,"/RCO_MES_ETL.R")
source(script_name)


#add missing columns from Maple (that is present in Proficy iODS) if at least one CO is available in data.
if(No_CO_Flag==0){
  CO_Event_Log$Reason1Category <- ""
  CO_Event_Log$Reason2Category <- ""
  CO_Event_Log$Reason3Category <- ""
  CO_Event_Log$Reason4Category <- ""
}
