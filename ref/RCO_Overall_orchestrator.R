Lines <- Line_Input_Data$MES_Line_Name #create a string of lines from site-level input data.
Minutes_to_take_Machine_data_after_CO <- 15 #after CO is complete, number of minutes to generate data for the Machine-level Gantt-chart visuals. (Note: For data before CO, it is set by default to 5mins.)

#if the site is entering data in non-Latin alphabet, their SQL data writing is done via different library.
if(Server_Name=="Novo" | Server_Name=="Chengdu"){
  Write_to_SQL_via_DBI <- "yes"
} else {
  Write_to_SQL_via_DBI <- "no"
}
library(lubridate)
library(tidyr)
library(dplyr)
library(RODBC)


#define SQL table names to be used in Transformed Data Storage
SQL_database_topic <- "RCO_v1"

SQL_tablename_CO_Aggregated_Data <- paste0(SQL_database_topic,"_CO_Aggregated_data")
SQL_tablename_CO_Event_Log <- paste0(SQL_database_topic,"_CO_Event_log")
SQL_tablename_Script_Data <- paste0(SQL_database_topic,"_Script_data")
SQL_tablename_Runtime_per_Day_data <- paste0(SQL_database_topic,"_Runtime_per_Day_data")
SQL_tablename_BRANDCODE_data <- paste0(SQL_database_topic,"_BRANDCODE_data")
SQL_tablename_Gantt_data <- paste0(SQL_database_topic,"_Gantt_data")
SQL_tablename_Event_Log_for_Gantt <- paste0(SQL_database_topic,"_Event_Log_for_Gantt")
SQL_tablename_First_Stop_after_CO_Data <- paste0(SQL_database_topic,"_First_Stop_after_CO_Data")


#create  Transformed Data Storage connection
Server_Address_Intermediate_Storage <- "azpg-sqlserver-fhcenganalyticsdatalab.database.windows.net"
Database_Intermediate_Storage <- "DatalabDB"

connStr <- paste(
  paste0("Server=",Server_Address_Intermediate_Storage),
  paste0("Database=",Database_Intermediate_Storage),
  "uid=",
  "pwd=",
  "Driver={ODBC Driver 17 for SQL Server}",
  sep=";"
)
conn_Intermediate_Storage <- odbcDriverConnect(connStr)

if(Write_to_SQL_via_DBI=="yes"){
  library(DBI)
  library(odbc)
  library(purrr)
  convertToUTF16 <- function(s){
    map(s, function(x) iconv(x,from="UTF-8",to="UTF-16LE",toRaw=TRUE) %>% unlist)
  }
  connectionString <- paste0("DRIVER={ODBC Driver 17 for SQL Server}; SERVER=",Server_Address_Intermediate_Storage,"; Database=",Database_Intermediate_Storage,"; uid=; pwd=")
  conn_DBI <- DBI::dbConnect(odbc::odbc(),
                             .connection_string = connectionString)
}


#Define "Number_of_days_to_look_back" automatically, based on when the refresh is run. Current logic is that if the system hour is at 3am then re-take 7 days of data since last refresh. This is done refresh the data for each site for past 7 days one time per day.
#Otherwise it only updates last 3 days.
Current_System_Time <- Sys.time()
Current_hour <- hour(Current_System_Time)
Number_of_days_to_look_back <- if(Current_hour==3){7} else {3}
print(paste0("Number of days data extracted: ",Number_of_days_to_look_back))


#definition of Start and EndTime of the queries - by default, the StartTime is a number of days before last time the script was run successfully, and EndTime is one day after the current system time (to accomodate for Asian sites, whose local time is later than the system time.)
#the format of the two variables are like following:
#StartTime_Analysis <- "'20200501 06:00:00'"
#EndTime_Analysis <- "'20200507 06:00:00'"
Query1 <- paste0("SELECT max(Data_Update_Time)
                   FROM ",SQL_tablename_Script_Data,"
                   WHERE Server='",Server_Name,"'")
k <- sqlQuery(conn_Intermediate_Storage,Query1)[1,1]
k <- k - Number_of_days_to_look_back*60*60*24
StartTime_Analysis <- paste0("'",year(k),if(month(k)<10){"0"},month(k),if(day(k)<10){"0"},day(k)," ",if(hour(k)<10){"0"},hour(k),":",if(minute(k)<10){"0"},minute(k),":",if(second(k)<10){"0"},floor(second(k)),"'")
k <- Current_System_Time
k <- k + 1*24*60*60
EndTime_Analysis <- paste0("'",year(k),if(month(k)<10){"0"},month(k),if(day(k)<10){"0"},day(k)," ",if(hour(k)<10){"0"},hour(k),":",if(minute(k)<10){"0"},minute(k),":",if(second(k)<10){"0"},floor(second(k)),"'")


#function to replace data types in master data frames
data_type_replace <- function(Data_to_be_replaced,Data_to_be_used){
  availability <- ifelse(nrow(Data_to_be_used)>0,"available","not_available")
  if (availability=="available"){
    temp <- as.data.frame(Data_to_be_replaced[,c(which(colnames(Data_to_be_replaced)==names(Data_to_be_used)[1]))])
    names(temp)[1] <- names(Data_to_be_used)[1]
    for (i in 2:(ncol(Data_to_be_used))){
      temp <- cbind(temp,Data_to_be_replaced[,c(which(colnames(Data_to_be_replaced)==names(Data_to_be_used)[i]))])
      names(temp)[i] <- names(Data_to_be_used)[i]
    }
    Data_to_be_replaced <- temp
    temp <- lapply(Data_to_be_used, class)
    data_types <- data.frame()
    for (i in 1:length(temp)){
      data_types[i,1] <- as.character(temp[[i]][1])
    }
    temp <- lapply(Data_to_be_replaced, class)
    data_types_master <- data.frame()
    for (i in 1:length(temp)){
      data_types_master[i,1] <- as.character(temp[[i]][1])
    }
    for (i in 1:ncol(Data_to_be_replaced)){
      if (data_types[i,1] != data_types_master[i,1]){
        if (data_types[i,1]=="POSIXct"){
          Data_to_be_replaced[,i] <- ymd_hms(as.character(Data_to_be_replaced[,i]))
        } else if (data_types[i,1]=="character") {
          Data_to_be_replaced[,i] <- as.character(Data_to_be_replaced[,i])
        } else if (data_types[i,1]=="numeric") {
          Data_to_be_replaced[,i] <- as.numeric(as.character(Data_to_be_replaced[,i]))
        } else if (data_types[i,1]=="Date") {
          Data_to_be_replaced[,i] <- date(as.character(Data_to_be_replaced[,i]))
        } 
      }
    }
  }
  temp_list <- list(Data_to_be_replaced,Data_to_be_used)
  return(temp_list)
}

#function to reduce the decimals, by automatically detecting number of decimals based on the numbers populated for each column.
dataframe_decimal_reducer <- function(input_df){
  df <- input_df
  for (j in 1:ncol(df)){
    col_type <- class(df[,j])
    if (col_type=="numeric"){
      col_mean <- mean(abs(df[!(is.na(df[,j])) & !(is.infinite(df[,j])),j]))
      if(is.nan(col_mean)){col_mean<-0}
      col_max_decimals <- ifelse(col_mean<0.1,5,ifelse(col_mean<1,4,ifelse(col_mean<10,3,ifelse(col_mean<100,2,1))))
      df[,j] <- round(df[,j],col_max_decimals)
    }
  }
  return(df)
}

#function to append data to existing SQL tables. It looks at the column data types in SQL and revises the data types and then performs the appending.
SQL_Appender <- function(connection,New_Rows,SQL_Existing_TableName){
  
  Query1 <- paste0("exec sp_columns ",SQL_Existing_TableName)
  columns_in_SQL <- sqlQuery(connection,Query1)
  columns_in_SQL$COLUMN_NAME <- as.character(columns_in_SQL$COLUMN_NAME)
  
  temp <- nrow(New_Rows)
  New_Table <- as.data.frame(matrix(NA,nrow = temp, ncol=1))
  
  temp <- which(colnames(New_Rows)==columns_in_SQL$COLUMN_NAME[1])
  if(length(temp)>0){
    New_Table <- cbind(New_Table,New_Rows[,temp])
    New_Table <- New_Table[,-1]
  } else {
    names(New_Table)[1] <- columns_in_SQL$COLUMN_NAME[1]
  }
  
  for (i in 2:nrow(columns_in_SQL)){
    temp <- which(colnames(New_Rows)==columns_in_SQL$COLUMN_NAME[i])
    if(length(temp)>0){
      temp <- as.data.frame(New_Rows[,temp])
      names(temp)[1] <- columns_in_SQL$COLUMN_NAME[i]
      New_Table <- cbind(New_Table,temp)
    } else {
      New_Table$AAAA <- NA
      names(New_Table)[ncol(New_Table)] <- columns_in_SQL$COLUMN_NAME[i]
    }
  }
  
  sqlSave(connection,New_Table,SQL_Existing_TableName,append=TRUE,rownames=FALSE,fast=FALSE)
  
}


#Get CO Events from MES - triggering this section of the script goes to F&HC generic RCO ETL, which performs (1)data extraction from MES server,
#(2)CO-related ETL like filtering out non-CO events, combining split events belonging to same CO, determining CO downtime & current/next brandcode, creating [CO_Identifier] column used to connect different tables,
#(3) run sub-ETL scripts like getting First Stop after CO or Machine-Level Stop data after CO.
if (Server_Type=="Maple"){
  CO_Script_file <- paste0(Root_folder_Master_Scripts,"/RCO_Maple_orchestrator.R")
} else if (Server_Type=="ProficyiODS"){
  CO_Script_file <- paste0(Root_folder_Master_Scripts,"/RCO_ProficyiODS_orchestrator.R")
}
script_name <- paste0(CO_Script_file)
source(script_name)
#The output of this script gives out the data frames [CO_Aggregated_Data], [CO_Event_Log], [Runtime_per_Day_data], [BRANDCODE_data], [First_Stop_after_CO_Data], [Gantt_Data] and [Event_Log_for_Gantt].
#The next section of this script mainly performs appending this new data to historical data already stored in Transformed Data Storage.


#if there is at least one CO available in the data, perfom few post-treatment steps.
if(No_CO_Flag==0){
  CO_Aggregated_Data_full <- CO_Aggregated_Data
  CO_Event_Log_full <- CO_Event_Log
  
  #substitute character "'" which creates issues when writing/reading data to SQL.
  if(exists("Run_Machine_Level_analysis")){
    if(Run_Machine_Level_analysis=="yes"){
      Gantt_Data_full <- Gantt_Data
      Event_Log_for_Gantt_full <- Event_Log_for_Gantt
      Event_Log_for_Gantt_full$OPERATOR_COMMENT <- gsub("'"," ",Event_Log_for_Gantt_full$OPERATOR_COMMENT)
      Event_Log_for_Gantt_full$CAUSE_LEVELS_3_NAME <- gsub("'"," ",Event_Log_for_Gantt_full$CAUSE_LEVELS_3_NAME)
      Event_Log_for_Gantt_full$CAUSE_LEVELS_4_NAME <- gsub("'"," ",Event_Log_for_Gantt_full$CAUSE_LEVELS_4_NAME)
    }
  }
  if(exists("Run_First_Stop_After_CO_analysis")){
    if(Run_First_Stop_After_CO_analysis=="yes"){
      First_Stop_after_CO_Data_full <- First_Stop_after_CO_Data
      First_Stop_after_CO_Data_full$OPERATOR_COMMENT <- gsub("'"," ",First_Stop_after_CO_Data_full$OPERATOR_COMMENT)
      First_Stop_after_CO_Data_full$CAUSE_LEVELS_3_NAME <- gsub("'"," ",First_Stop_after_CO_Data_full$CAUSE_LEVELS_3_NAME)
      First_Stop_after_CO_Data_full$CAUSE_LEVELS_4_NAME <- gsub("'"," ",First_Stop_after_CO_Data_full$CAUSE_LEVELS_4_NAME)
      First_Stop_after_CO_Data_full <- First_Stop_after_CO_Data_full[!is.na(First_Stop_after_CO_Data_full$START_TIME),]
    }
  }
  if(nrow(CO_Event_Log_full)>0){
    CO_Event_Log_full$OPERATOR_COMMENT <- gsub("'"," ",CO_Event_Log_full$OPERATOR_COMMENT)
    CO_Event_Log_full$CAUSE_LEVELS_3_NAME <- gsub("'"," ",CO_Event_Log_full$CAUSE_LEVELS_3_NAME)
    CO_Event_Log_full$CAUSE_LEVELS_4_NAME <- gsub("'"," ",CO_Event_Log_full$CAUSE_LEVELS_4_NAME)
  }
  if(nrow(BRANDCODE_data)>0){
    BRANDCODE_data$BRANDNAME <- gsub("'"," ",BRANDCODE_data$BRANDNAME)
  }
  
  #add blank column [Total_Uptime_till_Next_CO] if the First Stop after CO sub-ETL is not enabled.
  temp <- which(colnames(CO_Aggregated_Data_full)=="Total_Uptime_till_Next_CO")
  if(length(temp)==0){
    CO_Aggregated_Data_full$Total_Uptime_till_Next_CO <- NA
  }
}

Runtime_per_Day_data$Server <- Server_Name
Runtime_per_Day_data$Runtime <- round(Runtime_per_Day_data$Runtime,1)
Runtime_per_Day_data_full <- Runtime_per_Day_data


#run Transformed Data Storage appending per Line
for (Line_Number in 1:nrow(Line_Input_Data)){
  
  System <- Line_Input_Data$System[Line_Number]
  Line_Name <- Line_Input_Data$MES_Line_Name[Line_Number]
  
  print(paste0("Equipment #: ",Line_Number," (",System,") started"))
  
  
  #check if this line is already available in [Script_Data] and if not, add it.
  Query1 <- paste0("SELECT *
                   FROM ",SQL_tablename_Script_Data,"
                   WHERE System='",System,"'")
  new_row <- sqlQuery(conn_Intermediate_Storage,Query1)
  if(nrow(new_row)==0){
    Query1 <- paste0("SELECT TOP 1 *
                   FROM ",SQL_tablename_Script_Data)
    new_row <- sqlQuery(conn_Intermediate_Storage,Query1)
    new_row$System <- System
    new_row$MES_Line_Name <- Line_Name
    new_row$Server <- Server_Name
    new_row$BU <- "FHC"
    
    new_row$Data_Update_Time[1] <- Current_System_Time
    Query1 <- paste0("SELECT min(CO_StartTime) as Min_time, max(CO_StartTime) as Max_time
                   FROM ",SQL_tablename_CO_Aggregated_Data,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'")
    temp <- sqlQuery(conn_Intermediate_Storage,Query1)
    
    new_row$First_Available_Data_Point[1] <- temp$Min_time[1]
    new_row$Last_Available_Data_Point[1] <- temp$Max_time[1]
    
    Day_Start_hours <- Day_StartTime_per_Line$Day_Start_hours[Day_StartTime_per_Line$LINE==Line_Name]
    if(length(Day_Start_hours)>0){
      new_row$Day_Start_hours[1] <- Day_Start_hours
    } else {
      new_row$Day_Start_hours[1] <- 6
    }
    if(is.na(new_row$Number_of_Constraints[1])){
      new_row$Number_of_Constraints[1] <- 1
    }
    
    if(exists("Run_Multi_Constraint_Data_Line_Script")){
      if(Run_Multi_Constraint_Data_Line_Script=="yes"){
        temp <- Number_of_Constraints_data[Number_of_Constraints_data$LINE==Line_Name,]
        if(nrow(temp)>0){
          if(temp$Number_of_Constraints[1]>new_row$Number_of_Constraints[1]){
            new_row$Number_of_Constraints[1] <- temp$Number_of_Constraints[1]
          }
        }
      }
    }
    
    SQL_Appender(conn_Intermediate_Storage,new_row,SQL_tablename_Script_Data)
  }
  
  
  
  #run appending CO data to Transformed Data Storage only if a CO for the specific Line is available.
  if(No_CO_Flag==0){
    CO_Aggregated_Data <- CO_Aggregated_Data_full[CO_Aggregated_Data_full$LINE==Line_Name,]
    CO_Event_Log <- CO_Event_Log_full[CO_Event_Log_full$LINE==Line_Name,]
    if(exists("Run_Machine_Level_analysis")){
      if(Run_Machine_Level_analysis=="yes"){
        Gantt_Data <- Gantt_Data_full[Gantt_Data_full$Line==Line_Name,]
        Event_Log_for_Gantt <- Event_Log_for_Gantt_full[Event_Log_for_Gantt_full$LINE==Line_Name,]
      }
    }
    if(exists("Run_First_Stop_After_CO_analysis")){
      if(Run_First_Stop_After_CO_analysis=="yes"){
        First_Stop_after_CO_Data <- First_Stop_after_CO_Data_full[First_Stop_after_CO_Data_full$LINE==Line_Name,]
      }
    }
    
    if (nrow(CO_Aggregated_Data)>0){
      
      #remove all COs from [CO_Aggregated_Data] which happened after the StartTime of newly taken data.
      temp2 <- min(CO_Aggregated_Data$CO_StartTime)
      temp3 <- ymd_hms(StartTime_Analysis)
      if(temp3<temp2){temp2<-temp3}
      temp2 <- temp2 - 10 #to accomodate for also removing historical COs whose StartTime moved couple of seconds.
      temp2_backup <- temp2
      Query_1 <- paste0("SELECT COUNT(*) FROM ",SQL_tablename_CO_Aggregated_Data)
      init1 <- sqlQuery(conn_Intermediate_Storage,Query_1)[1,1] #these count variables are generated to compare the number of rows in data before/after to check how many new entries are generated.
      Query1 <- paste0("DELETE
                   FROM ",SQL_tablename_CO_Aggregated_Data,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'
                   AND CO_StartTime >= CONVERT(datetime,'",temp2,"')")
      sqlQuery(conn_Intermediate_Storage,Query1)
      
      #after deleting the historical COs in Transformed Data Storage which are also available in newly extracted data, get the last CO available in Transformed Data Storage.
      Query1 <- paste0("SELECT TOP 1 *
                   FROM ",SQL_tablename_CO_Aggregated_Data,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'
                   ORDER BY CO_StartTime Desc")
      temp <- sqlQuery(conn_Intermediate_Storage,Query1)
      temp <- as.character(temp$CO_Identifier[1])
      
      #get the last [START_TIME] of last event of last CO available in Transformed Data Storage.
      Query1 <- paste0("SELECT max(START_TIME)
                   FROM ",SQL_tablename_CO_Event_Log,"
                   WHERE CO_Identifier='",temp,"'
                   AND Server='",Server_Name,"'")
      temp2 <- sqlQuery(conn_Intermediate_Storage,Query1)[1,1]
      if(is.na(temp2)){ #if that CO is not found, use the [CO_EndTime] of the last CO.
        Query1 <- paste0("SELECT TOP 1 *
                   FROM ",SQL_tablename_CO_Aggregated_Data,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'
                   ORDER BY CO_StartTime Desc")
        temp <- sqlQuery(conn_Intermediate_Storage,Query1)
        temp2 <- temp$CO_EndTime[1]
      }
      if(is.na(temp2)){
        temp2 <- temp2_backup
      }
      
      
      #delete enties from other tables similar to COs deleted from [CO_Aggregated_Data]
      Query_2 <- paste0("SELECT COUNT(*) FROM ",SQL_tablename_CO_Event_Log)
      init2 <- sqlQuery(conn_Intermediate_Storage,Query_2)[1,1]
      Query1 <- paste0("DELETE
                   FROM ",SQL_tablename_CO_Event_Log,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'
                   AND START_TIME > CONVERT(datetime,'",as.character(temp2),"')")
      sqlQuery(conn_Intermediate_Storage,Query1)
      
      if(exists("Run_Machine_Level_analysis")){
        if(Run_Machine_Level_analysis=="yes"){
          Query_5 <- paste0("SELECT COUNT(*) FROM ",SQL_tablename_Gantt_data)
          init5 <- sqlQuery(conn_Intermediate_Storage,Query_5)[1,1]
          temp3 <- temp2 + (Minutes_to_take_Machine_data_after_CO+5) * 60
          Query1 <- paste0("DELETE
                   FROM ",SQL_tablename_Gantt_data,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'
                   AND StartTime > CONVERT(datetime,'",as.character(temp3),"')")
          sqlQuery(conn_Intermediate_Storage,Query1)
          
          Query_6 <- paste0("SELECT COUNT(*) FROM ",SQL_tablename_Event_Log_for_Gantt)
          init6 <- sqlQuery(conn_Intermediate_Storage,Query_6)[1,1]
          Query1 <- paste0("DELETE
                   FROM ",SQL_tablename_Event_Log_for_Gantt,"
                   WHERE LINE='",Line_Name,"'
                   AND Server='",Server_Name,"'
                   AND START_TIME > CONVERT(datetime,'",as.character(temp3),"')")
          sqlQuery(conn_Intermediate_Storage,Query1)
        }
      }
      if(exists("Run_First_Stop_After_CO_analysis")){
        if(Run_First_Stop_After_CO_analysis=="yes"){
          if(nrow(First_Stop_after_CO_Data)>0){
            temp2 <- min(CO_Aggregated_Data$CO_StartTime)
            Query_7 <- paste0("SELECT COUNT(*) FROM ",SQL_tablename_First_Stop_after_CO_Data)
            init7 <- sqlQuery(conn_Intermediate_Storage,Query_7)[1,1]
            Query1 <- paste0("DELETE
                   FROM ",SQL_tablename_First_Stop_after_CO_Data,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'
                   AND START_TIME > CONVERT(datetime,'",as.character(temp2),"')")
            sqlQuery(conn_Intermediate_Storage,Query1)
          }
        }
      }
      
      
      #append new COs to [CO_Aggregated Data].
      SQL_Appender(conn_Intermediate_Storage,CO_Aggregated_Data,SQL_tablename_CO_Aggregated_Data)
      fin1 <- sqlQuery(conn_Intermediate_Storage,Query_1)[1,1]
      
      #append new CO events to [CO_Event_Log] - note that for non-Latin data sources, different library is used, as this table may include non-Latin characters.
      if(Write_to_SQL_via_DBI=="yes"){
        CO_Event_Log <- CO_Event_Log %>% mutate(CAUSE_LEVELS_1_NAME = convertToUTF16(CAUSE_LEVELS_1_NAME))
        CO_Event_Log <- CO_Event_Log %>% mutate(CAUSE_LEVELS_2_NAME = convertToUTF16(CAUSE_LEVELS_2_NAME))
        CO_Event_Log <- CO_Event_Log %>% mutate(CAUSE_LEVELS_3_NAME = convertToUTF16(CAUSE_LEVELS_3_NAME))
        CO_Event_Log <- CO_Event_Log %>% mutate(CAUSE_LEVELS_4_NAME = convertToUTF16(CAUSE_LEVELS_4_NAME))
        CO_Event_Log <- CO_Event_Log %>% mutate(OPERATOR_COMMENT = convertToUTF16(OPERATOR_COMMENT))
        temp <- which(colnames(CO_Event_Log)=="ProdDesc")
        if(length(temp)>0){
          CO_Event_Log <- CO_Event_Log %>% mutate(ProdDesc = convertToUTF16(ProdDesc))
        }
        dbWriteTable(conn_DBI, SQL_tablename_CO_Event_Log, CO_Event_Log, append=TRUE)
      } else {
        SQL_Appender(conn_Intermediate_Storage,CO_Event_Log,SQL_tablename_CO_Event_Log)
      }
      fin2 <- sqlQuery(conn_Intermediate_Storage,Query_2)[1,1]
      
      print(paste0("Delta rows in CO_Aggregated_Data: ",fin1 - init1))
      print(paste0("Delta rows in CO_Event_Log: ",fin2 - init2))
      
      
      #append new machine level data to to [Gantt_Data].
      #append new machin elevel data to to [Event_Log_for_Gantt] - note that for non-Latin data sources, different library is used, as this table may include non-Latin characters.
      if(exists("Run_Machine_Level_analysis")){
        if(Run_Machine_Level_analysis=="yes"){
          SQL_Appender(conn_Intermediate_Storage,Gantt_Data,SQL_tablename_Gantt_data)
          fin5 <- sqlQuery(conn_Intermediate_Storage,Query_5)[1,1]
          print(paste0("Delta rows in Gantt_Data: ",fin5 - init5))
          
          if(Write_to_SQL_via_DBI=="yes"){
            Event_Log_for_Gantt <- Event_Log_for_Gantt %>% mutate(CAUSE_LEVELS_1_NAME = convertToUTF16(CAUSE_LEVELS_1_NAME))
            Event_Log_for_Gantt <- Event_Log_for_Gantt %>% mutate(CAUSE_LEVELS_2_NAME = convertToUTF16(CAUSE_LEVELS_2_NAME))
            Event_Log_for_Gantt <- Event_Log_for_Gantt %>% mutate(CAUSE_LEVELS_3_NAME = convertToUTF16(CAUSE_LEVELS_3_NAME))
            Event_Log_for_Gantt <- Event_Log_for_Gantt %>% mutate(CAUSE_LEVELS_4_NAME = convertToUTF16(CAUSE_LEVELS_4_NAME))
            Event_Log_for_Gantt <- Event_Log_for_Gantt %>% mutate(OPERATOR_COMMENT = convertToUTF16(OPERATOR_COMMENT))
            temp <- which(colnames(Event_Log_for_Gantt)=="Fault")
            if(length(temp)>0){
              Event_Log_for_Gantt <- Event_Log_for_Gantt %>% mutate(Fault = convertToUTF16(Fault))
            }
            dbWriteTable(conn_DBI, SQL_tablename_Event_Log_for_Gantt, Event_Log_for_Gantt, append=TRUE)
          } else {
            SQL_Appender(conn_Intermediate_Storage,Event_Log_for_Gantt,SQL_tablename_Event_Log_for_Gantt)
          }
          fin6 <- sqlQuery(conn_Intermediate_Storage,Query_6)[1,1]
          print(paste0("Delta rows in Event_Log_for_Gantt: ",fin6 - init6))
        }
      }
      
      #append new machin level data to to [First_Stop_after_CO_Data] - note that for non-Latin data sources, different library is used, as this table may include non-Latin characters.
      if(exists("Run_First_Stop_After_CO_analysis")){
        if(Run_First_Stop_After_CO_analysis=="yes"){
          if(nrow(First_Stop_after_CO_Data)>0){
            if(Write_to_SQL_via_DBI=="yes"){
              First_Stop_after_CO_Data <- First_Stop_after_CO_Data %>% mutate(CAUSE_LEVELS_1_NAME = convertToUTF16(CAUSE_LEVELS_1_NAME))
              First_Stop_after_CO_Data <- First_Stop_after_CO_Data %>% mutate(CAUSE_LEVELS_2_NAME = convertToUTF16(CAUSE_LEVELS_2_NAME))
              First_Stop_after_CO_Data <- First_Stop_after_CO_Data %>% mutate(CAUSE_LEVELS_3_NAME = convertToUTF16(CAUSE_LEVELS_3_NAME))
              First_Stop_after_CO_Data <- First_Stop_after_CO_Data %>% mutate(CAUSE_LEVELS_4_NAME = convertToUTF16(CAUSE_LEVELS_4_NAME))
              First_Stop_after_CO_Data <- First_Stop_after_CO_Data %>% mutate(OPERATOR_COMMENT = convertToUTF16(OPERATOR_COMMENT))
              temp <- which(colnames(First_Stop_after_CO_Data)=="Fault")
              if(length(temp)>0){
                First_Stop_after_CO_Data <- First_Stop_after_CO_Data %>% mutate(Fault = convertToUTF16(Fault))
              }
              dbWriteTable(conn_DBI, SQL_tablename_First_Stop_after_CO_Data, First_Stop_after_CO_Data, append=TRUE)
            } else {
              SQL_Appender(conn_Intermediate_Storage,First_Stop_after_CO_Data,SQL_tablename_First_Stop_after_CO_Data)
            }
            
            fin7 <- sqlQuery(conn_Intermediate_Storage,Query_7)[1,1]
            print(paste0("Delta rows in First_Stop_after_CO_Data: ",fin7 - init7))
          }
        }
      }
      
    }
    
  }
  
  
  #Appending new Runtime data to historical data stored in Transformed Data Storage.
  #Note that this part runs even if no new COs are in place.
  Runtime_per_Day_data <- Runtime_per_Day_data_full[Runtime_per_Day_data_full$LINE==Line_Name,]
  if (nrow(Runtime_per_Day_data)>0){
    
    temp2 <- min(Runtime_per_Day_data$Date)
    Query_3 <- paste0("SELECT COUNT(*) FROM ",SQL_tablename_Runtime_per_Day_data)
    init3 <- sqlQuery(conn_Intermediate_Storage,Query_3)[1,1]
    Query1 <- paste0("DELETE
                   FROM ",SQL_tablename_Runtime_per_Day_data,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'
                   AND Date >= CONVERT(datetime,'",as.character(temp2),"')")
    sqlQuery(conn_Intermediate_Storage,Query1)
    
    SQL_Appender(conn_Intermediate_Storage,Runtime_per_Day_data,SQL_tablename_Runtime_per_Day_data)
    fin3 <- sqlQuery(conn_Intermediate_Storage,Query_3)[1,1]
    
    print(paste0("Delta rows in Runtime_per_Day_data: ",fin3 - init3))
    
  }
  
  
  #Update script data
  Query1 <- paste0("SELECT *
                   FROM ",SQL_tablename_Script_Data,"
                   WHERE System='",System,"'")
  existing_row <- sqlQuery(conn_Intermediate_Storage,Query1)
  
  Data_Update_Time <- Current_System_Time
  Query1 <- paste0("SELECT min(CO_StartTime) as Min_time, max(CO_StartTime) as Max_time
                   FROM ",SQL_tablename_CO_Aggregated_Data,"
                   WHERE Line='",Line_Name,"'
                   AND Server='",Server_Name,"'")
  temp <- sqlQuery(conn_Intermediate_Storage,Query1)
  First_Available_Data_Point <- temp$Min_time[1]
  Last_Available_Data_Point <- temp$Max_time[1]
  
  Day_Start_hours <- Day_StartTime_per_Line$Day_Start_hours[Day_StartTime_per_Line$LINE==Line_Name]
  if(length(Day_Start_hours)==0){
    Day_Start_hours <- existing_row$Day_Start_hours[1]
  }
  
  if(is.na(existing_row$Number_of_Constraints[1])){
    Number_of_Constraints <- 1
  } else {
    Number_of_Constraints <- existing_row$Number_of_Constraints[1]
  }
  
  if(exists("Run_Multi_Constraint_Data_Line_Script")){
    if(Run_Multi_Constraint_Data_Line_Script=="yes"){
      temp <- Number_of_Constraints_data[Number_of_Constraints_data$LINE==Line_Name,]
      if(nrow(temp)>0){
        if(temp$Number_of_Constraints[1]>Number_of_Constraints){
          Number_of_Constraints <- temp$Number_of_Constraints[1]
        }
      }
    }
  }
  
  Query1 <- paste0("UPDATE ",SQL_tablename_Script_Data,"
                   SET Data_Update_Time = CONVERT(datetime,'",as.character(Data_Update_Time),"'),
                   First_Available_Data_Point = CONVERT(datetime,'",as.character(First_Available_Data_Point),"'),
                   Last_Available_Data_Point = CONVERT(datetime,'",as.character(Last_Available_Data_Point),"'),
                   Day_Start_hours = ",Day_Start_hours,",
                   Number_of_Constraints = ",Number_of_Constraints,"
                   WHERE System='",System,"'")
  sqlQuery(conn_Intermediate_Storage,Query1)
  
  #Script_Data <- sqlFetch(conn_Intermediate_Storage,SQL_tablename_Script_Data)
  
}





#save other non-line dedicated MES tables. ([BRANDCODE_Data])
#note that all historical data stored in Transformed Data Storage is extracted and combined with new data, removing duplicates. Then this appended data is re-wrote back in Transformed Data Storage. This logic is needed for Proficy iODS sites.

Query1 <- paste0("SELECT *
                   FROM ",SQL_tablename_BRANDCODE_data,"
                   WHERE Server='",Server_Name,"'")
temp <- sqlQuery(conn_Intermediate_Storage,Query1)

temp$BRANDCODE <- as.character(temp$BRANDCODE)
temp <- temp[!(temp$BRANDCODE %in% unique(BRANDCODE_data$BRANDCODE)),]

BRANDCODE_data <- bind_rows(BRANDCODE_data,temp)

Query_4 <- paste0("SELECT COUNT(*) FROM ",SQL_tablename_BRANDCODE_data)
init4 <- sqlQuery(conn_Intermediate_Storage,Query_4)[1,1]

Query1 <- paste0("DELETE
                   FROM ",SQL_tablename_BRANDCODE_data,"
                   WHERE Server='",Server_Name,"'")
sqlQuery(conn_Intermediate_Storage,Query1)

if(Write_to_SQL_via_DBI=="yes"){
  BRANDCODE_data <- BRANDCODE_data %>% mutate(BRANDNAME = convertToUTF16(BRANDNAME))
  dbWriteTable(conn_DBI, SQL_tablename_BRANDCODE_data, BRANDCODE_data, append=TRUE)
} else {
  SQL_Appender(conn_Intermediate_Storage,BRANDCODE_data,SQL_tablename_BRANDCODE_data)
}

fin4 <- sqlQuery(conn_Intermediate_Storage,Query_4)[1,1]
print(paste0("Delta rows in Brandcode_Data: ",fin4 - init4))



