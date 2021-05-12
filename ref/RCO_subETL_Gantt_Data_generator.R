#Define the range of data generation before/after CO.
Minutes_to_take_data_before_CO <- 5
Minutes_to_take_data_after_CO <- Minutes_to_take_Machine_data_after_CO

MACHINE_DOWNTIME$EndTime <- ymd_hms(as.character(MACHINE_DOWNTIME$EndTime))
MACHINE_DOWNTIME$MACHINE <- as.character(MACHINE_DOWNTIME$MACHINE)
MACHINE_DOWNTIME$LINE <- as.character(MACHINE_DOWNTIME$LINE)
MACHINE_DOWNTIME$DOWNTIME_PK <- as.character(MACHINE_DOWNTIME$DOWNTIME_PK)

#exception handling - add/remove rows or tables if they were missed in MES data extraction stage.
if(!exists("LINE_DOWNTIME_full")){
  LINE_DOWNTIME_full <- LINE_DOWNTIME
}
temp <- which(colnames(LINE_DOWNTIME_full)=="PUDesc")
if(length(temp)>0){
  names(LINE_DOWNTIME_full)[temp] <- "MACHINE"
}
temp <- which(colnames(LINE_DOWNTIME_full)=="PLC_CODE")
if(length(temp)>0){
  names(LINE_DOWNTIME_full)[temp] <- "Fault"
}
temp <- which(colnames(MACHINE_DOWNTIME)=="PLC_CODE")
if(length(temp)>0){
  names(MACHINE_DOWNTIME)[temp] <- "Fault"
}


#GANTT DATA GENERATION (FOR NON-CONSTRAINT MACHINES)
#By looking at the downtime log of each MES Machine, [Gantt_Data] log is created.
#For the data to be properly visualized in PowerBI, for every uptime and downtime, a data point is created both at the beginning and at the end of each uptime and downtime.
#[DOWNTIME_STATUS] key: "2": Downtime / "3": Uptime.
#For every row generated in [Gantt_Data], it's downtime log primary key [DOWNTIME_PK] is stored, and all those downtime log rows are stored in [Event_Log_for_Gantt].
Gantt_Data <- data.frame(StartTime="",Line="",Machine="",Downtime_Status="",DOWNTIME_PK="",CO_Identifier="", stringsAsFactors = FALSE)
Gantt_Data$StartTime <- ymd_hms(as.character(Gantt_Data$StartTime))
Gantt_Data$Downtime_Status <- as.numeric(Gantt_Data$Downtime_Status)

Event_Log_for_Gantt <- MACHINE_DOWNTIME[1,]
Event_Log_for_Gantt$CO_Identifier <- ""
Event_Log_for_Gantt <- MACHINE_DOWNTIME[0,]

#loop per line.
for (i in 1:length(Lines)){
  Line_name <- Lines[i]
  CO_Aggregated_Data_temp <- CO_Aggregated_Data[CO_Aggregated_Data$LINE==Line_name,]
  MACHINE_DOWNTIME_temp <- MACHINE_DOWNTIME[MACHINE_DOWNTIME$LINE==Line_name,]
  
  #if there is at least one CO available for that line..
  if (nrow(CO_Aggregated_Data_temp)>0){
    
    #loop per CO
    for (j in 1:nrow(CO_Aggregated_Data_temp)){
      #define the time window boundaries of the Gantt data taking.
      CO_EndTime <- CO_Aggregated_Data_temp$CO_EndTime[j]
      Max_Time <- CO_EndTime + Minutes_to_take_data_after_CO*60
      CO_StartTime <- CO_Aggregated_Data_temp$CO_StartTime[j]
      Min_Time <- CO_StartTime - Minutes_to_take_data_before_CO*60
      
      #filter Machine stops during this time window.
      Machine_Stops <- MACHINE_DOWNTIME_temp[MACHINE_DOWNTIME_temp$EndTime>Min_Time & MACHINE_DOWNTIME_temp$START_TIME<Max_Time,]
      Machine_Stops <- Machine_Stops[!is.na(Machine_Stops$START_TIME),]
      
      if (nrow(Machine_Stops)>0){
        Machines <- unique(Machine_Stops$MACHINE)
        
        #loop per machine
        for (k in 1:length(Machines)){
          Machine_name <- Machines[k]
          Stops_of_Machine <- Machine_Stops[Machine_Stops$MACHINE==Machine_name,]
          
          #convert uptime/downtime to seconds
          Stops_of_Machine$DOWNTIME <- Stops_of_Machine$DOWNTIME*60
          Stops_of_Machine$UPTIME <- Stops_of_Machine$UPTIME*60
          #add data of previous uptime's end moment
          Stops_of_Machine$Previous_Uptime_End <- Stops_of_Machine$START_TIME - Stops_of_Machine$UPTIME
          
          
          #create data with first row
          Gantt_Data[nrow(Gantt_Data)+1,] <- NA
          Gantt_Data$StartTime[nrow(Gantt_Data)] <- Min_Time
          Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
          Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
          Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
          Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
          
          #here, if the there was an uptime passing thru the StartTime of time window boundary for the CO, then an extra point is added at this StartTime of time window boundary.
          #note that when creating data, if a certain uptime or downtime event is less than or equal to 1sec, then that event is skipped, and that 1sec is automatically considered as continuation of previous status of the machine.
          if (Min_Time<Stops_of_Machine$START_TIME[1] & Min_Time>Stops_of_Machine$Previous_Uptime_End[1]){
            Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
            
            Gantt_Data[nrow(Gantt_Data)+1,] <- NA
            Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$START_TIME[1] - 1
            Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
            Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
            Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
            Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
            Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
            
            Gantt_Data[nrow(Gantt_Data)+1,] <- NA
            Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$START_TIME[1]
            Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
            Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
            Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 2
            Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
            Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
            
            if(Stops_of_Machine$DOWNTIME[1]>1){
              Gantt_Data[nrow(Gantt_Data)+1,] <- NA
              Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$EndTime[1]
              Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
              Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
              Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 2
              Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
              Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
            }
            
          } else {
            Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 2
            if(Stops_of_Machine$DOWNTIME[1]>1){
              Gantt_Data[nrow(Gantt_Data)+1,] <- NA
              Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$EndTime[1]
              Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
              Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
              Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 2
              Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
              Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
            }
            
          }
          
          #create data for rest of the rows for the given machine
          if(nrow(Stops_of_Machine)>1){
            for (row_no in 2:nrow(Stops_of_Machine)){
              if(Stops_of_Machine$UPTIME[row_no]>=2){
                Gantt_Data[nrow(Gantt_Data)+1,] <- NA
                Gantt_Data$StartTime[nrow(Gantt_Data)] <- Gantt_Data$StartTime[nrow(Gantt_Data)-1] + 1
                Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
                Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
                Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
                Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[row_no]
                Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
                
                Gantt_Data[nrow(Gantt_Data)+1,] <- NA
                Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$START_TIME[row_no] - 1
                Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
                Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
                Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
                Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[row_no]
                Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
              }
              if(Stops_of_Machine$DOWNTIME[row_no]>1){
                Gantt_Data[nrow(Gantt_Data)+1,] <- NA
                Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$START_TIME[row_no]
                Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
                Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
                Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 2
                Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[row_no]
                Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
                
                Gantt_Data[nrow(Gantt_Data)+1,] <- NA
                Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$EndTime[row_no]
                Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
                Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
                Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 2
                Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[row_no]
                Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
              }
            }
          }
          
          #here, if the there was an uptime passing thru the Endime of time window boundary for the CO, then an extra point is added at this EndTime of time window boundary.
          if(Gantt_Data$StartTime[nrow(Gantt_Data)]>Max_Time){
            Gantt_Data$StartTime[nrow(Gantt_Data)] <- Max_Time
          } else {
            Gantt_Data[nrow(Gantt_Data)+1,] <- NA
            Gantt_Data$StartTime[nrow(Gantt_Data)] <- Gantt_Data$StartTime[nrow(Gantt_Data)-1] + 1
            Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
            Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
            Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
            Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- ""
            Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
            
            Gantt_Data[nrow(Gantt_Data)+1,] <- NA
            Gantt_Data$StartTime[nrow(Gantt_Data)] <- Max_Time
            Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
            Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
            Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
            Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- ""
            Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
            
          }
        }
        
        Machine_Stops$CO_Identifier <- CO_Aggregated_Data_temp$CO_Identifier[j]
        Event_Log_for_Gantt <- rbind(Event_Log_for_Gantt,Machine_Stops)
      }
      
    }
  }
}

#non-constraint data is stored temporaily separately.
Gantt_Data <- Gantt_Data[!is.na(Gantt_Data$StartTime),]
Gantt_Data_temp <- Gantt_Data
Event_Log_for_Gantt_temp <- Event_Log_for_Gantt



#GANTT DATA GENERATION (FOR CONSTRAINT MACHINES)
#By looking at the downtime log of each MES Machine, [Gantt_Data] log is created.
#For the data to be properly visualized in PowerBI, for every uptime and downtime, a data point is created both at the beginning and at the end of each uptime and downtime.
#[DOWNTIME_STATUS] key: "1": CO Event / "1.7": Planned Downtime / "2.3": Unplanned Downtime / "3": Uptime / "4": Idle.
#For every row generated in [Gantt_Data], it's downtime log primary key [DOWNTIME_PK] is stored, and all those downtime log rows are stored in [Event_Log_for_Gantt].
temp <- CO_Event_Log[,c(which(colnames(CO_Event_Log)=="LINE"),which(colnames(CO_Event_Log)=="DOWNTIME_PK"))]
temp$CO_Event <- 1
LINE_DOWNTIME_for_Gantt <- merge(x=LINE_DOWNTIME_full, y=temp, by=c("LINE","DOWNTIME_PK"), all.x = TRUE)
LINE_DOWNTIME_for_Gantt$CO_Event[is.na(LINE_DOWNTIME_for_Gantt$CO_Event)] <- 0
LINE_DOWNTIME_for_Gantt <- LINE_DOWNTIME_for_Gantt[order(LINE_DOWNTIME_for_Gantt$START_TIME),]
LINE_DOWNTIME_for_Gantt$DOWNTIME_PK <- as.character(LINE_DOWNTIME_for_Gantt$DOWNTIME_PK)
LINE_DOWNTIME_for_Gantt$DOWNTIME_PK <- as.character(LINE_DOWNTIME_for_Gantt$DOWNTIME_PK)
LINE_DOWNTIME_for_Gantt$EndTime <- ymd_hms(as.character(LINE_DOWNTIME_for_Gantt$EndTime))

Gantt_Data <- Gantt_Data[0,]
Event_Log_for_Gantt <- Event_Log_for_Gantt[0,]

#this loop is very similar to above loop for non-constraint machines and therefore not commented.
#Only differences are that there is no separate loop done per machine (as anyway only single-constraint machines go thru this loop).
#Also the [DOWNTIME_STATUS] assignment is diffent, as explained with the key above.
for (i in 1:length(Lines)){
  Line_name <- Lines[i]
  CO_Aggregated_Data_temp <- CO_Aggregated_Data[CO_Aggregated_Data$LINE==Line_name,]
  LINE_DOWNTIME_temp <- LINE_DOWNTIME_for_Gantt[LINE_DOWNTIME_for_Gantt$LINE==Line_name,]
  
  Number_of_Constraints <- 1
  if(exists("Number_of_Constraints_data")){
    temp <- Number_of_Constraints_data[Number_of_Constraints_data$LINE==Lines[i],]
    if(nrow(temp)>0){
      Number_of_Constraints <- temp$Number_of_Constraints[1]
    }
  }
  
  rownames(CO_Aggregated_Data_temp) <- NULL
  
  if (nrow(CO_Aggregated_Data_temp)>0 & Number_of_Constraints==1){
    
    for (j in 1:nrow(CO_Aggregated_Data_temp)){
      CO_EndTime <- CO_Aggregated_Data_temp$CO_EndTime[j]
      Max_Time <- CO_EndTime + Minutes_to_take_data_after_CO*60
      CO_StartTime <- CO_Aggregated_Data_temp$CO_StartTime[j]
      Min_Time <- CO_StartTime - Minutes_to_take_data_before_CO*60
      
      Stops_of_Machine <- LINE_DOWNTIME_temp[LINE_DOWNTIME_temp$EndTime>Min_Time & LINE_DOWNTIME_temp$START_TIME<Max_Time,]
      Stops_of_Machine <- Stops_of_Machine[!is.na(Stops_of_Machine$START_TIME),]
      
      Machine_name <- unique(Stops_of_Machine$MACHINE)[1]
      
      Stops_of_Machine$DOWNTIME <- Stops_of_Machine$DOWNTIME*60
      Stops_of_Machine$UPTIME <- Stops_of_Machine$UPTIME*60
      Stops_of_Machine$Previous_Uptime_End <- Stops_of_Machine$START_TIME - Stops_of_Machine$UPTIME
      
      if(nrow(Stops_of_Machine)>0){
        
        #create data for first row
        Gantt_Data[nrow(Gantt_Data)+1,] <- NA
        Gantt_Data$StartTime[nrow(Gantt_Data)] <- Min_Time
        Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
        Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
        Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
        Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
        
        if (Min_Time<Stops_of_Machine$START_TIME[1] & Min_Time>Stops_of_Machine$Previous_Uptime_End[1]){
          Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
          
          Gantt_Data[nrow(Gantt_Data)+1,] <- NA
          Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$START_TIME[1] - 1
          Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
          Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
          Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
          Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
          Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
          
          Gantt_Data[nrow(Gantt_Data)+1,] <- NA
          Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$START_TIME[1]
          Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
          Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
          Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- ifelse(Stops_of_Machine$CO_Event[1]==1,1,ifelse(Stops_of_Machine$Planned_Stop_Check[1]==1,1.7,ifelse(Stops_of_Machine$Idle_Check[1]==0,2.3,4)))
          Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
          Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
          
          if(Stops_of_Machine$DOWNTIME[1]>1){
            Gantt_Data[nrow(Gantt_Data)+1,] <- NA
            Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$EndTime[1]
            Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
            Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
            Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- ifelse(Stops_of_Machine$CO_Event[1]==1,1,ifelse(Stops_of_Machine$Planned_Stop_Check[1]==1,1.7,ifelse(Stops_of_Machine$Idle_Check[1]==0,2.3,4)))
            Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
            Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
          }
          
        } else {
          Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- ifelse(Stops_of_Machine$CO_Event[1]==1,1,ifelse(Stops_of_Machine$Planned_Stop_Check[1]==1,1.7,ifelse(Stops_of_Machine$Idle_Check[1]==0,2.3,4)))
          if(Stops_of_Machine$DOWNTIME[1]>1){
            Gantt_Data[nrow(Gantt_Data)+1,] <- NA
            Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$EndTime[1]
            Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
            Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
            Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- ifelse(Stops_of_Machine$CO_Event[1]==1,1,ifelse(Stops_of_Machine$Planned_Stop_Check[1]==1,1.7,ifelse(Stops_of_Machine$Idle_Check[1]==0,2.3,4)))
            Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[1]
            Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
          }
          
        }
        
        
        #create data for rest of the rows
        if(nrow(Stops_of_Machine)>1){
          for (row_no in 2:nrow(Stops_of_Machine)){
            if(Stops_of_Machine$UPTIME[row_no]>=2){
              Gantt_Data[nrow(Gantt_Data)+1,] <- NA
              Gantt_Data$StartTime[nrow(Gantt_Data)] <- Gantt_Data$StartTime[nrow(Gantt_Data)-1] + 1
              Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
              Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
              Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
              Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[row_no]
              Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
              
              Gantt_Data[nrow(Gantt_Data)+1,] <- NA
              Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$START_TIME[row_no] - 1
              Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
              Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
              Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
              Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[row_no]
              Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
            }
            if(Stops_of_Machine$DOWNTIME[row_no]>1){
              Gantt_Data[nrow(Gantt_Data)+1,] <- NA
              Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$START_TIME[row_no]
              Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
              Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
              
              Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- ifelse(Stops_of_Machine$CO_Event[row_no]==1,1,ifelse(Stops_of_Machine$Planned_Stop_Check[row_no]==1,1.7,ifelse(Stops_of_Machine$Idle_Check[row_no]==0,2.3,4)))
              Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[row_no]
              Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
              
              Gantt_Data[nrow(Gantt_Data)+1,] <- NA
              Gantt_Data$StartTime[nrow(Gantt_Data)] <- Stops_of_Machine$EndTime[row_no]
              Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
              Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
              Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- ifelse(Stops_of_Machine$CO_Event[row_no]==1,1,ifelse(Stops_of_Machine$Planned_Stop_Check[row_no]==1,1.7,ifelse(Stops_of_Machine$Idle_Check[row_no]==0,2.3,4)))
              Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- Stops_of_Machine$DOWNTIME_PK[row_no]
              Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
            }
          }
        }
        
        
        if(Gantt_Data$StartTime[nrow(Gantt_Data)]>Max_Time){
          Gantt_Data$StartTime[nrow(Gantt_Data)] <- Max_Time
        } else {
          Gantt_Data[nrow(Gantt_Data)+1,] <- NA
          Gantt_Data$StartTime[nrow(Gantt_Data)] <- Gantt_Data$StartTime[nrow(Gantt_Data)-1] + 1
          Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
          Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
          Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
          Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- ""
          Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
          
          Gantt_Data[nrow(Gantt_Data)+1,] <- NA
          Gantt_Data$StartTime[nrow(Gantt_Data)] <- Max_Time
          Gantt_Data$Line[nrow(Gantt_Data)] <- Line_name
          Gantt_Data$Machine[nrow(Gantt_Data)] <- Machine_name
          Gantt_Data$Downtime_Status[nrow(Gantt_Data)] <- 3
          Gantt_Data$DOWNTIME_PK[nrow(Gantt_Data)] <- ""
          Gantt_Data$CO_Identifier[nrow(Gantt_Data)] <- CO_Aggregated_Data_temp$CO_Identifier[j]
          
        }
        
        Stops_of_Machine$CO_Identifier <- CO_Aggregated_Data_temp$CO_Identifier[j]
        Event_Log_for_Gantt <- rbind(Event_Log_for_Gantt,Stops_of_Machine)
      }
      
    }
  }
}


#remove data for constraint machines generated in non-constraint level data. (as the data for these constraint machines are already generated in constraint level data).
#only exception is that, for multi-constraint lines, the constraint machines' data is kept in non-constraint level data.
for (i in 1:length(Lines)){
  Line_name <- Lines[i]
  Number_of_Constraints <- 1
  if(exists("Number_of_Constraints_data")){
    temp <- Number_of_Constraints_data[Number_of_Constraints_data$LINE==Lines[i],]
    if(nrow(temp)>0){
      Number_of_Constraints <- temp$Number_of_Constraints[1]
    }
    if(Number_of_Constraints==1){
      temp <- Gantt_Data[Gantt_Data$Line==Line_name,]
      if(nrow(temp)>0){
        temp2 <- temp$Machine[1]
        Gantt_Data_temp <- Gantt_Data_temp[!(Gantt_Data_temp$Line==Line_name & Gantt_Data_temp$Machine==temp2),]
        Event_Log_for_Gantt_temp <- Event_Log_for_Gantt_temp[!(Event_Log_for_Gantt_temp$LINE==Line_name & Event_Log_for_Gantt_temp$MACHINE==temp2),]
      }
    }
  }
}

#rever the downtime & uptime to minutes.
Event_Log_for_Gantt$DOWNTIME <- Event_Log_for_Gantt$DOWNTIME/60
Event_Log_for_Gantt$UPTIME <- Event_Log_for_Gantt$UPTIME/60

#append non-constraint and constraint level data for [Gantt_Data]
Gantt_Data <- rbind(Gantt_Data_temp,Gantt_Data)
Gantt_Data$Server <- Server_Name


#(exception handling) if downtime status can't be detected, allocate it to unplanned stop
Gantt_Data$Downtime_Status[is.na(Gantt_Data$Downtime_Status)] <- 2.3

#(exception handling) correct StartTime if same with next row
for(i in 2:(nrow(Gantt_Data)-1)){
  if(Gantt_Data$CO_Identifier[i]==Gantt_Data$CO_Identifier[i+1] & Gantt_Data$Machine[i]==Gantt_Data$Machine[i+1]){
    if(Gantt_Data$StartTime[i]>=Gantt_Data$StartTime[i+1] & Gantt_Data$Downtime_Status[i]!=Gantt_Data$Downtime_Status[i+1]){
      if(Gantt_Data$StartTime[i]>(Gantt_Data$StartTime[i-1]+1)){
        Gantt_Data$StartTime[i] <- Gantt_Data$StartTime[i] - 1
      }
    }
  }
}
for(i in 2:(nrow(Gantt_Data)-1)){
  if(Gantt_Data$CO_Identifier[i]==Gantt_Data$CO_Identifier[i+1] & Gantt_Data$Machine[i]==Gantt_Data$Machine[i+1]){
    if(Gantt_Data$StartTime[i]>=Gantt_Data$StartTime[i+1] & Gantt_Data$Downtime_Status[i]!=Gantt_Data$Downtime_Status[i+1]){
      print(i)
    }
  }
}

#(exception handling) remove non-used columns in event log before appending
temp <- which(colnames(Event_Log_for_Gantt)=="Previous_Uptime_End")
if(length(temp)>0){
  Event_Log_for_Gantt <- Event_Log_for_Gantt[,-temp]
}
temp <- which(colnames(Event_Log_for_Gantt)=="CO_Event")
if(length(temp)>0){
  Event_Log_for_Gantt <- Event_Log_for_Gantt[,-temp]
}
temp <- which(colnames(Event_Log_for_Gantt)=="PUDesc")
if(length(temp)>0){
  names(Event_Log_for_Gantt)[temp] <- "MACHINE"
}
temp <- which(colnames(Event_Log_for_Gantt_temp)=="IS_CONSTRAINT")
if(length(temp)>0){
  Event_Log_for_Gantt_temp <- Event_Log_for_Gantt_temp[,-temp]
}
temp <- which(colnames(Event_Log_for_Gantt_temp)=="IS_STOP")
if(length(temp)>0){
  Event_Log_for_Gantt_temp <- Event_Log_for_Gantt_temp[,-temp]
}
temp <- which(colnames(Event_Log_for_Gantt)=="Planned_Stop_Check")
if(length(temp)>0){
  Event_Log_for_Gantt <- Event_Log_for_Gantt[,-temp]
}
temp <- which(colnames(Event_Log_for_Gantt)=="Idle_Check")
if(length(temp)>0){
  Event_Log_for_Gantt <- Event_Log_for_Gantt[,-temp]
}
temp <- which(colnames(Event_Log_for_Gantt)=="END_TIME")
if(length(temp)>0){
  Event_Log_for_Gantt <- Event_Log_for_Gantt[,-temp]
}
temp <- which(colnames(Event_Log_for_Gantt)=="START_TIME_of_Uptime")
if(length(temp)>0){
  Event_Log_for_Gantt <- Event_Log_for_Gantt[,-temp]
}


#append non-constraint and constraint level data for [Event_Log_for_Gantt]
Event_Log_for_Gantt <- rbind(Event_Log_for_Gantt_temp,Event_Log_for_Gantt)

#clean up columns
Event_Log_for_Gantt <- Event_Log_for_Gantt[,c(which(colnames(Event_Log_for_Gantt)=="START_TIME"),
                                              which(colnames(Event_Log_for_Gantt)=="DOWNTIME"),
                                              which(colnames(Event_Log_for_Gantt)=="UPTIME"),
                                              which(colnames(Event_Log_for_Gantt)=="Fault"),
                                              which(colnames(Event_Log_for_Gantt)=="CAUSE_LEVELS_1_NAME"),
                                              which(colnames(Event_Log_for_Gantt)=="CAUSE_LEVELS_2_NAME"),
                                              which(colnames(Event_Log_for_Gantt)=="CAUSE_LEVELS_3_NAME"),
                                              which(colnames(Event_Log_for_Gantt)=="CAUSE_LEVELS_4_NAME"),
                                              which(colnames(Event_Log_for_Gantt)=="BRANDCODE"),
                                              which(colnames(Event_Log_for_Gantt)=="OPERATOR_COMMENT"),
                                              which(colnames(Event_Log_for_Gantt)=="LINE"),
                                              which(colnames(Event_Log_for_Gantt)=="MACHINE"),
                                              which(colnames(Event_Log_for_Gantt)=="DOWNTIME_PK"),
                                              which(colnames(Event_Log_for_Gantt)=="CO_Identifier"))]
Event_Log_for_Gantt$Server <- Server_Name

Event_Log_for_Gantt$DOWNTIME <- round(Event_Log_for_Gantt$DOWNTIME,2)
Event_Log_for_Gantt$UPTIME <- round(Event_Log_for_Gantt$UPTIME,2)
