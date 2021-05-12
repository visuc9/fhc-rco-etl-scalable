#This sub-ETL looks at the Constraint Downtime Log per CO to identify the first Constraint Unplanned Stop after the CO.
#It logs these first stops after CO in [First_Stop_after_CO_Data] table. It also adds the [Total_Uptime_till_Next_CO] to [CO_Aggregated_Data], which is the total uptime in constraint until the next CO starts.
#This [Total_Uptime_till_Next_CO] is used later in PowerBI if there is zero unplanned stop between two COs, to report the uptime after CO.
#In other words, [First_Stop_after_CO_Data] may not include a row for all COs.

CO_Aggregated_Data$Total_Uptime_till_Next_CO <- 0

dummy1 <- 0
for (i in 1:length(Lines)){
  
  #filter data per line
  Line_name <- Lines[i]
  CO_Aggregated_Data_temp <- CO_Aggregated_Data[CO_Aggregated_Data$LINE==Line_name,]
  LINE_DOWNTIME_temp <- LINE_DOWNTIME_full[LINE_DOWNTIME_full$LINE==Line_name,]
  CO_Aggregated_Data_temp <- CO_Aggregated_Data_temp[order(CO_Aggregated_Data_temp$CO_StartTime),]
  
  if (nrow(CO_Aggregated_Data_temp)>0){
    
    #if there is at least one CO for the given line, perform the analysis per CO. 
    for (j in 1:nrow(CO_Aggregated_Data_temp)){
      
      #define the time range to explore. by default, it is the time between end of CO till start of next CO. if it's the last CO, then we only look at the 30mins after the CO.
      temp1 <- CO_Aggregated_Data_temp$CO_EndTime[j]
      if(nrow(CO_Aggregated_Data_temp)>1 & j<nrow(CO_Aggregated_Data_temp)){
        temp2 <- CO_Aggregated_Data_temp$CO_StartTime[j+1]
      } else {
        temp2 <- temp1 + 30*60*60*24
      }
      
      #filter downtime events for the given CO
      Line_Stops <- LINE_DOWNTIME_temp[LINE_DOWNTIME_temp$START_TIME>=temp1 & LINE_DOWNTIME_temp$START_TIME<temp2,]
      Line_Stops_all <- Line_Stops
      
      Total_Uptime_till_Next_CO <- 0
      if(nrow(Line_Stops)>0){
        
        Line_Stops$Uptime_cumul <- 0
        Line_Stops$UptimeDowntime_cumul <- 0
        
        Line_Stops$Uptime_cumul[1] <- Line_Stops$UPTIME[1]
        Line_Stops$UptimeDowntime_cumul[1] <- Line_Stops$UPTIME[1] + Line_Stops$DOWNTIME[1]
        
        #Calculate the cumulative uptime and downtime per downtime event
        if (nrow(Line_Stops)>1){
          for (k in 2:nrow(Line_Stops)){
            Line_Stops$Uptime_cumul[k] <- Line_Stops$Uptime_cumul[k-1] + Line_Stops$UPTIME[k]
            Line_Stops$UptimeDowntime_cumul[k] <- Line_Stops$UptimeDowntime_cumul[k-1] + Line_Stops$UPTIME[k] + Line_Stops$DOWNTIME[k]
          }
        }        
        
        Line_Stops$UptimeDowntime_at_Start <- Line_Stops$UptimeDowntime_cumul - Line_Stops$DOWNTIME
        Line_Stops <- Line_Stops[,-which(colnames(Line_Stops)=="UptimeDowntime_cumul")]
        Line_Stops$CO_Identifier <- CO_Aggregated_Data_temp$CO_Identifier[j]
        
        #filter only unplanned stops that are not to be excluded from PR calculations.
        Line_Stops <- Line_Stops[Line_Stops$Planned_Stop_Check==0 & Line_Stops$Idle_Check==0 & Line_Stops$IS_EXCLUDED==0,]
        
        #if at least one unplanned stop is found, append its first row (i.e. first stop after CO) to [First_Stop_after_CO_cumul] (i.e. temporary log First Stop after CO).
        if(nrow(Line_Stops)>0){
          if(dummy1==0){
            First_Stop_after_CO_cumul <- Line_Stops[1,]
            dummy1 <- 1
          } else {
            First_Stop_after_CO_cumul <- rbind(First_Stop_after_CO_cumul,Line_Stops[1,])
          }
        } else {
          #print(paste0(i," ",j))
        }
        #calculate total uptime until next CO by summing up the uptime for all events (i.e. NOT only the unplanned stops).
        Total_Uptime_till_Next_CO <- sum(Line_Stops_all$UPTIME)
        
      }
      
      #add the total uptime until next CO to [CO_Aggregated_Data] IF it's NOT the last CO for a given line.
      if(j<nrow(CO_Aggregated_Data_temp)){
        CO_Identifier_for_next_CO <- CO_Aggregated_Data_temp$CO_Identifier[j+1]
        temp <- CO_Event_Log[CO_Event_Log$CO_Identifier==CO_Identifier_for_next_CO,]
        temp <- temp[order(temp$START_TIME),]
        Total_Uptime_till_Next_CO <- Total_Uptime_till_Next_CO + sum(temp$UPTIME[1])
      }
      
      Total_Uptime_till_Next_CO <- round(Total_Uptime_till_Next_CO,2)
      CO_Aggregated_Data$Total_Uptime_till_Next_CO[CO_Aggregated_Data$CO_Identifier==CO_Aggregated_Data_temp$CO_Identifier[j]] <- Total_Uptime_till_Next_CO
    }
  }
}

First_Stop_after_CO_Data <- First_Stop_after_CO_cumul

#clean up columns & round up numerical columns
First_Stop_after_CO_Data <- First_Stop_after_CO_Data[,c(which(colnames(First_Stop_after_CO_Data)=="START_TIME"),
                                                        which(colnames(First_Stop_after_CO_Data)=="DOWNTIME"),
                                                        which(colnames(First_Stop_after_CO_Data)=="UPTIME"),
                                                        which(colnames(First_Stop_after_CO_Data)=="Uptime_cumul"),
                                                        which(colnames(First_Stop_after_CO_Data)=="Fault"),
                                                        which(colnames(First_Stop_after_CO_Data)=="CAUSE_LEVELS_1_NAME"),
                                                        which(colnames(First_Stop_after_CO_Data)=="CAUSE_LEVELS_2_NAME"),
                                                        which(colnames(First_Stop_after_CO_Data)=="CAUSE_LEVELS_3_NAME"),
                                                        which(colnames(First_Stop_after_CO_Data)=="CAUSE_LEVELS_4_NAME"),
                                                        which(colnames(First_Stop_after_CO_Data)=="BRANDCODE"),
                                                        which(colnames(First_Stop_after_CO_Data)=="OPERATOR_COMMENT"),
                                                        which(colnames(First_Stop_after_CO_Data)=="LINE"),
                                                        which(colnames(First_Stop_after_CO_Data)=="DOWNTIME_PK"),
                                                        which(colnames(First_Stop_after_CO_Data)=="CO_Identifier"))]
First_Stop_after_CO_Data$Server <- Server_Name

First_Stop_after_CO_Data$DOWNTIME <- round(First_Stop_after_CO_Data$DOWNTIME,2)
First_Stop_after_CO_Data$UPTIME <- round(First_Stop_after_CO_Data$UPTIME,2)
First_Stop_after_CO_Data$Uptime_cumul <- round(First_Stop_after_CO_Data$Uptime_cumul,2)


