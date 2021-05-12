if(!exists("SUD_specific_RCO_script")){
  SUD_specific_RCO_script <- "no"
}


#filter CO events
if (Server_Name=="Lima SUD"){
  CO_Event_Log <- LINE_DOWNTIME[(grepl(" CO",LINE_DOWNTIME$LINE_SUBSTATE) | grepl("Code Date Change",LINE_DOWNTIME$LINE_SUBSTATE) | grepl("Changeover",LINE_DOWNTIME$LINE_SUBSTATE))
                                & LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime"
                                & (LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="Changeover" | grepl(" CO",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME))
                                ,]
} else if (Server_Name=="Rakona LIQ"){
  CO_Event_Log <- LINE_DOWNTIME[(LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" | LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="PLANOVANE ZASTAVENI" | LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="PROCES PLAN")
                                & (grepl("Prejizdeni",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | grepl("prejizdeni",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | grepl("prestavba",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | 
                                     grepl("Prejizdeni",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | grepl("prejizdeni",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | grepl("prestavba",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME))
                                & !grepl("Cisteni stolku",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) & !grepl("Odhad tun",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) & !grepl("Odhad tun",LINE_DOWNTIME$CAUSE_LEVELS_4_NAME),]
} else if (Server_Name=="Rakona DL"){
  CO_Event_Log <- LINE_DOWNTIME[(grepl(" CO",LINE_DOWNTIME$LINE_SUBSTATE) | grepl("Changeover",LINE_DOWNTIME$LINE_SUBSTATE)) &
                                  LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="PREJIZDENI",]
} else if (Server_Name=="Amiens SUD"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & (LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="Changeover" | grepl("CO",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME)),]
} else if (Server_Name=="Alex SUD" | Server_Name=="Alex SUD Proficy"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & (LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="Changeover" | grepl("CO",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME)),]
  CO_Event_Log <- CO_Event_Log[!is.na(CO_Event_Log$CAUSE_LEVELS_1_NAME),]
} else if (Server_Name=="Amiens FE" | Server_Name=="Amiens HDL" | Server_Name=="Amiens LIQ" | grepl("Amiens",Server_Name)){
  CO_Event_Log <- LINE_DOWNTIME[(LINE_DOWNTIME$LINE_SUBSTATE=="Changeover" | grepl("CO",LINE_DOWNTIME$LINE_SUBSTATE)) & 
                                  grepl("Planned ",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) & 
                                  grepl(" CO",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Novo"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("C/O",LINE_DOWNTIME$Reason3Category) & grepl("-Planned",LINE_DOWNTIME$Reason2Category) & !grepl("Change Material",LINE_DOWNTIME$Reason3Category),]
} else if (Server_Name=="Tabler HDW"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Planned downtime",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) & grepl("Change Over",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) &
                                  (grepl("Change",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | grepl("change",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME)),]
} else if (Server_Name=="StLouis Proficy"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="StLouis Maple"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & 
                                  (grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | grepl("Brand Change",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME)),]
} else if (Server_Name=="Takasaki SUD"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Gattatico"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
}  else if (Server_Name=="London HDW"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & grepl(" Change",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Gebze HDW"){
  CO_Event_Log <- LINE_DOWNTIME[(LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="SCO" | LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="BCO") & grepl("PLANLI DURUS",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME),]
} else if (Server_Name=="Gebze DL"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("DEGISIM",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME)
                                & grepl("PLANLI DURUS",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME)
                                & !grepl("PAKETLEME MALZEMESI",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Cabuyao"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME)
                                | grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME),]
} else if (Server_Name=="Lima LIQ"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Changeover" | grepl("Changeover Failure",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME),]
} else if (Server_Name=="Chengdu"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Change over",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME),]
} else if (Server_Name=="Binh Duong"){
  CO_Event_Log <- LINE_DOWNTIME[(LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="Changeover") |
                                  LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Changeover",]
} else if (Server_Name=="Gebze BabyCare"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("990",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) | grepl("991",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) | grepl("992",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME),]
} else if (Server_Name=="Gebze FemCare"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$Reason1Category=="Planned Downtime" & grepl("CHANGEOVER",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Alexandria HDL"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME),]
} else if (Server_Name=="Alexandria DL"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
}

#skip all RCO ETL section if no CO is identified.
No_CO_Flag <- 0
if(nrow(CO_Event_Log)==0){
  No_CO_Flag <- 1
} else {
  
  #define [CO_Trigger_Column] based on Cause Lvl 1/2/3. This column is then used in the logic for detecting split CO events belonging to same CO.
  CO_Event_Log$CAUSE_LEVELS_1_NAME <- as.character(CO_Event_Log$CAUSE_LEVELS_1_NAME)
  CO_Event_Log$CAUSE_LEVELS_2_NAME <- as.character(CO_Event_Log$CAUSE_LEVELS_2_NAME)
  CO_Event_Log$CAUSE_LEVELS_3_NAME <- as.character(CO_Event_Log$CAUSE_LEVELS_3_NAME)
  CO_Event_Log$CAUSE_LEVELS_4_NAME <- as.character(CO_Event_Log$CAUSE_LEVELS_4_NAME)
  CO_Event_Log$CO_Trigger_Column <- paste0(
    ifelse(is.na(CO_Event_Log$CAUSE_LEVELS_1_NAME),"",CO_Event_Log$CAUSE_LEVELS_1_NAME),
    " - ",
    ifelse(is.na(CO_Event_Log$CAUSE_LEVELS_2_NAME),"",CO_Event_Log$CAUSE_LEVELS_2_NAME),
    " - ",
    ifelse(is.na(CO_Event_Log$CAUSE_LEVELS_3_NAME),"",CO_Event_Log$CAUSE_LEVELS_3_NAME))
  
  #order data per line and starttime. this ordering becomes crucial in combining split CO events into single CO as we look at previous row to make the decision.
  CO_Event_Log <- CO_Event_Log[order(CO_Event_Log$LINE,CO_Event_Log$START_TIME),]
  CO_Event_Log <- CO_Event_Log[!is.na(CO_Event_Log$START_TIME),] #exception handling
  
  
  #add number of seconds of downtime, the endtime of the downtime and row index
  CO_Event_Log$Downtime_sec <- CO_Event_Log$DOWNTIME * 60
  CO_Event_Log$END_TIME <- CO_Event_Log$START_TIME + CO_Event_Log$Downtime_sec
  CO_Event_Log$index <- row(CO_Event_Log)[,1]
  
  #add previous rows' relevant data to current row, and calculate the minutes difference between end-time of previous CO event and start-time of current CO event
  CO_Event_Log$BRANDCODE <- as.character(CO_Event_Log$BRANDCODE)
  CO_Event_Log <- transform(CO_Event_Log, Previous_BRANDCODE = lag(BRANDCODE, n=1, default=0))
  CO_Event_Log <- transform(CO_Event_Log, Previous_LINE = lag(LINE, n=1, default=0))
  CO_Event_Log <- transform(CO_Event_Log, Previous_CO_Trigger_Column = lag(CO_Trigger_Column, n=1, default=0))
  CO_Event_Log <- transform(CO_Event_Log, Previous_END_TIME = lag(END_TIME, n=1, default=0))
  CO_Event_Log$MinutesDifference_vs_PreviousRow <- difftime(CO_Event_Log$START_TIME, CO_Event_Log$Previous_END_TIME, units=c("secs")) / 60
  
  #add [CO_Trigger] column, which tells whether this row is a new CO vs the previous row. Conditions to confirm whether two events belong to same CO (at least one of them needs to be true):
  #-if Cause Model Lvl 1/2/3 is same vs previous row, then is the minute diffence between end-time of previous CO event and start-time of current CO event is less than the parameter defined at site-level input script?
  #-if Brandcode is same vs previous row, then is the minute diffence between end-time of previous CO event and start-time of current CO event is less than the parameter defined at site-level input script?
  #-if both above is true, then is the minute diffence between end-time of previous CO event and start-time of current CO event is less than 4/3 times the parameter defined at site-level input script?
  #-if none of above is true, then is the minute diffence between end-time of previous CO event and start-time of current CO event is less than 2/3 times the parameter defined at site-level input script?
  CO_Event_Log$CO_Trigger <- ifelse((CO_Event_Log$MinutesDifference_vs_PreviousRow < CO_Trigger_Parameter
                                     & CO_Event_Log$CO_Trigger_Column == CO_Event_Log$Previous_CO_Trigger_Column
                                     & CO_Event_Log$LINE == CO_Event_Log$Previous_LINE) |
                                      (CO_Event_Log$MinutesDifference_vs_PreviousRow < CO_Trigger_Parameter*4/3
                                       & CO_Event_Log$CO_Trigger_Column == CO_Event_Log$Previous_CO_Trigger_Column
                                       & CO_Event_Log$BRANDCODE == CO_Event_Log$Previous_BRANDCODE
                                       & CO_Event_Log$LINE == CO_Event_Log$Previous_LINE) |
                                      (CO_Event_Log$MinutesDifference_vs_PreviousRow < CO_Trigger_Parameter
                                       & CO_Event_Log$BRANDCODE == CO_Event_Log$Previous_BRANDCODE
                                       & CO_Event_Log$LINE == CO_Event_Log$Previous_LINE) |
                                      (CO_Event_Log$MinutesDifference_vs_PreviousRow < CO_Trigger_Parameter/3*2
                                       & CO_Event_Log$LINE == CO_Event_Log$Previous_LINE),
                                    0,1)
  
  #this is exception - for certain plants, we have to split CO events into separate COs if the Cause Model Lvl 1/2/3 is different. The input parameter is set at site's input script.
  if(exists("Split_COs_based_on_Cause_Model")){
    if(Split_COs_based_on_Cause_Model=="yes"){
      CO_Event_Log$CO_Trigger <- ifelse(CO_Event_Log$CO_Trigger_Column != CO_Event_Log$Previous_CO_Trigger_Column,1,
                                        CO_Event_Log$CO_Trigger)   
    }
  }
  
  #exception handling - for Lima SUD, there is specific request not ot split CO events if the Cause Model includes "Changeover Failure" and the minute difference vs previous row is less than 2hr.
  if (Server_Name=="Lima SUD"){
    CO_Event_Log$CO_Trigger <- ifelse(CO_Event_Log$CO_Trigger==1 & grepl("Changeover Failure",CO_Event_Log$CO_Trigger_Column) & CO_Event_Log$MinutesDifference_vs_PreviousRow < 120
                                      & CO_Event_Log$LINE == CO_Event_Log$Previous_LINE,
                                      0,CO_Event_Log$CO_Trigger)
  }
  
  #For rows which is the first events of a new CO, add [CO_Identifier], and fill the rest of the events belonging to the same CO with same [CO_Identifier].
  temp <- CO_Event_Log[CO_Event_Log$CO_Trigger==1,]
  temp$CO_Identifier <- paste0(temp$LINE," - ",substr(as.character(temp$START_TIME),1,10)," - ",substr(temp$DOWNTIME_PK,1,10))
  temp <- temp[,c(which(colnames(temp)=="index"),
                  which(colnames(temp)=="CO_Identifier"))]
  CO_Event_Log <- merge(x= CO_Event_Log, y= temp, by="index", all.x = TRUE)
  CO_Event_Log <- fill(CO_Event_Log,c("CO_Identifier"), .direction = c("down"))
  
  #Generate [CO_Aggregated data]
  CO_Aggregated_Data <- CO_Event_Log %>% group_by(CO_Identifier, LINE) %>% summarise(CO_StartTime = min(START_TIME),
                                                                                     CO_EndTime = max(END_TIME),
                                                                                     Index_of_First_CO_Event = min(index),
                                                                                     Index_of_Last_CO_Event = max(index),
                                                                                     CO_DOWNTIME = sum(DOWNTIME))
  CO_Aggregated_Data <- CO_Aggregated_Data[order(CO_Aggregated_Data$LINE,CO_Aggregated_Data$CO_StartTime),]
  
  #fix datetime fields' data type
  CO_Aggregated_Data$CO_StartTime <- ymd_hms(as.character(CO_Aggregated_Data$CO_StartTime))
  CO_Aggregated_Data$CO_EndTime <- ymd_hms(as.character(CO_Aggregated_Data$CO_EndTime))
  
  #add Downtime PK to first/last events
  temp <- CO_Event_Log[,c(which(colnames(CO_Event_Log)=="index"),
                          which(colnames(CO_Event_Log)=="DOWNTIME_PK"))]
  names(temp)[which(colnames(temp)=="DOWNTIME_PK")] <- "DOWNTIME_PK_of_First_CO_Event"
  CO_Aggregated_Data <- merge(x= CO_Aggregated_Data, y= temp, by.x= "Index_of_First_CO_Event", by.y= "index")
  
  temp <- CO_Event_Log[,c(which(colnames(CO_Event_Log)=="index"),
                          which(colnames(CO_Event_Log)=="DOWNTIME_PK"))]
  names(temp)[which(colnames(temp)=="DOWNTIME_PK")] <- "DOWNTIME_PK_of_Last_CO_Event"
  CO_Aggregated_Data <- merge(x= CO_Aggregated_Data, y= temp, by.x= "Index_of_Last_CO_Event", by.y= "index")
  
  
  
  
  
  
  #BRANDCODE DETERMINATION
  #add columns on next CO StartTime
  CO_Aggregated_Data <- transform(CO_Aggregated_Data, Next_CO_StartTime = lead(CO_StartTime, n=1, default=0))
  CO_Aggregated_Data <- transform(CO_Aggregated_Data, Next_Line = lead(LINE, n=1, default=0))
  #if this is the last CO for the line, then assume there 60min timespan we can explore for Next Brandcode after CO.
  for(i in 1:nrow(CO_Aggregated_Data)){
    if(CO_Aggregated_Data$LINE[i]!=CO_Aggregated_Data$Next_Line[i]){
      CO_Aggregated_Data$Next_CO_StartTime[i] <- CO_Aggregated_Data$CO_EndTime[i] + 60*60
    }
  }
  
  CO_Aggregated_Data <- transform(CO_Aggregated_Data, Previous_CO_EndTime = lag(CO_EndTime, n=1, default=0))
  CO_Aggregated_Data <- transform(CO_Aggregated_Data, Previous_Line = lag(LINE, n=1, default=0))
  #if this is the first CO for the line, then assume there 60min timespan we can explore for Current Brandcode before CO.
  for(i in 1:nrow(CO_Aggregated_Data)){
    if(CO_Aggregated_Data$LINE[i]!=CO_Aggregated_Data$Previous_Line[i]){
      CO_Aggregated_Data$Previous_CO_EndTime[i] <- CO_Aggregated_Data$CO_StartTime[i] - 60*60
    }
  }
 
  LINE_DOWNTIME_full$END_TIME <- LINE_DOWNTIME_full$START_TIME + LINE_DOWNTIME_full$DOWNTIME*60
  LINE_DOWNTIME_full$START_TIME_of_Uptime <- LINE_DOWNTIME_full$START_TIME - LINE_DOWNTIME_full$UPTIME*60
  
  CO_Aggregated_Data$Current_BRANDCODE <- ""
  CO_Aggregated_Data$Next_BRANDCODE <- ""
  
  #per CO, look at the line downtime log to determine brandcodes.
  #For Current Brandcode, by default the full timespan between the end of previous CO until the start of current CO is investigated. And the last available brandcode is taken. i.e. In this timespan, if two brandcodes are observed, then the one observed latest is used.
  #For Next Brandcode, by default the full timespan between the end of current CO until the start of next CO is investigated. And the first available brandcode that is NOT equal to Current Brandcode is taken. If no such brandcode is available, then Next Brandcode is made equal to Current Brandcode.
  
  for (i in 1:nrow(CO_Aggregated_Data)){
    temp <- LINE_DOWNTIME_full[LINE_DOWNTIME_full$LINE==CO_Aggregated_Data$LINE[i] & LINE_DOWNTIME_full$START_TIME>CO_Aggregated_Data$Previous_CO_EndTime[i] & LINE_DOWNTIME_full$START_TIME<=CO_Aggregated_Data$CO_StartTime[i],]
    temp2 <- temp[temp$START_TIME_of_Uptime<CO_Aggregated_Data$CO_StartTime[i],]
    if(nrow(temp2)>0){
      CO_Aggregated_Data$Current_BRANDCODE[i] <- temp2$BRANDCODE[nrow(temp2)]
    } else {
      if(nrow(temp)>0){
        CO_Aggregated_Data$Current_BRANDCODE[i] <- temp$BRANDCODE[nrow(temp)]
      }
    }
    
    temp <- LINE_DOWNTIME_full[LINE_DOWNTIME_full$LINE==CO_Aggregated_Data$LINE[i] & LINE_DOWNTIME_full$START_TIME_of_Uptime>CO_Aggregated_Data$CO_StartTime[i] & LINE_DOWNTIME_full$START_TIME_of_Uptime<CO_Aggregated_Data$Next_CO_StartTime[i],]
    if(nrow(temp)>0){
      temp2 <- temp[temp$BRANDCODE!=CO_Aggregated_Data$Current_BRANDCODE[i],]
      if(nrow(temp2)>0){
        CO_Aggregated_Data$Next_BRANDCODE[i] <- temp2$BRANDCODE[1]
      } else {
        CO_Aggregated_Data$Next_BRANDCODE[i] <- CO_Aggregated_Data$Current_BRANDCODE[i]
      }
    }
  }
  #add column indicating whether brandcode is changed.
  CO_Aggregated_Data$Brandcode_Status <- ifelse(CO_Aggregated_Data$Current_BRANDCODE==CO_Aggregated_Data$Next_BRANDCODE,"Not Changed","OK")
  
  
  #clean-up columns
  CO_Aggregated_Data <- CO_Aggregated_Data[c(which(colnames(CO_Aggregated_Data)=="CO_Identifier"),
                                             which(colnames(CO_Aggregated_Data)=="LINE"),
                                             which(colnames(CO_Aggregated_Data)=="CO_StartTime"),
                                             which(colnames(CO_Aggregated_Data)=="CO_EndTime"),
                                             which(colnames(CO_Aggregated_Data)=="CO_DOWNTIME"),
                                             which(colnames(CO_Aggregated_Data)=="Current_BRANDCODE"),
                                             which(colnames(CO_Aggregated_Data)=="Next_BRANDCODE"),
                                             which(colnames(CO_Aggregated_Data)=="DOWNTIME_PK_of_First_CO_Event"),
                                             which(colnames(CO_Aggregated_Data)=="DOWNTIME_PK_of_Last_CO_Event"),
                                             which(colnames(CO_Aggregated_Data)=="Brandcode_Status"))]
  CO_Aggregated_Data$Server <- Server_Name
  CO_Aggregated_Data <- CO_Aggregated_Data[order(CO_Aggregated_Data$CO_StartTime),]
  
  CO_Event_Log <- CO_Event_Log[c(which(colnames(CO_Event_Log)=="CO_Identifier"),
                                 which(colnames(CO_Event_Log)=="LINE"),
                                 which(colnames(CO_Event_Log)=="CAUSE_LEVELS_1_NAME"),
                                 which(colnames(CO_Event_Log)=="CAUSE_LEVELS_2_NAME"),
                                 which(colnames(CO_Event_Log)=="CAUSE_LEVELS_3_NAME"),
                                 which(colnames(CO_Event_Log)=="CAUSE_LEVELS_4_NAME"),
                                 which(colnames(CO_Event_Log)=="START_TIME"),
                                 which(colnames(CO_Event_Log)=="UPTIME"),
                                 which(colnames(CO_Event_Log)=="DOWNTIME"),
                                 which(colnames(CO_Event_Log)=="BRANDCODE"),
                                 which(colnames(CO_Event_Log)=="TEAM"),
                                 which(colnames(CO_Event_Log)=="SHIFT"),
                                 which(colnames(CO_Event_Log)=="OPERATOR_COMMENT"),
                                 which(colnames(CO_Event_Log)=="CO_Trigger_Column"),
                                 which(colnames(CO_Event_Log)=="END_TIME"),
                                 which(colnames(CO_Event_Log)=="DOWNTIME_PK"),
                                 which(colnames(CO_Event_Log)=="Reason1Category"),
                                 which(colnames(CO_Event_Log)=="Reason2Category"),
                                 which(colnames(CO_Event_Log)=="Reason3Category"),
                                 which(colnames(CO_Event_Log)=="Reason4Category"),
                                 which(colnames(CO_Event_Log)=="ProdDesc"),
                                 which(colnames(CO_Event_Log)=="ProcessOrder"))]
  CO_Event_Log <- CO_Event_Log[CO_Event_Log$CO_Identifier %in% unique(CO_Aggregated_Data$CO_Identifier),] #double check to ensure no events in this table that does not appear in [CO_Aggregated_Data].
  CO_Event_Log$Server <- Server_Name
  CO_Event_Log <- CO_Event_Log[order(CO_Event_Log$START_TIME),]
  
  
  #replace characters which are later causing issues in SQL or csv writing, and PowerBI reading
  CO_Event_Log$OPERATOR_COMMENT <- gsub(paste0("\\","r","\\","n")," ",CO_Event_Log$OPERATOR_COMMENT)
  CO_Event_Log$OPERATOR_COMMENT <- gsub(paste0("\\","n")," ",CO_Event_Log$OPERATOR_COMMENT)
  CO_Event_Log <- CO_Event_Log[!is.na(CO_Event_Log$LINE),]
  
  
  
  #Run stops after CO and Machine stops ETL - generates specific timestamp data used in PowerBI machine level visualization.
  #Output tables are called [Event_Log_for_Gantt] and [Gantt_Data].
  if(exists("Run_Machine_Level_analysis")){
    if(Run_Machine_Level_analysis=="yes"){
      script_name <- paste0(Root_folder_Master_Scripts,"/RCO_subETL_Gantt_Data_generator.R")
      source(script_name)
    }
  }
  
  #Run First Stop after CO ETL - used to log first Unplanned Constraint Stop after CO and the total uptime passed till that stop.
  #Output table is called [First_Stop_after_CO_Data].
  if(exists("Run_First_Stop_After_CO_analysis")){
    if(Run_First_Stop_After_CO_analysis=="yes"){
      script_name <- paste0(Root_folder_Master_Scripts,"/RCO_subETL_First_Stop_after_CO.R")
      source(script_name)
    }
  }
  
  #Run Machine Stops after CO ETL - used to log all the Machine Stops after CO till start of next CO. If Converter CO, includes machine stops for Converter_plus_Legs, and if Leg CO, machine stops for the specific Leg. Includes both constraint and non-constraint machines. 
  #Output table is called [MACHINE_DOWNTIME_Final].
  if(exists("SUD_specific_RCO_script")){
    if(SUD_specific_RCO_script=="yes"){
      script_name <- paste0(Root_folder,"Scripts/ETL_SUD_machine_stops_after_CO.R")
      source(script_name)
    }
  }
  
  #Run Converter Constraint Stops after CO ETL - used to log all the Constraint Stops after CO.
  #Output table is called [Converter_Downtime_Final].
  #CURRENTLY DISABLED.
  if(exists("SUD_specific_RCO_script")){
    if(SUD_specific_RCO_script=="yes" & FALSE){
      script_name <- paste0(Root_folder,"Scripts/ETL_SUD_Converter_Constraint_stops_after_CO.R")
      source(script_name)
    }
  }
}
