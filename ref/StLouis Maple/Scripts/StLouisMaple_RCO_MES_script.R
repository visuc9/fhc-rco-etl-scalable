node <- as.character(data.frame(Sys.info())["nodename",1])

if (node=="azw-datalab-02"){
  Root_OneDrive <- "F:/OneDrive FhcEngAnalytics/OneDrive - Procter and Gamble/Root/"
  .libPaths(.libPaths()[2])
} else {
  Root_OneDrive <- "C:/OneDrive Sync/Procter and Gamble/FHCEngAnalytics, Ion - Root/"
}

Root_folder <- paste0(Root_OneDrive,"Deployed Applications/CO and Reblend Analytics/RCO v1/")
Root_folder_Master_Scripts <- paste0(Root_OneDrive,"Master Scripts/")

Server_Type <- "Maple" #Maple or ProficyiODS
Server_Address <- "stl-mespakdb.na.pg.com"
Database <- "Packing"
Server_Name <- "StLouis Maple"
CO_Trigger_Parameter <- 20

Run_Machine_Level_analysis <- "yes"
Run_First_Stop_After_CO_analysis <- "yes"


Line_Input_Data <- data.frame(System = c("StLouis_HCshared_L1","StLouis_HCshared_L2","StLouis_HCshared_L4","StLouis_HCshared_L5","StLouis_HCshared_L6","StLouis_HCshared_L7","StLouis_HCshared_L8"),
                              MES_Line_Name = c("Line 1","Line 2","Line 4","Line 5","Line 6","Line 7","Line 8"),
                              stringsAsFactors = FALSE)


Root_folder <- paste0(Root_folder,Server_Name,"/")


script_name <- paste0(Root_folder_Master_Scripts,"/RCO_Overall_orchestrator.R")
source(script_name)

rm(list = ls())