node <- as.character(data.frame(Sys.info())["nodename",1])

if (node=="azw-datalab-02"){
  Root_OneDrive <- "F:/OneDrive FhcEngAnalytics/OneDrive - Procter and Gamble/Root/"
  .libPaths(.libPaths()[2])
} else {
  Root_OneDrive <- "C:/OneDrive Sync/Procter and Gamble/FHCEngAnalytics, Ion - Root/"
}

Root_folder <- paste0(Root_OneDrive,"Deployed Applications/CO and Reblend Analytics/RCO v1/")
Root_folder_Master_Scripts <- paste0(Root_OneDrive,"Master Scripts/")

Server_Type <- "ProficyiODS" #Maple or ProficyiODS
Server_Address <- "stl-mesrptfhc.na.pg.com"
Server_Name <- "StLouis Proficy"
CO_Trigger_Parameter <- 30

Run_Machine_Level_analysis <- "no"
Run_First_Stop_After_CO_analysis <- "yes"


Line_Input_Data <- data.frame(System = c("StLouis_ADW_Tubs26","StLouis_ADW_Tubs27","StLouis_ADW_Tubs35","StLouis_ADW_Tubs24",
                                         "StLouis_ADW_Bags29","StLouis_ADW_Bags31","StLouis_ADW_Bags25","StLouis_ADW_Bags28","StLouis_ADW_Bags32"),
                              MES_Line_Name = c("TFSL-026","TFSL-027","TFSL-035","TFSL-024",
                                                "TFSL-029","TFSL-031","TFSL-025","TFSL-028","TFSL-032"),
                              MES_Constraint_Machine_String = c("ATL","ATL","ATL","Inline Tub Filler",
                                                                "Bagger","Bagger","Bagger","Bagger","Bagger"),
                              stringsAsFactors = FALSE)


Root_folder <- paste0(Root_folder,Server_Name,"/")


script_name <- paste0(Root_folder_Master_Scripts,"/RCO_Overall_orchestrator.R")
source(script_name)

rm(list = ls())