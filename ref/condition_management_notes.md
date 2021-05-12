
Any specified columns Contain specified string (s) 	Dict of -> column names : [strings, in, that, column] 
& 
Any specified columns are Exactly specified string (s)	Dict of -> column names : [legal, values, for, that, column] 
&
String in Any of specified columns	 Dict of -> strings : [column, names, to, check]
& 
No specified Columns contain specified strings	Dict of -> column names : [List, of, disqualifier, strings]


Query builder for df.query('a==b & x==z') to build these out in plain text

Give list of column names & equivalence, let them build & test their queries to see if they are getting all of the changeovers they are expecting?

https://queirozf.com/entries/pandas-query-examples-sql-like-syntax-queries-in-dataframes


Rakona Liq: 

CO_Event_Log <- LINE_DOWNTIME[(LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" | LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="PLANOVANE ZASTAVENI" | LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="PROCES PLAN")
                                & (grepl("Prejizdeni",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | grepl("prejizdeni",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | grepl("prestavba",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | 
                                     grepl("Prejizdeni",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | grepl("prejizdeni",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | grepl("prestavba",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME))
                                & !grepl("Cisteni stolku",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) & !grepl("Odhad tun",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) & !grepl("Odhad tun",LINE_DOWNTIME$CAUSE_LEVELS_4_NAME),]


 "(['Planned Downtime', PLANOVANE ZASTAVENI, 'PROCES PLAN'] in reason1 ) & ((['Prejizdeni', 'prejizdeni', 'prestavba'] in reason2) | (['Prejizdeni', 'prejizdeni', 'prestavba'] in reason3)) & ('Cisteni stolku' not in reason3) & ('Odhad tun' not in reason3) & ('Odhad tun' not in reason4)"


St Louis Proficy: 

"('Changeover' in CAUSE_LEVELS_2_NAME)"


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
} else if (Server_Name=="Tabler HC"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Planned downtime",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) & LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="Changeover",]
} else if (Server_Name=="StLouis Proficy"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="StLouis Maple"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & 
                                  (grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | grepl("Brand Change",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME)),]
} else if (Server_Name=="Takasaki SUD"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Gattatico"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="London HDW"){
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
} else if (Server_Name=="Gebze BabyCare" | Server_Name=="Euskirchen"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("990",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) | grepl("991",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) | grepl("992",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME),]
} else if (Server_Name=="Gebze FemCare"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$Reason1Category=="Planned Downtime" & grepl("CHANGEOVER",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Alexandria HDL"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME),]
} else if (Server_Name=="Alexandria DL"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Urlati BC"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("-Planned",LINE_DOWNTIME$Reason2Category) & 
                                  (grepl("C/O",LINE_DOWNTIME$Reason3Category) | grepl("C/O",LINE_DOWNTIME$Reason4Category) | grepl("3D",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME))
                                & !grepl("Folie",LINE_DOWNTIME$Reason3Category) & !grepl("End of tank",LINE_DOWNTIME$Reason3Category) & !grepl("Graphics",LINE_DOWNTIME$Reason4Category),]
} else if (Server_Name=="Cairo"){
  #CO_Event_Log <- LINE_DOWNTIME[grepl("C/O",LINE_DOWNTIME$Reason4Category) | grepl("???????? ??????_1",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | 
  #                                grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | grepl("CHANGE OVER",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME),]
  
  CO_Event_Log <- LINE_DOWNTIME[grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | grepl("CHANGE OVER",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME),]
} else if (Server_Name=="Cairo FemCare"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Change",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME) | grepl("CHANGE",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Urlati SUD"){
  CO_Event_Log <- LINE_DOWNTIME[LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & (LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="Changeover" | grepl("CO",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME)),]
  CO_Event_Log <- CO_Event_Log[!is.na(CO_Event_Log$CAUSE_LEVELS_1_NAME),]
} else if (Server_Name=="Takasaki LIQ"){
  CO_Event_Log <- LINE_DOWNTIME[(LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Planned Downtime" & grepl("Change",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME)) |
                                  LINE_DOWNTIME$CAUSE_LEVELS_1_NAME=="Changeover",]
} else if (Server_Name=="Pomezia"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Planned",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) & grepl("Cambio",LINE_DOWNTIME$CAUSE_LEVELS_2_NAME),]
} else if (Server_Name=="Dammam"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("-Planned",LINE_DOWNTIME$Reason1Category) & 
                                  (grepl("C/O",LINE_DOWNTIME$Reason2Category) | grepl("C/O",LINE_DOWNTIME$Reason3Category) | grepl("C/O",LINE_DOWNTIME$Reason4Category)
                                   | grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME) | grepl("changeover",LINE_DOWNTIME$CAUSE_LEVELS_3_NAME)
                                   | grepl("Changeover",LINE_DOWNTIME$CAUSE_LEVELS_4_NAME) | grepl("changeover",LINE_DOWNTIME$CAUSE_LEVELS_4_NAME)),]
} else if (Server_Name=="Mechelen"){
  CO_Event_Log <- LINE_DOWNTIME[grepl("Planned",LINE_DOWNTIME$CAUSE_LEVELS_1_NAME) & LINE_DOWNTIME$CAUSE_LEVELS_2_NAME=="Changeover",]
}
