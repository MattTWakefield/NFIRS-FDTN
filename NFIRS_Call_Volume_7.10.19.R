library(data.table)
library(lubridate)
library(tidyverse)
library(magrittr)
library(RODBC)
library(odbc)

InYear<-data.frame(InYear = c(2014:2018))

nfirsfile<-'./data/NFIRS.rds'
if (file.exists(nfirsfile)){
  print('Reading from RDS')
  NFIRS<-readRDS(nfirsfile)
} else {
  print('Reading from database')
  dbhandle <- DBI::dbConnect(odbc::odbc(), dsn = "STS GIS NEW USER")
  NFIRS<-odbc::dbGetQuery(dbhandle, "SELECT * FROM GA_NFIRS WHERE Year BETWEEN 2014 AND 2018")
  saveRDS(NFIRS, nfirsfile)
}


NFIRS<-NFIRS%>%select(
          Incident_Number,
          Exposure,
          Last_Unit_Cleared_Date___Time,
          Controlled_Date___Time,
          Arrival_Date___Time,
          Alarm_Date___Time,
          Incident_Type_Code__National_,
          Incident_Type_Code__Category_,
          Incident_Type_Description,
          Action_Taken_1_Code__National_,
          Action_Taken_1_Description,
          Year,
          FDID,
          Aid_Given_or_Received_Code__National_,
          Aid_Given_or_Received_Description,
          Total_Loss,
          Number_of_Acres_Burned,
          New_Cause_Description,
          Fire_Ignition_Factor_1_Code,
          Fire_Ignition_Factor_1_Description,
          Equipment_Involved_in_Ignition_Code__National_,
          Equipment_Involved_in_Ignition_Description,
          Item_First_Ignited_Code,
          Item_First_Ignited_Description,
          Human_Factors_1_Code,
          Human_Factors_1_Description,
          Cause_of_Ignition_Code__National_,
          Cause_of_Ignition_Description,
          Area_of_Origin_Code__National_,
          Area_of_Origin_Description,
          Heat_Source_Code__National_,
          Heat_Source_Description,
          Property_Use_Code__National_,
          Property_Use_Description,
          Mobile_Property_Type_Code__National_,
          Mobile_Property_Type_Description,
          Fire_Service_Fatalities,
          Non_Fire_Service_Fatalities,
          Fire_Service_Injuries,
          Non_Fire_Service_Injuries,
          Detector_Presence_Code,
          Detector_Presence_Description,
          Automatic_Extinguishing_System_Presence_Code,
          Automatic_Extinguishing_System_Presence_Description,
          Wildland_Fire_Cause_Code,
          Wildland_Fire_Cause_Description,
          GSM_FLAG,
          IN_TYP_DESC,
          IN_TYP_DESC2,
          MUTFLAG
          )

NFIRS<-NFIRS%>%filter(GSM_FLAG == 0)
####Incident Summary####
#Count Incidents by year & FDID
NFIRSCNTIS<-NFIRS%>%count(Year, FDID, IN_TYP_DESC)
#Count of Incidents by year & FDID excluding aid given. 
NFIRSCNTISNA<-NFIRS%>%filter(MUTFLAG==0)%>%count(Year, FDID, IN_TYP_DESC)
#rename no aid columns (prior to pivot)
NFIRSCNTISNA$IN_TYP_DESC<-paste0(NFIRSCNTISNA$IN_TYP_DESC,"NoAid")
#bind total and no aid columns together
NFIRSCNTIS<-rbind(NFIRSCNTIS,NFIRSCNTISNA)
#Pivot out data according to IN_TYP_DESC by year and FDID.
NFIRSPVTIS<-dcast(NFIRSCNTIS, Year + FDID ~ IN_TYP_DESC)
#sort by FDID
NFIRSPVTIS<-arrange(NFIRSPVTIS,FDID)


#**Loss and Acres
#sum each values and quantity of acres for all and no aid. 
Loss<-NFIRS%>%filter(IN_TYP_DESC=='Fires') %>%
  group_by(FDID, Year)%>%
  summarise(TotalPropertyLoss = sum(as.numeric(Total_Loss), na.rm= TRUE))

Acres<-NFIRS%>%filter(IN_TYP_DESC2=='VegetationFires')%>%
  group_by(FDID, Year)%>%
  summarise(VegetationFireAcres = sum(as.numeric(Number_of_Acres_Burned), na.rm = TRUE))

LossNA<-NFIRS%>%filter(IN_TYP_DESC=='Fires', MUTFLAG == 0) %>%
  group_by(FDID, Year)%>%
  summarise(TotalPropertyLoss = sum(as.numeric(Total_Loss), na.rm= TRUE))

colnames(LossNA)[3]<-"TotalPropertyLossNoAid"


AcresNA<-NFIRS%>%filter(IN_TYP_DESC2=='VegetationFires', MUTFLAG == 0)%>%
  group_by(FDID, Year)%>%
  summarise(VegetationFireAcres = sum(as.numeric(Number_of_Acres_Burned), na.rm = TRUE))

colnames(AcresNA)[3]<-"VegetationFireAcresNoAid"

LossAcresCast<-full_join(Loss, Acres, by=c('FDID'='FDID', 'Year' = 'Year'))%>%
  full_join(LossNA, by=c('FDID' = 'FDID','Year'='Year'))%>%
  full_join(AcresNA, by = c('FDID'='FDID','Year'='Year'))

myconn<-DBI::dbConnect(odbc::odbc(), dsn = "STS GIS NEW USER")
FDS <- dbGetQuery(myconn, "select FDID, FDNAME from FR_SuppData")

DeptYears<-merge(InYear,FDS, all=TRUE)

NFIRS$New_Cause_Description<-substr(NFIRS$New_Cause_Description,7,nchar(NFIRS$New_Cause_Description))
Causes<-unique(NFIRS$New_Cause_Description[NFIRS$New_Cause_Description!=""])

DeptYearsCauses<-merge(DeptYears, Causes, all=TRUE)%>%arrange(FDID, InYear,y)

Incident_Summary<-full_join(NFIRSPVTIS, LossAcresCast, by = c("FDID" = "FDID", "Year" = "Year"))

Incident_Summary$Year<-as.integer(Incident_Summary$Year)

Incident_Summary<-left_join(DeptYears,Incident_Summary, by=c("FDID"="FDID", "InYear"="Year"))

rm(LossAcresCast,Loss, LossNA, Acres, AcresNA, NFIRSPVTIS,
   NFIRSCNTIS, NFIRSCNTISNA)
gc()

#**Incident Summary subcategories.


#Count Incidents by year & FDID
NFIRSCNTSUB<-NFIRS%>%count(Year, FDID, IN_TYP_DESC2)
#Count of Incidents by year & FDID excluding aid given. 
NFIRSCNTSUBNA<-NFIRS%>%filter(MUTFLAG==0)%>%count(Year, FDID, IN_TYP_DESC2)
#rename no aid columns (prior to pivot)
NFIRSCNTSUBNA$IN_TYP_DESC2<-paste0(NFIRSCNTSUBNA$IN_TYP_DESC2,"NoAid")
#bind total and no aid columns together
NFIRSCNTSUB<-rbind(NFIRSCNTSUB,NFIRSCNTSUBNA)
#Pivot out data according to IN_TYP_DESC by year and FDID.
NFIRSPVTSUB<-dcast(NFIRSCNTSUB, Year + FDID ~ IN_TYP_DESC2)
#sort by FDID
NFIRSPVTSUB<-arrange(NFIRSPVTSUB,FDID)

NFIRSPVTSUB$Year<-as.integer(NFIRSPVTSUB$Year)

Incident_Summary<-left_join(Incident_Summary, NFIRSPVTSUB, by=c("FDID"="FDID", "InYear"="Year"))
rm(NFIRSCNTSUB,NFIRSCNTSUBNA, NFIRSPVTSUB)
gc()


#**Structure Incident Loss

StrLoss<-NFIRS%>%filter(IN_TYP_DESC2 == 'StructureFires')%>%select(FDID, Year,Total_Loss)
StrLossMelt<-melt(StrLoss,id=c("FDID","Year"),na.rm=TRUE)
StrLossMelt$variable<-"StructureFireLoss"

StrLossNA<-NFIRS%>%filter(MUTFLAG==0, IN_TYP_DESC2 == 'StructureFires')%>%select(FDID, Year, Total_Loss)
StrLossNAMelt<-melt(StrLossNA,id=c("FDID","Year"),na.rm=TRUE)
StrLossNAMelt$variable<-"StructureFireLossNoAid"
StrLossMelt<-rbind(StrLossNAMelt, StrLossMelt)
StrLossMelt$value<-as.numeric(StrLossMelt$value)
StrLossCast<-dcast(StrLossMelt, FDID + Year ~ variable, sum, na.rm=TRUE)
StrLossCast$Year<-as.integer(StrLossCast$Year)
Incident_Summary<-left_join(Incident_Summary, StrLossCast, by=c("FDID"="FDID", "InYear"="Year"))

rm(StrLoss, StrLossMelt, StrLossNA, StrLossNAMelt, StrLossCast)
gc()
#**by mutual aid
MutAidSelect<-NFIRS%>%select(FDID, Year, Aid_Given_or_Received_Description)
MutAidMelt<-melt(MutAidSelect, id=c("FDID","Year"), na.rm=TRUE)
MutAidCast<-dcast(MutAidMelt, FDID + Year ~ value)
MutAidCast$Year<-as.integer(MutAidCast$Year)

Incident_Summary<-left_join(Incident_Summary, MutAidCast, by=c("FDID"="FDID", "InYear"="Year"))


#**by Mutual Aid Given Structure Fires
MutAidStrCnt<-NFIRS%>%filter(IN_TYP_DESC2 == 'StructureFires',  Aid_Given_or_Received_Description == "Mutual aid given")%>%
  select(FDID, Year, Aid_Given_or_Received_Description)%>%count(FDID,Year)

MutAidStrCnt$Year<-as.integer(MutAidStrCnt$Year)

Incident_Summary<-left_join(Incident_Summary, MutAidStrCnt, by=c("FDID"="FDID", "InYear"="Year"))


rm(MutAidSelect, MutAidMelt, MutAidCast, MutAidStrCnt)
gc()

Incident_Summary<-rename(Incident_Summary, 
                         Name = 'FDNAME',
                         AutoAidGiven='Automatic aid given',
                         AutoAidReceived='Automatic aid received',
                         MutualAidGiven='Mutual aid given',
                         MutualAidReceived='Mutual aid received',
                         MutualAidGiven_StructureFires='n')

Incident_Summary<-Incident_Summary%>%select(-`Other aid given`, -None,-Var.3)

Incident_Summary[is.na(Incident_Summary)|Incident_Summary==""]<-0

Incident_Summary<-select(Incident_Summary, FDID, Name, InYear, Fires, FiresNoAid, Explosions, ExplosionsNoAid, RescuesAndEMS, RescuesAndEMSNoAid
                         , Hazards, HazardsNoAid, ServiceCalls, ServiceCallsNoAid, GoodIntent, GoodIntentNoAid, FalseAlarm
                         , FalseAlarmNoAid, SevereWeather, SevereWeatherNoAid, SpecialIncident, SpecialIncidentNoAid
                         , StructureFires, StructureFiresNoAid, StructureFireLoss, StructureFireLossNoAid, VehicleFires
                         , VehicleFiresNoAid, VegetationFires, VegetationFiresNoAid, VegetationFireAcres, VegetationFireAcresNoAid
                         , MotorVehicle, MotorVehicleNoAid, MotorVehicleExtraction, MotorVehicleExtractionNoAid, EMSALSCalls
                         , EMSALSCallsNoAid, EMSBLSCalls, EMSBLSCallsNoAid, MutualAidReceived, AutoAidReceived
                         , MutualAidGiven, AutoAidGiven, MutualAidGiven_StructureFires, TotalPropertyLoss
                         , TotalPropertyLossNoAid, Rescues, RescuesNoAid)


####Fire Fatalities####
#Note that odbcConnectAccess2007, may require its own driver. Also, ensure that the database is not password protected.  
myconn<-odbcConnectAccess2007("H:/Fire Prevention/Education and Outreach/Fire Fatalities/Fire Fatality DB/Fire Fatality_be.accdb")
fatalities <- sqlQuery(myconn, "select * FROM Fires")
Causes<-sqlQuery(myconn, "select * FROM Causes")
close(myconn)
fatalities$Year<-year(fatalities$FireDate)
fatalities<-fatalities%>%filter(Cause != 8, between(Year, 2014,2018))
fatalities$FDID<-sprintf("%05d",fatalities$FDID)

fatalities<-left_join(fatalities, Causes, by=c("Cause"="ID"))

fatalities<-fatalities%>%select(FireDate, Cause.y, Year ,Fatalities, FDID)

fatalities<-rename(fatalities, Cause = Cause.y)

Sum_fatalities<-fatalities%>%group_by(FDID, Year, Cause)%>%
  summarise(CivilianFatalitiesNoAid = sum(Fatalities))
gc()

#Similar process to Incident Summary above.

StructureFires<-NFIRS%>%filter(IN_TYP_DESC2 == 'StructureFires')%>%
  rename(Cause = New_Cause_Description)

StructureFires$Year<-as.integer(StructureFires$Year)

StructureFiresCNT<-StructureFires%>%count(Year, FDID, Cause)%>%
  arrange(FDID,Year)%>%rename('StructureFires'='n')
StructureFiresCNTNA<-StructureFires%>%filter(MUTFLAG==0)%>%
  count(Year, FDID, Cause)%>%rename('StructureFiresNoAid'='n')
#######THIS IS WHERE IS MESSES UP#########

StructureFiresCNT2<-left_join(DeptYearsCauses, StructureFiresCNT, by=c('FDID'='FDID', 'InYear'='Year', 'y'='Cause'))
StructureFiresCNT3<-full_join(DeptYearsCauses, StructureFiresCNT, by=c('FDID'='FDID', 'InYear'='Year', 'y'='Cause'))

StructureFiresCNT2%>%group_by(InYear)%>%summarise(sum(StructureFires, na.rm = T))

StructureFiresCNT<-left_join(StructureFiresCNT, StructureFiresCNTNA, by=c('InYear'='Year', 'FDID'='FDID','y'='Cause'))

#**Fatalities and Injuries
#Note that only incidents were no aid was given are included.
#Select and filter columnns for fatalities and injuries.
StFires<-StructureFires%>%filter(MUTFLAG == 0)%>%
  select(FDID, 
         Year, 
         Cause, 
         FireServiceFatalitiesNoAid = Fire_Service_Fatalities,
         CivilianFatalitiesTFIRSNoAid = Non_Fire_Service_Fatalities,
         FireServiceInjuriesNoAid = Fire_Service_Injuries,
         CivilianInjuriesNoAid = Non_Fire_Service_Injuries)

#create long dataframe (melt) by FDID, year and cause. 
MltStFrs<-melt(StFires, id=c("FDID","Year","Cause"))
MltStFrs$value<-as.numeric(MltStFrs$value)

#cast long dataframe into pivot table. 
StrCast<-dcast(MltStFrs, FDID + Year + Cause ~ variable, sum)

#**Combine Structure Fires & Fatalities and Injuries
StrFrCauses<-left_join(StructureFiresCNT, StrCast
                       , by=c("FDID"="FDID", "InYear"="Year","y"="Cause"))

#rename to match column namese in 2015 access file. 
StrFrCauses<-rename(StrFrCauses, 
                    Name = 'FDNAME',
                    Cause = 'y')
#join filtered fatalities object to our structure fires.

StrFrCauses<-left_join(StrFrCauses, Sum_fatalities, by = c('FDID'='FDID', 'Cause'='Cause','InYear'='Year'))

#turn nulls into zeroes.
StrFrCauses[is.na(StrFrCauses)|StrFrCauses==""]<-0

#in 2016 there is a fire department that has no name in our FD recognition DB
# This removes that department. 
StrFrCauses<-StrFrCauses%>%filter(Name != '0')%>%arrange(FDID, InYear, Cause)



#fatalities$FireDate<-as.character(fatalities$FireDate)
NFIRS$Last_Unit_Cleared_Date___Time<-format(as.POSIXct(NFIRS$Last_Unit_Cleared_Date___Time),'%m/%d/%Y %H:%M:%S')
NFIRS$Arrival_Date___Time<-format(as.POSIXct(NFIRS$Arrival_Date___Time),'%m/%d/%Y %H:%M:%S')
NFIRS$Controlled_Date___Time<-format(as.POSIXct(NFIRS$Controlled_Date___Time),'%m/%d/%Y %H:%M:%S')
NFIRS$Alarm_Date___Time<-format(NFIRS$Alarm_Date___Time,'%m/%d/%Y %H:%M:%S')

varTypes=c(`Alarm Date / Time 2`="datetime",
           `Last Unit Cleared Date / Time 2`="datetime",
           `Controlled Date / Time 2`="datetime",
           `Arrival Date / Time 2`="datetime")

fatalities$FireDate<-format(as.POSIXct(fatalities$FireDate),'%m/%d/%Y')
fatalities$Cause<-as.character(fatalities$Cause)
fatalities$Year<-as.character(fatalities$Year)
fatalities$Fatalities<-as.character(fatalities$Fatalities)
fatalities$FDID<-as.character(fatalities$FDID)

varTypesFat=c(FireDate = "datetime" )

###Write to empty database in file named DBTFIRS2016.accdb####

DBTFIRS<-odbcConnectAccess2007("H:/Fire Prevention/Education and Outreach/FDTN/Data/2018 DB/DBTFIRS2018.accdb")

sqlSave(DBTFIRS, fatalities, tablename = "FireMortality", safer = FALSE, rownames = FALSE, varTypes = varTypesFat)
sqlSave(DBTFIRS, Incident_Summary, tablename = "IncidentSummary", safer = FALSE, rownames = FALSE)
sqlSave(DBTFIRS, StrFrCauses, tablename = "Structure_Fire_Causes" ,safer = FALSE, rownames = FALSE)

odbcCloseAll()

gc()

saveRDS(NFIRS, "./Output/NFIRS_Analyzed.rds")
saveRDS(fatalities, "./Output/fatalities.rds")
saveRDS(Incident_Summary, "./Output/Incident_Summary.rds")
saveRDS(StrFrCauses, "./Output/StrFrCausese.rds")

write.table(NFIRS, "C:/GIS/NFIRS-FDTN/FDTN2.txt", sep = "^", na = "", row.names = FALSE, quote = FALSE)
