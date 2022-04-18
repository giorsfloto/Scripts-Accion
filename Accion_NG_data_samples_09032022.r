### Libraries ###
require(readr)
require(data.table)
require(tidyverse)
library(Hmisc)
require(plyr)
require(bigrquery)
require(readxl)
require(lubridate)
library(googleCloudStorageR)

options(scipen = 999) # to prevent scientific notation
setwd("C:/Users/giorg/OneDrive/Documents/Mia")
UploadBigQuery <- function(data, bqtable, bqdataset = "Test", bqproject = "888318385764", overwrite = FALSE) {
  # The default project is Advans
  message(paste("Upload data to table:", bqtable, "Data set:", bqdataset,  "overwrite =", overwrite))
  if (overwrite == TRUE) {
    message(paste("Deleting table", bqtable, "in", bqdataset))
    tryCatch(delete_table(bqproject, bqdataset, bqtable), 
             error = function(e) {message("Cannot delete table", e)})
  }
  job <- insert_upload_job(bqproject, bqdataset, bqtable, data)
  wait_for(job)
}

### Parameters ###
FoldData   <-'C:/Users/giorg/Dropbox/Rubyx/Accion/Data/'
FoldResult <-'C:/Users/giorg/Dropbox/Rubyx/Accion/Data/Analysis'
options(googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/bigquery",
                                        "https://www.googleapis.com/auth/cloud-platform",
                                        "https://www.googleapis.com/auth/devstorage.full_control",
                                        "https://www.googleapis.com/auth/gmail.compose"))
# the key should be uploaded in directory using upload button of Rstudio file explorer
googleAuthR::gar_auth_service(json_file = "accion-ng-63c0f4c24ac7.json",
                              scope = getOption("googleAuthR.scopes.selected"))
gcs_global_bucket("accion-ng_sandbox")
options("httr_oob_default" = TRUE)
project <- "888318385764" # put your project ID here
bq_auth(path = "accion-ng-63c0f4c24ac7.json")  ### json to allow bigrquery to query project advans-ng
setwd("C:/Users/giorg/Dropbox/Rubyx/Accion")
####### customers
cus <- 'accion-ng_ng_cus_20220303'

gcs_get_object(paste0('cus/',cus,'.csv'), saveToDisk = paste0(FoldData,cus,'.csv'), overwrite=TRUE)

customers <- read_delim(paste0("Data/",cus,".csv"), 
                        ";", escape_double = FALSE, col_types = cols(Customer_ID = col_character(), 
								   Customer_Update_Date = col_date(format = "%Y-%m-%d"), 
                                   Department_ID = col_character(), 
                                   Customer_Inputter_ID = col_character(), 
                                   Customer_Channel_Name = col_character(), 
                                   PortfolioManager_ID = col_character(), 
                                   Customer_Area_Code = col_character(), 
                                   Customer_Creation_Date = col_date(format = "%Y-%m-%d"), 
                                   Gender_Code = col_character(), Customer_Birth_Year = col_integer(), 
                                   Customer_Birth_Month = col_character(), 
                                   Sector_Code = col_character(), Subsector_Code = col_character(), 
                                   Customer_Category_Code = col_character()), 
                             trim_ws = TRUE)

Hmisc::describe(customers)


####### loans
loa <- 'accion-ng_ng_loa_20220303'

gcs_get_object(paste0('loa/',loa,'.csv'), saveToDisk = paste0(FoldData,loa,'.csv'), overwrite=TRUE)

loans <- read_delim(paste0("Data/",loa,".csv"), 
                     ";", escape_double = FALSE, col_types = cols(Loan_ID = col_character(),
					                                              Loan_Update_Date = col_date(format = "%Y-%m-%d"), 
                                                                  Customer_ID = col_character(), Department_ID = col_character(), 
                                                                  Inputter_ID = col_character(), Authoriser_ID = col_character(), 
                                                                  Channel_Name = col_character(), PortfolioManager_ID = col_character(), 
                                                                  Loan_Product_Code = col_character(), 
                                                                  Loan_Start_Date = col_date(format = "%Y-%m-%d"), 
                                                                  Loan_Disbursement_Date = col_date(format = "%Y-%m-%d"), 
                                                                  Loan_End_Date = col_date(format = "%Y-%m-%d"), 
                                                                  Loan_Maturity_Date = col_date(format = "%Y-%m-%d"), 
                                                                  Loan_Purpose_Code = col_character(), 
                                                                  Loan_Process_Code = col_character(), 
                                                                  Instalments_Total_Number = col_integer(), 
                                                                  Loan_Cycle_Number = col_integer(), 
                                                                  Payment_Type_Name = col_character(), 
                                                                  Guarantors_Number = col_integer(), 
                                                                  Guarantors_IDs = col_character()), 
                     trim_ws = TRUE)

Hmisc::describe(loans)

sum(loans$Loan_Start_Date>loans$Loan_Disbursement_Date)/length(loans$Loan_ID) # check if Loan_Start_Date>Loan_Disbursement_Date 0% OK
sum(loans$Loan_Start_Date==loans$Loan_Disbursement_Date)/length(loans$Loan_ID) # check how many Loan_Start_Date=Loan_Disbursement_Date 
sum(loans$Loan_Disbursement_Date<loans$Loan_Maturity_Date)/length(loans$Loan_ID) # check if not 1, KO 
table(is.na(loans$Loan_End_Date),loans$Loan_Status_Name)
table(loans$Loan_End_Date<=loans$Loan_Maturity_Date)
sum(loans$Net_Disbursed_Amount>loans$Capital_Disbursed_Amount)/length(loans$Loan_ID) # check if not 0, KO 
sum(loans$Effective_Interest_Rate<loans$Loan_Interest_Rate)/length(loans$Loan_ID) # check if not 0, KO 


##### repeated loans
count_loans <- ddply(loans,.(Loan_ID),summarise,rows=length(Loan_ID))

####### loan_balances
lba <- 'accion-ng_ng_lba_20220303'

gcs_get_object(paste0('lba/',lba,'.csv'), saveToDisk = paste0(FoldData,lba,'.csv'), overwrite=TRUE)

loan_balance <- read_delim(paste0("Data/",lba,".csv"), 
                           ";", escape_double = FALSE, col_types = cols(Loan_ID = col_character(),Loan_Balance_Date = col_date(format = "%Y-%m-%d"), 
                                                                        Customer_ID = col_character(), Loan_Product_Code = col_character(), 
                                                                        Days_Overdue_Number = col_integer()), 
                           trim_ws = TRUE)

Hmisc::describe(loan_balance)
table(loan_balance$Principal_Outstanding_Amount>0,loan_balance$Loan_Status_Name)
table(loan_balance$Principal_Overdue_Amount>0,loan_balance$Days_Overdue_Number>0)
sum(loan_balance$Principal_Overdue_Amount>loan_balance$Principal_Outstanding_Amount) # check if 0, OK
sum(loan_balance$Interest_Overdue_Amount>loan_balance$Interest_Outstanding_Amount) # check if 0, OK

####### schedules
sch <- 'accion-ng_ng_sch_20220303'

gcs_get_object(paste0('sch/',sch,'.csv'), saveToDisk = paste0(FoldData,sch,'.csv'), overwrite=TRUE)

schedule <- read_delim(paste0("Data/",sch,".csv"), 
                                     ";", escape_double = FALSE, col_types = cols(Department_ID = col_character(), 
                                                                                  Customer_ID = col_character(), Contract_ID = col_character(), 
                                                                                  Contract_Type_Code = col_character(), 
                                                                                  Payment_Frequency_Name = col_character(), 
                                                                                  Instalment_Sequence_Number = col_character(), 
                                                                                  Payment_Due_Date = col_date(format = "%Y-%m-%d"), 
                                                                                  Schedule_Update_Date = col_date(format = "%Y-%m-%d")), 
                                     trim_ws = TRUE)

Hmisc::describe(schedule)

### repeated instalments
count_instalments <- ddply(schedule,.(Contract_ID,Instalment_Sequence_Number),summarise,rows=length(Contract_ID))

repeated_instalments <- subset(count_instalments, count_instalments$rows>1)

repeated_instalments <- merge(repeated_instalments,schedule, all.x=T)

count_instalments2 <- ddply(schedule,.(Contract_ID,Instalment_Sequence_Number,Total_Due_Amount,Instalment_Status_Code),summarise,rows=length(Contract_ID))

sum(count_instalments2$rows>1)

count_contract_dates <- ddply(schedule,.(Contract_ID,Payment_Due_Date,Schedule_Update_Date),summarise,rows=length(Contract_ID))

repeated_contract_dates <- subset(count_contract_dates, rows>1)
sum(schedule$Payment_Due_Date==schedule$Schedule_Update_Date,na.rm=T)/length(schedule$Contract_ID)

sum(is.na(schedule$Schedule_Update_Date))/length(schedule$Contract_ID)
no_update_date <- subset(schedule,is.na(schedule$Schedule_Update_Date))
####### event
eve <- 'accion-ng_ng_eve_20220303'

gcs_get_object(paste0('eve/',eve,'.csv'), saveToDisk = paste0(FoldData,eve,'.csv'), overwrite=TRUE)

event <- read_delim(paste0("Data/",eve,".csv"), 
                                     ";", escape_double = FALSE, col_types = cols(Department_ID = col_character(), 
                                                                                  Customer_ID = col_character(), Contract_ID = col_character(), 
                                                                                  Instalment_Sequence_Number = col_character(), 
                                                                                  Transaction_ID = col_character(), 
                                                                                  Payment_Due_Date = col_date(format = "%Y-%m-%d"), 
                                                                                  Event_DateTime = col_datetime(format = "%Y-%m-%d %H:%M:%S")), 
                                     trim_ws = TRUE)

Hmisc::describe(event)

table(event$Transaction_ID==event$Instalment_Sequence_Number) ## Transaction_ID=Instalment_Sequence_Number

##### check doubles  
count_transactions <- ddply(event,.(Transaction_ID),summarise,rows=length(Transaction_ID))

double_transactions <- subset(count_transactions, count_transactions$rows>1)
transactions_repeated <- merge(double_transactions,event, all.x=T)

#write.csv(double_transactions,"Data/double_transactions_2.csv",row.names = F)

count_transactions_instalments <- ddply(event,.(Transaction_ID,Instalment_Sequence_Number),summarise,rows=length(Transaction_ID))

sum(count_transactions_instalments$rows>1) ### transaction/Instalment_Sequence_Number are unique

event$Event_Date <- as.Date(event$Event_DateTime)

#### check Event_Code
table(event$Event_Code,as.Date(event$Event_Date)>as.Date(event$Payment_Due_Date))
table(event$Event_Code,as.Date(event$Event_Date)==as.Date(event$Payment_Due_Date))
table(event$Event_Code,as.Date(event$Event_Date)<as.Date(event$Payment_Due_Date))
sum(event$Event_Date==event$Payment_Due_Date)/length(event$Contract_ID)

sum(event$Total_Paid_Amount!=round((event$Capital_Paid_Amount+event$Interest_Paid_Amount+event$Penalty_Paid_Amount),2))
####### transaction
txn <- 'accion-ng_ng_txn_20220303'

gcs_get_object(paste0('txn/',txn,'.csv'), saveToDisk = paste0(FoldData,txn,'.csv'), overwrite=TRUE)

transaction <- read_delim(paste0("Data/",txn,".csv"),
                          ";", escape_double = FALSE, col_types = cols(Department_ID = col_character(), 
                                                                       Customer_ID = col_character(), Transaction_ID = col_character(), 
                                                                       Operator_ID = col_character(), Terminal_ID = col_character(), 
                                                                       Counterpart_ID = col_character(), 
                                                                       Counterpart_Account_ID = col_character(), 
                                                                       Transaction_Type_Code = col_character(), 
                                                                       Transaction_DateTime = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                                       Value_Date = col_date(format = "%Y-%m-%d"), 
                                                                       Transaction_Fee_Amount = col_double(), 
                                                                       Transaction_Commission_Amount = col_double(), 
                                                                       Transaction_Latitude = col_double(), 
                                                                       Transaction_Longitude = col_double(), 
                                                                       Float_AccountBalance_Amount = col_double(), 
                                                                       Contract_ID = col_character(),
                                                                       Instalment_Sequence_Number = col_integer()), 
                          trim_ws = TRUE)


Hmisc::describe(transaction)

####### portfolio_manager
pfm <- 'accion-ng_ng_pfm_20220309'

gcs_get_object(paste0('pfm/',pfm,'.csv'), saveToDisk = paste0(FoldData,pfm,'.csv'), overwrite=TRUE)

portfolio_manager <- read_delim(paste0("Data/",pfm,".csv"), 
                                ";", escape_double = FALSE, col_types = cols(Department_ID = col_character(), 
                                                                             PortfolioManager_ID = col_character(), 
                                                                             Customer_ID = col_character(), PortfolioManager_Update_Date = col_date(format = "%Y-%m-%d"), 
                                                                             PortfolioManager_Area_Code = col_character(), 
                                                                             PortfolioManager_Name = col_character()), 
                                trim_ws = TRUE)

Hmisc::describe(portfolio_manager)

####### dictionary
dic <- 'advans-ng_dic_20210228'

gcs_get_object(paste0('dic/',dic,'.csv'), saveToDisk = paste0(FoldData,dic,'.csv'), overwrite=TRUE)

dictionary <- read_delim(paste0("Data/",dic,".csv"),  
                         ";", escape_double = FALSE, col_types = cols(Field_Code = col_character(), 
                                                                      Parent_Field_Name = col_character(), 
                                                                      Parent_Field_Code = col_character(), 
                                                                      Dictionary_Update_Date = col_datetime(format = "%Y-%m-%d %H:%M:%S")), 
                         trim_ws = TRUE)



Hmisc::describe(dictionary)

check_doubles <- ddply(dictionary,.(Field_Name, Field_Code),summarise,rows=length(Field_Name))
doubles <- subset(check_doubles,check_doubles$rows>1)
doubles <- merge(doubles,dictionary,all.x=T)
