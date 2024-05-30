required_packages <- c("tidyverse", "janitor", "lubridate", "glamr")

missing_packages <- required_packages[!(required_packages %in% installed.packages()
                                        [,"Package"])]

if(length(missing_packages) >0){
    install.packages(missing_packages, repos = c("https://usaid-oha-si.r-universe.dev",
                                                 "https://cloud.r-project.org"))
}


library(tidyverse)
library(janitor)
library(lubridate)
library(glamr)

# OTHER SETUP--------------------------------------

folder_setup()  #only run one-time


#PATHS---------------------------------------------

SUBOBLIGATION_SUMMARY_FOLDER_PATH <-  "Data/subobligation_summary/"
ACTIVE_AWARDS_FOLDER_PATH <- "Data/active_awards/"
PHOENIX_TRANSACTION_FOLDER_PATH <- "Data/phoenix_transactions/"
PHOENIX_PIPELINE_FOLDER_PATH <- "Data/phoenix_pipeline/"



#FILTERS------------------------------------------------
PROGRAM_AREA_FILTER <- c("A11", "A26", "EG.3", "EG.10", "HL.1",
                         "HL.2", "HL.3", "HL.4","HL.5", "HL.6", "HL.7",
                         "HL.8", "HL.9", "PO.1", "DR.3", "DR.4", "DR.6")


DISTRIBUTION_FILTER <- c("656-GH-M", "656-GH-W", "656-W", "656-M")


OBLIGATION_TYPE_FILTER <-  c("OBLG_SUBOB", "OBLG_UNI")

PROGRAM_SUB_ELEMENT_FILTER <- c("A0158", "A0159", "A0161", "A0162",
                                "A0163", "A0165", "A0166", "A0168",
                                "A0170", "A0171", "A0187", "A0188",
                                "A0189", "A0190", "A0191", "A0192", 
                                "A0194", "A0204", "A0206", "A0207",
                                "A0210", "A0211", "A0212", "A0213",
                                "A0214", "A0215", "A0216", "A0217",
                                "A0218", "A0219", "A0220", "A0221",
                                "A0222", "A0223", "A0224", "A0225",
                                "A0226", "A0227", "A0228", "A0229",
                                "A0238", "A0239", "A0240", "A0244", 
                                "A0245", "A0410", "A0417", "A0419",
                                "A0420", "PO.1.1.2", "PO.1.1.6")



#READ ALL FUNCTIONS ------------------------------------'

source("Scripts/utilities.R")

#READ DATA----------------------------------------

#Active Awards
active_awards_input_file <- dir(ACTIVE_AWARDS_FOLDER_PATH,
                                full.name = TRUE,
                                pattern = "*.xlsx")

active_awards_df <- map(active_awards_input_file, create_active_awards) %>% 
    bind_rows()


#Subobligation Summary
sub_obligation_input_file <- dir(SUBOBLIGATION_SUMMARY_FOLDER_PATH, 
                                 full.name = TRUE, 
                                 pattern = "*.xlsx")

subobligation_summary_df <- map(sub_obligation_input_file, create_subobligation_summary) %>% 
    bind_rows()


#List of all active award numbers
#all active award IDs
active_award_number <- active_awards_df %>% 
    select(award_number) %>% 
    distinct() %>% 
    pull()


# Phoenix - pipeline
phoenix_pipeline_input_file <- dir(PHOENIX_PIPELINE_FOLDER_PATH,
                                   full.name = TRUE,
                                   pattern = "*.xlsx")

phoenix_pipeline_df <- map(phoenix_pipeline_input_file, 
                           ~create_phoenix_pipeline(.x, active_award_number)) %>%
    bind_rows()


# Phoenix - transaction
phoenix_transaction_input_file <- dir(PHOENIX_TRANSACTION_FOLDER_PATH,
                                      full.name = TRUE,
                                      pattern = "*.xlsx")


phoenix_transaction_df <- map(phoenix_transaction_input_file, 
                              ~create_phoenix_transaction(.x, active_award_number)) %>% 
    bind_rows() 


# CREATE PIPELINE DATASET (one row per award, per quarter per program area name)



