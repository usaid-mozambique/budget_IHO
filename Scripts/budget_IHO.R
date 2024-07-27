required_packages <- c("dplyr","purrr", "tidyr","stringr", "janitor", "lubridate", "glamr",
                       "readxl", "readr")

missing_packages <- required_packages[!(required_packages %in% installed.packages()
                                        [,"Package"])]

if(length(missing_packages) >0){
    install.packages(missing_packages, repos = c("https://usaid-oha-si.r-universe.dev",
                                                 "https://cloud.r-project.org"))
}

options(scipen=999) 

library(janitor)
library(lubridate)
library(glamr)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(readxl)
library(readr)


# OTHER SETUP  - only run one-time --------------------------------------

folder_setup() 
folder_setup(folder_list = list("Data/subobligation_summary", 
                                "Data/active_awards", 
                                "Data/phoenix_transactions", 
                                "Data/phoenix_pipeline"))


# This should match the existing folder structure
SUBOBLIGATION_SUMMARY_FOLDER_PATH <-  "Data/subobligation_summary/"
ACTIVE_AWARDS_FOLDER_PATH <- "Data/active_awards/"
PHOENIX_TRANSACTION_FOLDER_PATH <- "Data/phoenix_transactions/"
PHOENIX_PIPELINE_FOLDER_PATH <- "Data/phoenix_pipeline/"

#FILTERS ------------------------------------------------
EVENT_TYPE_FILTER <- c("OBLG_UNI", "OBLG_SUBOB")
DISTRIBUTION_FILTER <- c("656-M", "656-GH-M", "656-W", "656-GH-W")
REMOVE_AWARDS <- c("MEL", "FORTE")

#READ ALL FUNCTIONS ------------------------------------'

source("Scripts/utilities.R")
source("Scripts/tests.R")

#FUNCTIONS----------------------------------------
create_active_awards_df <- function(ACTIVE_AWARDS_PATH){
    
    active_awards_input_file <- dir(ACTIVE_AWARDS_PATH,
                                    full.name = TRUE,
                                    pattern = "*.xlsx")
    
    active_awards_df <- map(active_awards_input_file, create_active_awards) |> 
        bind_rows() |> 
    filter(!str_detect(activity_name, paste(REMOVE_AWARDS, collapse = "|"))) 
}

create_sub_obligation_df <- function(SUBOBLIGATION_SUMMARY_PATH){
    
    sub_obligation_input_file <- dir(SUBOBLIGATION_SUMMARY_PATH, 
                                     full.name = TRUE, 
                                     pattern = "*.xlsx")
    
    subobligation_summary_df <- map(sub_obligation_input_file, create_subobligation_summary) |> 
        bind_rows()
}

create_phoenix_pipeline_df <- function(PHOENIX_PIPELINE_PATH){
    
    phoenix_pipeline_input_file <- dir(PHOENIX_PIPELINE_PATH,
                                       full.name = TRUE,
                                       pattern = "*.xlsx")
    
    phoenix_pipeline_df <- map(phoenix_pipeline_input_file, 
                               ~create_phoenix_pipeline(.x, active_award_number, 
                                                        EVENT_TYPE_FILTER,
                                                        DISTRIBUTION_FILTER)) |>
        bind_rows()
}

create_phoenix_transaction_df <- function(PHOENIX_TRANSACTION_PATH, active_award_number){
    
    phoenix_transaction_input_file <- dir(PHOENIX_TRANSACTION_FOLDER_PATH,
                                         full.name = TRUE,
                                         pattern = "*.xlsx")
    
    phoenix_transaction_df <- map(phoenix_transaction_input_file, 
                                  ~create_phoenix_transaction(.x, active_award_number,
                                                              DISTRIBUTION_FILTER)) |> 
        bind_rows() |> 
        group_by(award_number, period, program_area, fiscal_year, quarter) |>
        summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), .groups = "drop")
    
    phoenix_transaction_disbursement_fy <- phoenix_transaction_df |> 
        select(award_number, fiscal_year, quarter, program_area, transaction_disbursement, period) |>
        arrange(award_number, fiscal_year, quarter)  |> 
        group_by(award_number, fiscal_year, program_area) |> 
        mutate(cumulative_transaction_disbursement_fy = cumsum(transaction_disbursement))  |> 
        ungroup() |> 
        select(-c(fiscal_year, quarter, transaction_disbursement))
    
    phoenix_transaction_df <- phoenix_transaction_df |>
        left_join(phoenix_transaction_disbursement_fy, by = c("award_number", "period", "program_area")) |> 
        select(-c(fiscal_year, quarter))
}

create_pipeline_dataset<- function(){
    
    active_awards_one_row <- active_awards_df |> 
        left_join(phoenix_pipeline_df, by = c("award_number", "period")) |> 
        left_join(subobligation_summary_df, by = c("award_number", "period", "program_area")) |> 
        left_join(phoenix_transaction_df, by = c("award_number", "period", "program_area")) |> 
        mutate(across(where(is.numeric), ~ replace_na(., 0))) |> 
        mutate(program_area_name = case_when(program_area == "HL.1" ~ "HIV/AIDS",
                                             program_area == "HL.6" ~ "MCH",
                                             program_area == "HL.4"~ "GHS" ,
                                             program_area == "HL.9"~ "Nutrition",
                                             program_area == "HL.7"~  "FP/RH",
                                             program_area == "HL.2" ~ "TB",
                                             program_area == "HL.3" ~ "Malaria",
                                             program_area == "HL.8" ~ "WASH",
                                             program_area == "PO.1"~ "PD&L",
                                             program_area == "DR.4" ~ "Civil Society",
                                             program_area == "DR.6" ~ "Human Rights",
                                             program_area == "DR.3" ~ "Political Competition and Consensus-Building",
                                             TRUE ~ as.character(program_area)    
                                             
        ))  
    
    temp_comments <- active_awards_one_row |> 
        select(award_number, period, comments) |> 
        drop_na(comments) |>
        distinct()
    
    active_awards_one_row <- active_awards_one_row |>
        select(-comments) |> 
        left_join(temp_comments, by = c("award_number", "period"))
    
    return(active_awards_one_row)
    
}

create_transaction_dataset <- function() {
    #latest active awards with accrual amount
    active_awards_accrual_latest <- pipeline_dataset |> 
        filter(period == max(period)) |> 
        group_by(award_number, period, program_area) |> 
        summarise(last_qtr_accrual_amt = sum(last_qtr_accrual_amt, na.rm = TRUE), .groups = "drop")
    
    active_awards_one_row_transaction <- active_awards_df |> 
         select(award_number, activity_name) |> 
        distinct() |> #needed as there are multiple lines due to period
        left_join(phoenix_transaction_df, by = "award_number") |> 
        select(award_number, activity_name, transaction_disbursement,
               transaction_obligation, transaction_amt, avg_monthly_exp_rate, 
               period, program_area) |> 
        left_join(active_awards_accrual_latest, by = c("award_number", "period", "program_area")) |> 
        mutate(across(where(is.numeric), ~ replace_na(., 0)))  |> 
        mutate(program_area_name = case_when(program_area == "HL.1" ~ "HIV/AIDS",
                                             program_area == "HL.6" ~ "MCH",
                                             program_area == "HL.4"~ "GHS" ,
                                             program_area == "HL.9"~ "Nutrition",
                                             program_area == "HL.7"~  "FP/RH",
                                             program_area == "HL.2" ~ "TB",
                                             program_area == "HL.3" ~ "Malaria",
                                             program_area == "HL.8" ~ "WASH",
                                             program_area == "PO.1"~ "PD&L",
                                             program_area == "DR.4" ~ "Civil Society",
                                             program_area == "DR.6" ~ "Human Rights",
                                             program_area == "DR.3" ~ "Political Competition and Consensus-Building",
                                             TRUE ~ as.character(program_area)    
                                             
        )) 
    
}

#READ DATA----------------------------------------
active_awards_df <- create_active_awards_df(ACTIVE_AWARDS_FOLDER_PATH) 
#all active award IDs
active_award_number <- active_awards_df |> 
    select(award_number) |> 
    distinct() |> 
    pull()

subobligation_summary_df <- create_sub_obligation_df(SUBOBLIGATION_SUMMARY_FOLDER_PATH)
phoenix_pipeline_df <- create_phoenix_pipeline_df(PHOENIX_PIPELINE_FOLDER_PATH)
phoenix_transaction_df <- create_phoenix_transaction_df(PHOENIX_TRANSACTION_FOLDER_PATH, active_award_number)


# CREATE PIPELINE DATASET (one row per award, per quarter per program area name)

pipeline_dataset <- create_pipeline_dataset()
write_csv(pipeline_dataset,"Dataout/pipeline.csv")

# CREATE TRANSACTION DATASET (one row per award, per transaction per program area)
transaction_dataset <- create_transaction_dataset()
write_csv(transaction_dataset, "Dataout/transaction.csv")

#RUN TESTS ------------------------------------------------------------------

test_award_exists <- test_awards(active_awards_df, subobligation_summary_df, 
                            phoenix_pipeline_df, phoenix_transaction_df)
write_csv(test_award_exists, "Dataout/awards_exist.csv")

# shows missing program areas - needed to bring all data over
test_program_area_exists <- test_missing_program_area(active_awards_df, subobligation_summary_df)
write_csv(test_program_area_exists, "Dataout/program_area_missing.csv")


# program area missing 
test_program_area_all <- test_program_area(active_awards_df, subobligation_summary_df, 
                                       phoenix_transaction_df, phoenix_pipeline_df, active_award_number)
write_csv(test_program_area_all, "Dataout/program_area_all_datasets.csv")


# Data not included in Tableau - but shows if anything has been assigned to the program area PO.2
test_po_2_exists <- test_po_2(active_awards_df, phoenix_pipeline_df, phoenix_transaction_df, active_award_number)
write_csv(test_po_2_exists, "Dataout/po_2_test.csv")


#checking numbers in raw data

test_raw_pipeline <- test_pipeline_number(phoenix_pipeline_df, active_awards_df, pipeline_dataset)
write_csv(test_raw_pipeline, "Dataout/pipeline_test_all.csv")


       

    
