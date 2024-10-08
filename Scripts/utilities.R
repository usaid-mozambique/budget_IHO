#' Title
#'
#' @param ACTIVE_AWARDS_PATH 
#'
#' @return
#' @export
#'
#' @examples
#' 
create_active_awards <- function(ACTIVE_AWARDS_PATH){
    
    
    temp <- readxl::read_xlsx(ACTIVE_AWARDS_PATH,
                              sheet = "Active Awards", 
                              skip = 1) |> 
        clean_names() |> 
        filter(sector == "IHO") |> 
        mutate(award_number = str_trim(award_number),
               filename = basename(ACTIVE_AWARDS_PATH),
               period = str_extract(filename, "^[^_]+"),
               sub_sector = recode(sub_sector, "AFG" = "HTHS",
                                   "PCMD" = "Family Health", 
                                   "FAMILY HEALTH" = "Family Health"),
               u_s_org_local = recode(u_s_org_local, "International" = "US/Int'l",
                                      "US" = "US/Int'l")
        ) |> 
        mutate_if(is.numeric, ~replace_na(., 0)
                  ) |> 
        select(sub_sector, activity_name, award_number, total_estimated_cost,
               start_date, end_date, u_s_org_local, aor_cor_or_activity_manager,
               period, funding_type, pepfar_funding) |> 
        drop_na(award_number) 
    
    return(temp)
}

#' Title
#'
#' @param ACTIVE_AWARDS_PATH 
#' @param CLOSE_OUT_TRACKER_PATH 
#'
#' @return
#' @export
#'
#' @examples
create_expired_awards <- function(ACTIVE_AWARDS_PATH){
  
  temp <- readxl::read_xlsx(ACTIVE_AWARDS_PATH,
                            sheet = "Expired Awards", 
                            skip = 1,
  ) |> 
    clean_names() |> 
    filter(sector == "IHO") |> 
    mutate(award_number = str_trim(award_number),
           filename = basename(ACTIVE_AWARDS_PATH),
           period = str_extract(filename, "^[^_]+"),
           sub_sector = recode(sub_sector, "AFG" = "HTHS",
                               "PCMD" = "Family Health", 
                               "FAMILY HEALTH" = "Family Health"),
           u_s_org_local = recode(u_s_org_local, "International" = "US/Int'l",
                                  "US" = "US/Int'l"),
           
    ) |> 
    select(sector, sub_sector, activity_name, award_number,
           end_date, u_s_org_local, aor_cor_or_activity_manager,
           period, funding_type) |> 
    drop_na(award_number) 
  
  return(temp)

  
}

#' Title
#'
#' @param CLOSE_OUT_TRACKER_PATH 
#'
#' @return
#' @export
#'
#' @examples
create_close_out_tracker <- function(CLOSE_OUT_TRACKER_PATH){
  
  temp <- readxl::read_xlsx(CLOSE_OUT_TRACKER_PATH,
                            sheet = "IHO- Priority", 
                            skip = 1) |> 
    clean_names() |> 
    mutate(award_number = str_trim(award_number),
           filename = basename(CLOSE_OUT_TRACKER_PATH),
           period = str_extract(filename, "^[^_]+"),
    ) |> 
    select(award_number, to_be_deobligated, status, period) |> 
    drop_na(award_number) 
  
  return(temp)
}


#' Title
#'
#' @param SUBOBLIGATION_SUMMARY_PATH 
#'
#' @return
#' @export
#'
#' @examples
create_subobligation_summary <- function(SUBOBLIGATION_SUMMARY_PATH){
    
  
  lookup <- c("unliquidated_obligations_beg_fy" = "unliquidated_obligations_at_the_beginning_of_the_fy")
  
  temp <- readxl::read_xlsx(SUBOBLIGATION_SUMMARY_PATH,
                              sheet = "Sheet1", 
                              skip = 1) |> 
        mutate(filename = basename(SUBOBLIGATION_SUMMARY_PATH)) |> 
        clean_names() |> 
        rename_with(~lookup[.x], any_of(names(lookup))) |> 
        mutate(award_number = str_trim(award_number),
               period = str_extract(filename, "^[^_]+")
               
        ) |> 
        filter(implementing_mechanism != "Total") |>
        select(-c(filename, implementing_mechanism)) |> 
        drop_na(award_number) |> 
        rename("program_area_name" = "program_area") |> 
        mutate(program_area = case_when(program_area_name == "HIV/AIDS" ~ "HL.1",
                                        program_area_name == "MCH" ~ "HL.6",
                                        program_area_name == "GHS" ~ "HL.4",
                                        program_area_name == "Nutrition" ~ "HL.9",
                                        program_area_name == "FP/RH" ~ "HL.7",
                                        program_area_name == "TB" ~ "HL.2",
                                        program_area_name == "Malaria" ~ "HL.3",
                                        program_area_name == "WASH" ~ "HL.8",
                                        program_area_name == "PD&L" ~ "PO.1",
                                        program_area_name == "Civil Society" ~ "DR.4",
                                        program_area_name == "Human Rights" ~ "DR.6",
                                        program_area_name == "Political Competition and Consensus-Building" ~ "DR.3",
        )) |> 

        mutate(
            planned_subobligations_for_the_next_months = as.numeric(planned_subobligations_for_the_next_months),
            total_obligations_this_fy = as.numeric(total_obligations_this_fy),
            projected_monthly_burn_rate = as.numeric(projected_monthly_burn_rate),
            un_sub_obligated_funds = as.numeric(un_sub_obligated_funds),
            planned_sub_oblig_date = ymd(planned_sub_oblig_date),
            approved_budget_cop_op = as.numeric(approved_budget_cop_op),
            unliquidated_obligations_at_the_beginning_of_the_fy = as.numeric(unliquidated_obligations_at_the_beginning_of_the_fy),
            across(where(is.numeric), ~ ifelse(is.na(.), NA_real_, .)),
            
        )  |> 
        mutate_if(is.numeric, ~replace_na(., 0)) |> 
        select(-program_area_name)
    
    return(temp)
}


#' Title
#'
#' @param PHOENIX_PIPLINE_PATH 
#' @param active_award_number 
#'
#' @return
#' @export
#'
#' @examples
create_phoenix_pipeline <- function(PHOENIX_PIPELINE_PATH, all_award_number, obligation_type_filter, distribution_filter){
    
 lookup <- c("last_qtr_adj_amt" = "last_qtr_accrual_amt")
    
  
  temp <- readxl::read_xlsx(PHOENIX_PIPELINE_PATH,
                              col_types = "text") |> 
        clean_names() |> 
        rename_with(~lookup[.x], any_of(names(lookup))) |> 
        filter(obligation_type %in% obligation_type_filter,
               distribution %in% distribution_filter) |>
        select(document_amt, 
               obligation_amt, subobligation_amt, disbursement_amt, 
               undisbursed_amt, last_qtr_accrual_amt,document_number,
               program_area, award_number, program_element,pipeline_amt,
               bilateral_obl_number
        ) |> 
        
        mutate(filename = basename(PHOENIX_PIPELINE_PATH),
               period = str_extract(filename,"^[^_]+" ),
               document_amt = as.numeric(document_amt),
               disbursement_amt = as.numeric(disbursement_amt),
               undisbursed_amt = as.numeric(undisbursed_amt),
               last_qtr_accrual_amt = as.numeric(last_qtr_accrual_amt),
               pipeline_amt = as.numeric(pipeline_amt),
               program_area = case_when(program_element == "A047" ~ "HL.1", 
                                            program_element == "A048"~ "HL.2",
                                            program_element == "A049"~ "HL.3",
                                            program_element == "A050"~ "HL.4",
                                            program_element == "A051"~ "HL.5",
                                            program_element == "A052"~ "HL.6",
                                            program_element == "A053"~ "HL.7",
                                            program_element == "A054"~ "HL.8",
                                            program_element == "A142"~ "HL.9",
                                            program_element == "A141"~ "PO.2",
                                            program_element == "A140"~ "PO.1",
                                            TRUE ~ program_area),
               award_number = case_when(
                   award_number %in% all_award_number ~ award_number,
                   TRUE ~ document_number) ,
               total_disbursement_outlays = disbursement_amt + last_qtr_accrual_amt
               
        ) |> 
        
        filter(award_number %in% all_award_number) |>
        group_by(award_number, period, program_area, bilateral_obl_number) |>
        summarise(across(where(is.numeric), sum), .groups = "drop")
    
    return(temp)
}





#' Title
#'
#' @param PHOENIX_TRANSACTION_PATH 
#'
#' @return
#' @export
#'
#' @examples
create_phoenix_transaction <- function(PHOENIX_TRANSACTION_PATH, all_award_number, distribution_filter){
    
    temp <- readxl::read_xlsx(PHOENIX_TRANSACTION_PATH,
                              col_types = "text") |> 
        clean_names() |>
        select(award_number, document_number, obl_document_number,
               transaction_date, transaction_amt, transaction_event,
               transaction_event_type, program_element, distribution,
               program_area) |>
        filter(distribution %in% distribution_filter) |>
        mutate(
            transaction_amt = as.numeric(transaction_amt),
            transaction_date = as_date(as.numeric(transaction_date) - 1, origin = "1899-12-30"),
            transaction_date = floor_date(transaction_date, unit = "quarter"),
          #  count = nrows(PHOENIX_TRANSACTION_PATH),
            program_area = case_when(program_element == "A047" ~ "HL.1", 
                                         program_element == "A048" ~ "HL.2",
                                         program_element == "A049" ~ "HL.3",
                                         program_element == "A050" ~ "HL.4",
                                         program_element == "A051" ~ "HL.5",
                                         program_element == "A052" ~ "HL.6",
                                         program_element == "A053" ~ "HL.7",
                                         program_element == "A054" ~ "HL.8",
                                         program_element == "A142" ~ "HL.9",
                                         program_element == "A141" ~ "PO.2",
                                         program_element == "A140" ~ "PO.1",
                                         TRUE ~ program_area),
            
                award_number = case_when(
                award_number %in% active_award_number ~ award_number,
                document_number %in% active_award_number ~ document_number,
                TRUE ~ obl_document_number ),
            transaction_disbursement = case_when(transaction_event == "DISB" ~ transaction_amt,
                                                 .default = NA_real_),
            transaction_obligation = case_when(transaction_event_type == "OBLG_SUBOB" ~ transaction_amt,
                                               transaction_event_type == "OBLG_UNI" ~ transaction_amt,
                                               .default = NA_real_),
            avg_monthly_exp_rate = transaction_disbursement/3
        ) |> 
        filter(award_number %in% active_award_number) |> 
        select(-c(program_element, transaction_event_type, transaction_event,
                                    document_number, obl_document_number,
                  )) |> 
    
        #create at month level and add a mutate column with a 1. 
    
    #    temp_2 <- temp |> 
    #      select(award_number, transaction_date, transaction_disbursement) |>
    #      drop_na(transaction_disbursement) |> 
    #      filter(transaction_disbursement != 0) |>
    #      mutate(month_level = transaction_date %m+% months(3),
    #             count = 1) |>
    #      group_by(award_number, month_level) |>
    
    
        mutate(fiscal_transaction_date = transaction_date %m+% months(3),
             period = paste0("FY", year(fiscal_transaction_date) %% 100, 
                             "Q", quarter(fiscal_transaction_date))) |> 
        group_by(award_number, period, program_area) |> 
        summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), .groups = "drop") |>

        separate(period, into = c("fiscal_year", "quarter"), sep = "Q", convert = TRUE, remove = FALSE) |> 
        mutate(
            fiscal_year = as.numeric(str_sub(fiscal_year, 3, 4)) + 2000,
            quarter = as.numeric(quarter)
        ) |>
        group_by(award_number, fiscal_year, quarter, period, program_area) |>
        summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), .groups = "drop") 


    return(temp)
}


vars_new_doag <- c(
  "656-DOAG-656-22-020-DRG",
  "656-DOAG-656-22-019-EDU",
  "656-DOAG-656-22-019-IH",
  "656-DOAG-656-22-021-NUT",
  "656-DOAG-656-22-020-EG",
  "656-DOAG-656-22-021-ENV",
  "656-DOAG-656-22-021-WASH"
)




