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
                              sheet = "Active Awards") |> 
        clean_names() |> 
        filter(sector == "IHO") |> 
        mutate(award_number = str_trim(award_number),
               filename = basename(ACTIVE_AWARDS_PATH),
               period = str_extract(filename, "^[^_]+")
        ) |> 
        mutate_if(is.numeric, ~replace_na(., 0)) |> 
        select(sub_sector, activity_name, award_number, total_estimated_cost,
               start_date, end_date, u_s_org_local, aor_cor_or_activity_manager,
               period, funding_type, pepfar_funding) |> 
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
    temp <- readxl::read_xlsx(SUBOBLIGATION_SUMMARY_PATH,
                              sheet = "Sheet1", 
                              skip = 1) |> 
        mutate(filename = basename(SUBOBLIGATION_SUMMARY_PATH)) |> 
        clean_names() |> 
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
              opu_reprogram = as.numeric(opu_reprogram),
               planned_subobligations_for_the_next_months = as.numeric(planned_subobligations_for_the_next_months),
               total_obligations_this_fy = as.numeric(total_obligations_this_fy),
               projected_monthly_burn_rate = as.numeric(projected_monthly_burn_rate),
               un_sub_obligated_funds = as.numeric(un_sub_obligated_funds),
               planned_sub_oblig_date = ymd(planned_sub_oblig_date),
               approved_budget_cop_op = as.numeric(approved_budget_cop_op),
               unliquidated_obligations_at_the_beginning_of_the_fy = as.numeric(unliquidated_obligations_at_the_beginning_of_the_fy),
               across(where(is.numeric), ~ ifelse(is.na(.), NA_real_, .))
               )  |> 
        mutate_if(is.numeric, ~replace_na(., 0)) 
    
    temp_comments <- temp |> 
        select(award_number, period, comments) |> 
        drop_na(comments)
    
    temp <- temp |>
        select(-comments) |> 
        left_join(temp_comments, by = c("award_number", "period"))
    
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
create_phoenix_pipeline <- function(PHOENIX_PIPELINE_PATH, active_award_number){
    
    temp <- readxl::read_xlsx(PHOENIX_PIPELINE_PATH,
                              col_types = "text") |> 
        clean_names() |> 
        filter(distribution %in% DISTRIBUTION_FILTER,
               obligation_type %in% OBLIGATION_TYPE_FILTER,
               program_area %in% PROGRAM_AREA_FILTER,
               is.na(program_sub_element) | program_sub_element %in% PROGRAM_SUB_ELEMENT_FILTER
        ) |> 
        filter(str_detect(bfy_fund, "DV") |
                   str_detect(bfy_fund, "ES") |
                   str_detect(bfy_fund, "GH") |
                   str_detect(bfy_fund, "HV")) |> 
        
        select(program_element, bfy_fund, obligation_type, document_amt, 
               obligation_amt, subobligation_amt, disbursement_amt, 
               undisbursed_amt, last_qtr_accrual_amt,document_number,
               program_area, distribution, award_number, program_sub_element,
               program_element
        ) |> 
        
        mutate(filename = basename(PHOENIX_PIPELINE_PATH),
               period = str_extract(filename,"^[^_]+" ),
               document_amt = as.numeric(document_amt),
               disbursement_amt = as.numeric(disbursement_amt),
               undisbursed_amt = as.numeric(undisbursed_amt),
               last_qtr_accrual_amt = as.numeric(last_qtr_accrual_amt),
               program_area_new = case_when(program_element == "A047" ~ "HL.1", 
                                            program_element == "A048"~ "HL.2",
                                            program_element == "A049"~ "HL.3",
                                            program_element == "A050"~ "HL.4",
                                            program_element == "A051"~ "HL.5",
                                            program_element == "A052"~ "HL.6",
                                            program_element == "A053"~ "HL.7",
                                            program_element == "A054"~ "HL.8",
                                            program_element == "A142"~ "HL.9",
                                            program_element == "A026"~ "PO.1",
                                            program_element == "A027"~ "DR.2",
                                            program_element == "A140"~ "PO.1",
                                            TRUE ~ program_area)
                   
               
               ) |> 

        
        group_by(award_number,  bfy_fund, obligation_type,document_number,
                 program_area_new, distribution, period) |> 
        summarise(across(where(is.numeric), sum), .groups = "drop") |> 
        
        mutate(total_disbursement_outlays = disbursement_amt + last_qtr_accrual_amt) |> 
        select(-c( bfy_fund, obligation_type, distribution)) |> 
        mutate(award_number = case_when(
            award_number %in% active_award_number ~ award_number,
            TRUE ~ document_number)
        ) |> 
        select(-document_number) |>
        rename(program_area = program_area_new) |> 
        group_by(award_number, period, program_area) |>
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
create_phoenix_transaction <- function(PHOENIX_TRANSACTION_PATH, active_award_number){
    
    
    
    temp <- readxl::read_xlsx(PHOENIX_TRANSACTION_PATH,
                              col_types = "text") |> 
        clean_names() |> 
        filter(distribution %in% DISTRIBUTION_FILTER,
               program_area %in% PROGRAM_AREA_FILTER,
               is.na(program_sub_element) | program_sub_element %in% PROGRAM_SUB_ELEMENT_FILTER  
        ) |> 
        filter(str_detect(bfy_fund, "DV") |
                   str_detect(bfy_fund, "ES") |
                   str_detect(bfy_fund, "GH") |
                   str_detect(bfy_fund, "HV")) |> 
        mutate(
            transaction_amt = as.numeric(transaction_amt),
            transaction_date = as_date(as.numeric(transaction_date) - 1, origin = "1899-12-30"),
            transaction_date = floor_date(transaction_date, unit = "quarter"),
            fiscal_transaction_date = transaction_date %m+% months(3),
            period = paste0("FY", year(fiscal_transaction_date) %% 100, 
                            "Q", quarter(fiscal_transaction_date)),
            program_area_new = case_when(program_element == "A047" ~ "HL.1", 
                                         program_element == "A048" ~ "HL.2",
                                         program_element == "A049" ~ "HL.3",
                                         program_element == "A050" ~ "HL.4",
                                         program_element == "A051" ~ "HL.5",
                                         program_element == "A052" ~ "HL.6",
                                         program_element == "A053" ~ "HL.7",
                                         program_element == "A054" ~ "HL.8",
                                         program_element == "A142" ~ "HL.9",
                                         program_element == "A026" ~ "PO.1",
                                         program_element == "A027" ~ "DR.2",
                                         program_element == "A140" ~ "PO.1",
                                         TRUE ~ program_area)
            
        ) |> 
        
        select(award_number, distribution, bfy_fund,
               transaction_event, transaction_amt, period, transaction_date, document_number, 
               obl_document_number, transaction_event_type,  program_area_new) |> 
        group_by(award_number, distribution,  bfy_fund,
                 transaction_event, transaction_event_type, period, transaction_date, document_number, obl_document_number,
                  program_area_new) |> 
        summarise(transaction_amt = sum(transaction_amt, na.rm = TRUE), .groups = "drop") |> 
        select(award_number, period, transaction_date, transaction_amt, transaction_event, 
               document_number, obl_document_number,   program_area_new,
               transaction_event_type) |> 
        mutate(award_number = case_when(
            award_number %in% active_award_number ~ award_number,
            document_number %in% active_award_number ~ document_number,
            TRUE ~ obl_document_number
        )  ,
        transaction_disbursement = case_when(transaction_event == "DISB" ~ transaction_amt,
                                             .default = NA_real_),
        transaction_obligation = case_when(transaction_event_type == "OBLG_SUBOB" ~ transaction_amt,
                                           transaction_event_type == "OBLG_UNI" ~ transaction_amt,
                                           .default = NA_real_),
        avg_monthly_exp_rate = transaction_disbursement/3
        ) |> 
        group_by(award_number, transaction_date, period,  program_area_new) |> 
        summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), .groups = "drop") |> 
        rename(program_area = program_area_new)
    
    return(temp)
}