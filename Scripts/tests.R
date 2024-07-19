

#' Title
#'
#' @param active_awards 
#' @param subobligation_summary 
#' @param phoenix_pipeline 
#' @param phoenix_transaction 
#'
#' @return
#' @export
#'
#' @examples
test_awards <- function(active_awards, subobligation_summary, phoenix_pipeline, 
                        phoenix_transaction) {
    
    pipeline <- phoenix_pipeline |>
        select(award_number, period) |> 
        distinct() |> 
        mutate(pipeline = "exists")

    
    subobligation_summary_df <- subobligation_summary |>
        select(award_number, period) |> 
        distinct() |> 
        mutate(subobligation_summary = "exists")
  
    transaction <- phoenix_transaction |> 
        select(award_number, period) |> 
        distinct() |> 
        mutate(transaction = "exists")
    
    active_award_tbl <- active_awards |> 
        select(activity_name, award_number, period) |> 
        distinct() |> 
        left_join(subobligation_summary_df, by = c("award_number", "period")) |>  
        left_join(pipeline, by = c("award_number", "period")) |> 
        left_join(transaction, by = c("award_number", "period")) 
    
    return(active_award_tbl)
    
}

#' Title
#'
#' @param active_awards 
#' @param subobligation_summary 
#' @param phoenix_transaction 
#' @param phoenix_pipeline 
#' @param active_award_number 
#'
#' @return
#' @export
#'
#' @examples
test_program_area <- function(active_awards, subobligation_summary, phoenix_transaction,
                              phoenix_pipeline, active_award_number) {
  
  program_area_in_suboblgiation<- active_awards |> 
    left_join(subobligation_summary, by = c("award_number", "period")) |>
    select(program_area, period) |> 
    filter(!is.na(program_area)) |> 
    mutate(subobligation = "yes") |> 
    distinct() 
  
  
  program_area_in_pipeline <- phoenix_pipeline |> 
    filter(award_number %in% active_award_number) |>
    select(program_area, period) |> 
    mutate(pipeline = "yes") |> 
    distinct()
  
  program_area_in_transaction <- phoenix_transaction |> 
    filter(award_number %in% active_award_number) |>
    select(program_area, period) |> 
    mutate(transaction = "yes") |> 
    distinct()
  
  
  all_program_area <- program_area_in_suboblgiation |> 
    full_join(program_area_in_pipeline, by = c("program_area", "period")) |> 
    full_join(program_area_in_transaction, by = c("program_area", "period")) |> 
    distinct() |> 
    arrange(period, program_area)
  
  return(all_program_area)
    
}


#' Title
#'
#' @param active_awards 
#' @param subobligation_summary 
#'
#' @return
#' @export
#'
#' @examples
test_missing_program_area <- function(active_awards, subobligation_summary) {
  
  temp <- active_awards |> 
    left_join(subobligation_summary, by = c("award_number", "period")) |> 
    filter(!is.na(activity_name)) |> 
    select(award_number, period, activity_name, program_area) |> 
    distinct() |> 
    filter(is.na(program_area))
  
  return(temp)
    
}

#' Title
#'
#' @param active_awards 
#' @param phoenix_pipeline 
#' @param phoenix_transaction 
#' @param active_award_number 
#'
#' @return
#' @export
#'
#' @examples
test_po_2 <- function(active_awards, phoenix_pipeline, phoenix_transaction, active_award_number){
  pipeline <- phoenix_pipeline |> 
    filter(award_number %in% active_award_number,
           program_area == "PO.2")
  
  transaction <- phoenix_transaction |> 
    filter(award_number %in% active_award_number,
           program_area == "PO.2")
  
  awards <- active_awards |> 
    select(award_number, activity_name, period)
  
  po_2_data <- pipeline |> 
    full_join(transaction, by = c("award_number", "period")) |> 
    left_join(awards, by = c("award_number", "period")) |> 
    select(award_number, activity_name, period, everything())
  
  return(po_2_data)
  
}




#' Title
#'
#' @param raw_active_awards 
#' @param pipeline_dataset 
#'
#' @return
#' @export
#'
#' @examples
test_active_awards <- function(raw_active_awards, pipeline_dataset){
  
  raw_active <- raw_active_awards |> 
    select(award_number, activity_name, period, total_estimated_cost) |>
    mutate(raw_total_estimated_cost = total_estimated_cost)
  
  new_active <- pipeline_dataset |> 
    select(award_number, period, total_estimated_cost) |>
    group_by(award_number, period, total_estimated_cost) |>
    rename(new_total_estimated_cost = total_estimated_cost) |> 
    summarise(across(where(is.numeric), ~ avg(., na.rm = TRUE)), .groups = "drop") 
  
  final_active <- raw_active |> 
    left_join(new_active, by = c("award_number", "period")) |> 
    mutate(TEA_diff = raw_total_estimated_cost - new_total_estimated_cost)  |> 
    select(-total_estimated_cost)
  
  return(final_active)
  
}




#' Title
#'
#' @param raw_phoenix_transaction 
#' @param raw_active_awards 
#' @param new_pipeline 
#'
#' @return
#' @export
#'
#' @examples
test_transaction_number_pipeline <- function(raw_phoenix_transaction, raw_active_awards, new_pipeline){
  
  test_raw_active_awards <- raw_active_awards |>
    select(award_number, activity_name) |> 
    distinct()
  
  test_raw_transaction <- raw_phoenix_transaction |> 
    select(award_number, period, transaction_amt) |>
    group_by(award_number, period) |> 
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") |> 
    left_join(test_raw_active_awards, by = "award_number") |> 
    mutate(raw_transaction_amt = transaction_amt)
  
  
  test_pipeline <- new_pipeline |> 
    select(award_number, period, transaction_amt) |>
    group_by(award_number, period) |>
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") |> 
    left_join(test_raw_active_awards, by = "award_number") |> 
    mutate(new_transaction_amt = transaction_amt)
  

  final <- test_raw_transaction |> 
    left_join(test_pipeline, by = c("award_number", "period")) |> 
    mutate(across(where(is.numeric), ~ replace_na(., 0)) ) |> 
    mutate(transaction_diff = raw_transaction_amt - new_transaction_amt) |> 
    select(award_number, period, 
           raw_transaction_amt, new_transaction_amt, transaction_diff)
  
  
  return(final)
  
  
  
}



#' Title
#'
#' @param active_awards 
#' @param phoenix_transaction 
#' @param transaction 
#'
#' @return
#' @export
#'
#' @examples
test_transaction_number_transaction <- function(active_awards, phoenix_transaction, transaction){
  
  test_active_awards <- active_awards |> 
    select(award_number, activity_name) |> 
    distinct()
  
  test_raw_transaction <- phoenix_transaction |> 
    select(award_number, period, transaction_amt) |>
    group_by(award_number, period) |> 
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") |> 
    left_join(test_active_awards, by = "award_number") |> 
    mutate(raw_transaction_amt = transaction_amt)
  
  test_transaction <- transaction |> 
    select(award_number, period, transaction_amt) |>
    group_by(award_number, period) |>
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") |> 
    left_join(test_active_awards, by = "award_number") |> 
    mutate(new_transaction_amt = transaction_amt)
  
  final <- test_raw_transaction |> 
    left_join(test_transaction, by = c("award_number", "period")) |> 
    mutate(across(where(is.numeric), ~ replace_na(., 0)) ) |> 
    mutate(transaction_diff = raw_transaction_amt - new_transaction_amt) |> 
    select(award_number, period, 
           raw_transaction_amt, new_transaction_amt, transaction_diff)
  
  return(final)
  

}



#' Title
#'
#' @param raw_phoenix_pipeline 
#' @param raw_active_awards 
#' @param new_pipeline 
#'
#' @return
#' @export
#'
#' @examples
test_pipeline_number <- function(raw_phoenix_pipeline, raw_active_awards, new_pipeline){
  
  #original active awards dataset   
  test_raw_active_awards <- raw_active_awards |> 
    group_by(award_number, activity_name, total_estimated_cost, period) |> 
    summarise(across(where(is.numeric), ~ max(., na.rm = TRUE)), .groups = "drop") |> 
    mutate(raw_total_estimated_cost = total_estimated_cost) 
    
  
  #original pipeline dataset with filters applied and activity names added
  test_raw_pipeline <- raw_phoenix_pipeline |> 
    group_by(award_number, period) |> 
    summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), .groups = "drop") |> 
    select(award_number, period, document_amt, disbursement_amt, last_qtr_accrual_amt, undisbursed_amt) |> 
    rename(raw_document_amt = document_amt, 
           raw_disbursement_amt = disbursement_amt,
           raw_last_qtr_accrual_amt = last_qtr_accrual_amt,
         raw_undisbursed_amt = undisbursed_amt) |> 
    left_join(test_raw_active_awards, by = c("award_number", "period")) |> 
    select(award_number, activity_name, period, everything())
  
  #bring in the created pipeline dataset
  test_pipeline <- new_pipeline |> 
    group_by(award_number, activity_name, period) |>
    summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), .groups = "drop") |>
    select(award_number, activity_name, period, 
           document_amt, disbursement_amt, last_qtr_accrual_amt,
           total_estimated_cost, undisbursed_amt) |> 
    rename(new_document_amt = document_amt,
           new_disbursement_amt = disbursement_amt,
           new_last_qtr_accrual_amt = last_qtr_accrual_amt,
           new_total_estimated_cost = total_estimated_cost,
           new_undisbursed_amt = undisbursed_amt)
  
  #compare the datasets
  test_final <- test_raw_pipeline |> 
    left_join(test_pipeline, by = c("award_number", "period", "activity_name")) |> 
    mutate(across(where(is.numeric), ~ replace_na(., 0))) |> 
    mutate(disbursement_dif = raw_disbursement_amt - new_disbursement_amt,
           document_dif = raw_document_amt - new_document_amt,
           last_qtr_accrual_diff = raw_last_qtr_accrual_amt - new_last_qtr_accrual_amt,
           TEA_dff = raw_total_estimated_cost - new_total_estimated_cost,
           undisbursed_diff = raw_undisbursed_amt - new_undisbursed_amt) |> 
    select(award_number, activity_name, period, 
           raw_document_amt, new_document_amt, document_dif, 
           raw_disbursement_amt, new_disbursement_amt, disbursement_dif,
           raw_last_qtr_accrual_amt, new_last_qtr_accrual_amt, last_qtr_accrual_diff,
           raw_undisbursed_amt, new_undisbursed_amt, undisbursed_diff)  
  
  return(test_final)
  
}


#test_subobligation_numbers <- function(active_awards, subobligation_summary, new_pipeline){
  
#  test_active_awards <- active_awards |> 
#    select(award_number, activity_name) |> 
#    distinct()
  
#  test_raw_subobligation <- subobligation_summary |> 
#    select(-c(planned_sub_oblig_date, program_area, comments)) |> 
#    group_by(award_number, period) |> 
#    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") |> 
#    left_join(test_active_awards, by = "award_number") |> 
#    mutate(across(where(is.numeric), ~ paste0("raw_",.)))
  
#  test_pipeline <- new_pipeline |> 
#    select(-c(sub_sector, program_area, program_area_name, comments, start_date, end_date,
#              planned_sub_oblig_date, total_estimated_cost, u_s_org_local,
#              aor_cor_or_activity_manager, funding_type, pepfar_funding, 
#              document_amt, disbursement_amt, undisbursed_amt, last_qtr_accrual_amt, 
#              total_disbursement_outlays, transaction_amt, 
#              transaction_disbursement, transaction_obligation, )) |> 
#    group_by(award_number, period) |> 
#    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") |> 
#    left_join(test_active_awards, by = "award_number") |> 
#    mutate(across(where(is.numeric), ~ paste0("raw_",.)))
#  
#  final <- test_raw_subobligation |> 
#    left_join(test_pipeline, by = c("award_number", "period")) |> 
#    mutate(across(where(is.numeric), ~ replace_na(., 0)) ) |> 
#    mutate(subobligation_diff = raw_subobligation_amt - new_subobligation_amt) |> 
#    select(award_number, period, 
#           raw_subobligation_amt, new_subobligation_amt, subobligation_diff)
  
#  return(final)
  
#}


