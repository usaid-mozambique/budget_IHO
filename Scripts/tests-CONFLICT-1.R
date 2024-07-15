
# TODO all NAS are added to the top of the list

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

test_pipeline_number <- function(raw_phoenix_pipeline, raw_active_awards, new_pipeline){
  
  test_raw_pipeline <- raw_phoenix_pipeline |> 
    group_by(award_number, period) |> 
    summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), .groups = "drop") |> 
    select(award_number, period, document_amt, disbursement_amt, last_qtr_accrual_amt) |> 
    rename(raw_document_amt = document_amt, 
           raw_disbursement_amt = disbursement_amt,
           raw_last_qtr_accrual_amt = last_qtr_accrual_amt)
  
  
  test_raw_active_awards <- raw_active_awards |> 
    select(award_number, activity_name, total_estimated_cost, period)
  
  
  test_raw_active_pipeline <- test_raw_pipeline |> 
    left_join(test_raw_active_awards, by = c("award_number", "period")) |> 
    select(award_number, activity_name, period, everything())
  
  
  test_pipeline <- new_pipeline |> 
    group_by(award_number, activity_name, period) |>
    summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), .groups = "drop") |>
    select(award_number, activity_name, period, document_amt, disbursement_amt) 
  
  test_final <- test_raw_active_pipeline |> 
    left_join(test_pipeline, by = c("award_number", "period", "activity_name")) |> 
    mutate(across(where(is.numeric), ~ replace_na(., 0))) |> 
    rename(new_document_amt = document_amt, new_disbursement_amt = disbursement_amt) |> 
    mutate(disbursement_dif = raw_disbursement_amt - new_disbursement_amt,
           document_dif = raw_document_amt - new_document_amt) |> 
    select(award_number, activity_name, period, 
           raw_document_amt, new_document_amt, document_dif, 
           raw_disbursement_amt, new_disbursement_amt, disbursement_dif)  
  
  return(test_final)
  
}


