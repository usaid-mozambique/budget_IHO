
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


