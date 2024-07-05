
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

test_program_area <- function(active_awards, subobligation_summary, phoenix, 
                               active_award_number) {
    
    program_active_awards_sub <- active_awards |> 
        left_join(subobligation_summary, by = c("award_number", "period")) |>
        select(activity_name, award_number, program_area, program_area_name, period) |> 
        distinct() 
    
    program_phoenix <- phoenix |> 
        select(program_area, award_number, period) |> 
      filter(award_number %in% active_award_number) |>
        mutate(phoenix = "exists") |> 
        distinct()

    temp <- program_active_awards_sub |> 
        left_join(program_phoenix, by = c("award_number", "period", "program_area")) |> 
        distinct()
    
    return(temp)
    
}


test_program_area_from_phoenix <- function(active_awards, subobligation_summary, phoenix, 
                               active_award_number) {
    
    program_active_awards_sub <- active_awards |> 
       left_join(subobligation_summary, by = c("award_number", "period")) |>
       select(activity_name, award_number, program_area, program_area_name, period) |> 
        distinct() 
    
    program_phoenix <- phoenix |> 
        select(program_area, award_number, period) |>
        filter(award_number %in% active_award_number) |>
        distinct()

    temp <-  program_phoenix|> 
        left_join(program_active_awards_sub, by = c("award_number", "period", "program_area")) |> 
        distinct()
    
    return(temp)
    
}
