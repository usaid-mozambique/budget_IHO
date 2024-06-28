
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

