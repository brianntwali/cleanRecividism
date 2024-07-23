# Brian's Github Path
# encripted_drive_path <- 
# Neil's PC Github Path
github_path <- "C:/Users/silveus/Documents/github projects/cleanRecividism/"

setwd(github_path)

# Pull functions from helper functions.R --------------------------------------------------
source("helper functions.R", echo = TRUE)

# Creating calendar file --------------------------------------------------



print(head(unique_IDs, 10))

# Create a cluster with the number of available cores
num_cores <- detectCores() - 1
cl <- makeCluster(num_cores)

#define populate_IDs2 with dates hard coded
populate_IDs2 <- function(x){
  return(populate_IDs(x, check_date = '01012008', end_date = '01012021'))
}
# Check if the dataset is available on all worker nodes. Use for diagnostics
check_dataset <- function(dataset) {
  exists(dataset, where = .GlobalEnv)
}

# Export the required libraries and functions to each worker node

# Export functions and required variables to the cluster
export_fns <- list("check_dataset","populate_IDs2","populate_IDs","get_prev_status_date","get_next_status_date","check_mov_IDs","check_status_and_ID","compare_dates","increment_date")
export_datasets <- prelim_datasets

# Define a function to load packages
load_packages <- function(packages) {
  lapply(packages, library, character.only = TRUE)
}

#  List of packages to export to cluster
export_packages <- list("parallel","gtsummary", "stringr", "schoolmath", "data.table", "readxl", "dplyr", "tidyr", "lubridate", "fastDummies", "ggplot2", "readr")

# Export packages to cluster
clusterCall(cl,load_packages,export_packages)

# Export datasets and functions to cluster
clusterExport(cl, c(export_datasets,export_fns))

#Run paralleled code to populate_IDs              
result <- parLapply(cl, unique_IDs, populate_IDs2)
write_csv(result,"C:/Users/silveus/Documents/PA DOC/intermediate data/TEMP_result.csv")
final_calendar_file <- Reduce(rbind, result)


# Diagnostic to check whether a dataset is present on worker nodes (cores)
#clusterEvalQ(cl, check_dataset("cc_counts_dt"))

# end cluster to free up cores
stopCluster(cl)


  # OLD -NAS 7/22/24
  # numberOfCores <- detectCores()
  # system.time({
  #   final_list_of_dts <- mclapply(unique_IDs, populate_IDs, check_date = '01012008', end_date = '01012021')
  #   final_calendar_file <- Reduce(rbind, final_list_of_dts)
  # })
  # 
  # View(final_list_of_dts)


# Set the first column as row names
rownames(final_calendar_file) <- final_calendar_file[, 1]
# Remove the first column from the dataframe
final_calendar_file <- final_calendar_file[, -1]
write_csv(final_calendar_file,"C:/Users/silveus/Documents/PA DOC/intermediate data/calander_file.csv")

# Creating main dataframe -------------------------------------------------

unique_facilities <- ccc_cohort %>% 
  distinct(facility, .keep_all = TRUE) %>% 
  select(center_code, facility, region_code)


main_df <- pop_m_yr_df(final_calendar_file)
print("Function 2 Complete")

main_df_complete <- main_df %>% 
  rowwise() %>%
  mutate(
    age = get_age(ID, m_yr),
    lsir = get_lsir(ID, m_yr), 
    race = get_race(ID),
    sex = get_sex(ID),
    facility_type = get_facility_type(loc),
    facility_region = get_facility_region(loc)
  )  %>%
  ungroup()
print("Function 3 Complete")

main_comparison_df <- main_df_complete %>% 
  group_by(loc, m_yr) %>% 
  summarize(
    avg_lsir = mean(lsir, na.rm = TRUE),
    avg_age = mean(age, na.rm = TRUE),
    perc_black = round(mean(race == 'BLACK') * 100, 3), 
    perc_male = round(mean(sex == 'MALE') * 100, 3),
    pop_count = n(),
    facility_type = first(facility_type), 
    facility_region = first(facility_region),  
    across(starts_with("facility_type_"), first),  
    across(starts_with("facility_region_"), first)
  )

print("Function 4 Complete")

main_comparison_summary <- main_comparison_df %>% 
  select(!c(loc, m_yr)) %>%
  tbl_summary(by = facility_type)

main_comparison_summary
print("Function 5 Complete")

# Creating facility level dummy variables ---------------------------------
# 
# main_df_with_dummy <- dummy_cols(
#   main_df_complete,
#   select_columns = c("facility_type", "facility_region"),
#   remove_first_dummy = TRUE
#   # remove_selected_columns = TRUE (Removes the original columns after creating the dummy variables)
# )
# 
# 
# main_facility_analysis <- main_df_with_dummy %>% 
#   group_by(loc, m_yr) %>% 
#   summarize(
#     avg_lsir = mean(lsir, na.rm = TRUE),
#     avg_age = mean(age, na.rm = TRUE),
#     perc_black = round(mean(race == 'BLACK') * 100, 3), 
#     perc_male = round(mean(sex == 'MALE') * 100, 3),
#     pop_count = n(),
#     facility_type = first(facility_type), 
#     facility_region = first(facility_region),  
#     across(starts_with("facility_type_"), first),  
#     across(starts_with("facility_region_"), first)
#   )
# 
