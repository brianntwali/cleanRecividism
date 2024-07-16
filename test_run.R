# Brian's Github Path
# encripted_drive_path <- 
# Neil's PC Github Path
github_path <- "C:/Users/silveus/Documents/github projects/cleanRecividism/"

setwd(github_path)

# Pull functions from helper functions.R --------------------------------------------------
source("helper functions.R", echo = TRUE)

# Creating calendar file --------------------------------------------------



print(head(unique_IDs, 10))

numberOfCores <- detectCores()
system.time({
  final_list_of_dts <- mclapply(unique_IDs, populate_IDs, check_date = '01012008', end_date = '01012021')
  final_calendar_file <- Reduce(rbind, final_list_of_dts)
})

View(final_list_of_dts)


# Set the first column as row names
rownames(final_calendar_file) <- final_calendar_file[, 1]
# Remove the first column from the dataframe
final_calendar_file <- final_calendar_file[, -1]


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
