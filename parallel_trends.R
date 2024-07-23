# install.packages("multidplyr")
# install.packages("furrr")

library("multidplyr")
library("furrr")
library("parallel")
library("data.table")
library("readxl")
library("dplyr")
library("tidyr")
library("lubridate")

library("fastDummies")
library("ggplot2")
library("readr")


# Loading data ------------------------------------------------------------

# Brian's Path
encripted_drive_path <- "/Volumes/Untitled/PA DOC/"
# Neil's PC
# encripted_drive_path <- "E:/PA DOC/"

movements <- read.csv(paste0(encripted_drive_path, "2021-22_Silveus_deidentified_prison_spells.csv"))

demographics <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "demographics")

ccc_cohort <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_cohort")

ccc_moves <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_moves")

sentencing <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "sentencing")

lsir <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "lsir")


# Define the cluster
cluster <- new_cluster(parallel::detectCores() - 2)

# Load necessary libraries on each worker
cluster_library(cluster, c('tidyverse', 'furrr'))

# Copy required objects to each worker
# ADD: 'get_program', 'get_facility_region'
cluster_copy(cluster, c('get_age', 'get_lsir', 'get_race', 'get_sex', 'get_facility_type', 'ccc_moves', 'demographics', 'lsir', 'unique_facilities_df'))



# Select columns and drop duplicates by control number status code, and date. 
# Sometimes there are multiple entry records if the resident is enrolled in multiple programs.

new_arrivals <- ccc_moves %>% 
  distinct(movement_id, .keep_all = TRUE) %>% 
  filter(status_code %in% c('INRS', 'TRRC')) %>% 
  select(c('control_number','status_date', 'location' = 'location_from_code'))
  

View(new_arrivals)


unique_fac_codes <- ccc_cohort %>% 
  distinct(center_code, .keep_all = TRUE) %>% 
  pull(center_code)

INRS_codes <- new_arrivals %>% 
  distinct(location, .keep_all = TRUE) %>% 
  pull(location)

# There are 23 codes missing from ccc_cohort that are in ccc_moves rows with INRS


print(INRS_codes)

print(unique_fac_codes)

disjointed <- setdiff(INRS_codes, unique_fac_codes)

unique_facilities_df <- ccc_cohort %>% 
  distinct(facility, .keep_all = TRUE) %>% 
  select(center_code, facility, region_code)


get_facility_type <- function(loc) {
  facility <- unique_facilities_df %>% 
    filter(center_code == loc) %>% 
    pull(facility)
  
  if(str_detect(facility, 'CCC')) {
    return('CCC')
  }
  else {
    return('CCF')
  }
}


get_lsir <- function(id, current_date) {
  form_current_date <- as.Date(current_date, format = '%Y-%m-%d')
  current_lsir <- lsir %>% 
    filter(control_number == id &  as.Date(test_date, format = '%Y-%m-%d') < form_current_date) %>%
    arrange(desc(as.Date(test_date, format = '%Y-%m-%d'))) %>% 
    distinct(control_number, .keep_all = TRUE) %>% 
    pull(as.numeric(test_score))
  if(length(current_lsir) > 0 && !is.na(current_lsir)) {
    return(current_lsir)
  }
  else {
    return(NA)
  }
}


get_race <- function(id) {
  id_race <- demographics %>% 
    filter(control_number == id) %>%
    distinct(control_number, .keep_all = TRUE) %>% 
    pull(race)
  return(id_race)
}

get_sex <- function(id) {
  id_sex <- demographics %>% 
    filter(control_number == id) %>%
    distinct(control_number, .keep_all = TRUE) %>% 
    pull(sex)
  return(id_sex)
}

get_age <- function(id, current_date) {
  form_current_date <- as.Date(current_date, format = '%Y-%m-%d')
  id_dob <- demographics %>% 
    filter(control_number == id) %>%
    distinct(control_number, .keep_all = TRUE) %>% 
    pull(dob)
  age_in_days <- form_current_date - as.Date(id_dob, format = '%Y-%m-%d')
  age_in_years <- as.numeric(age_in_days) / 365.25
  return(age_in_years)
}


new_arrivals_filled <- new_arrivals %>%
  filter(!(location %in% disjointed)) %>% 
  rowwise() %>%
  partition(cluster) %>% 
  mutate(
    lsir = get_lsir(control_number, status_date),
    facility_type = get_facility_type(location),
    race = get_race(control_number)
    # age = get_age(control_number, status_date),
    # sex = get_sex(control_number),
    # programs = list(get_program(ID, m_yr))
  )  %>%
  ungroup() %>% 
  collect()

View(new_arrivals_filled)


# get_program <- function(id, current_date) {
#   form_current_date <- as.Date(strptime(current_date, format = '%m%d%Y'))
#   
#   # Filter rows based on control_number and date, then arrange by date
#   date_filtered <- ccc_moves %>% 
#     filter(control_number == id & as.Date(status_date, format = '%Y-%m-%d') < form_current_date) %>%
#     arrange(desc(as.Date(status_date, format = '%Y-%m-%d')))
#   
#   nearest_movement_id <- date_filtered %>% 
#     arrange(desc(movement_id)) %>% 
#     distinct(control_number, .keep_all = TRUE) %>% 
#     pull(movement_id)
#   
#   if(length(nearest_movement_id) > 0) {
#     # Filter to get all rows with the identified duplicate movement_id
#     all_programs <- date_filtered %>%
#       filter(movement_id == nearest_movement_id) %>% 
#       pull(program_code)
#     
#     return(all_programs)
#   } 
#   # add condition for when program is associated with unique movement id
#   
#   else {
#     return(character(0)) 
#   }
# }


new_arrivals_program <- new_arrivals_filled[1:10, ] %>% 
  rowwise() %>%
  partition(cluster) %>% 
  mutate( 
    programs = list(get_program(ID, m_yr))
  )


View(new_arrivals_filled)

write_csv(new_arrivals_filled, "arrival_trends.csv")


arrivals_floored <- new_arrivals_filled %>% 
  mutate(
    month_year = floor_date(status_date, unit = 'month')
  ) %>% 
  filter(year(month_year) > 2007 & year(month_year) < 2021)

View(arrivals_floored)


# 1 month (1), 2 months (2), 1 quarter (3), 6 months (6), 1 year (12), 2 years (24)
set_period <- function(input_df, input_period) {
input_df <- input_df %>%
    mutate(
      period_group = case_when(
        input_period == 2 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "2 months", labels = FALSE),
        input_period == 3 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "3 months", labels = FALSE),
        input_period == 6 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "6 months", labels = FALSE),
        input_period == 12 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "1 year", labels = FALSE),
        input_period == 24 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "2 years", labels = FALSE),
        TRUE ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "1 month", labels = FALSE)
      )
    ) %>%
    group_by(facility_type, period_group)
  
  return(input_df)
}

testing <- set_period(arrivals_floored, 2)



create_variable_flow <- function(df, period_set, var_name) {
  binned_df <- set_period(df, period_set)
  if(var_name == 'avg_lsir') {
    binned_df <- binned_df %>% 
      summarise(
        avg_lsir = mean(lsir, na.rm = TRUE)
      )
  }
  else if(var_name == 'perc_black') {
    binned_df <- binned_df %>% 
      summarise(
        perc_black = round(mean(race == 'BLACK') * 100, 3) 
      ) 
  }
  else if(var_name == 'avg_age') {
    binned_df <- binned_df %>% 
      summarise(
        avg_age = mean(age, na.rm = TRUE)
      )
  }
  else if(var_name == 'perc_male') {
    binned_df <- binned_df %>% 
      summarise(
        perc_male = round(mean(sex == 'MALE') * 100, 3)
      )
  }
  # Should I have a final else clause in case none of these are entered?
  else {
    stop("Unknown variable name")
  }
  
  start_date <- as.Date("2008-01-01") 
  binned_df <- binned_df %>%
    arrange(period_group) %>%
    mutate(
      period_group = start_date + months((period_group - 1) * period_set)
    ) %>%
    as.data.frame()
  
  date_breaks_string <- paste0(period_set, " month")
  
  # , colour = facility_type
  plot <- ggplot(binned_df, aes(x = period_group, y = .data[[var_name]], colour = facility_type)) +
    geom_point() +
    geom_line() +
    scale_x_date(name = "Group Period", date_breaks = date_breaks_string, 
                 date_labels = "%Y-%m")
  
  print(plot)
  
  return(binned_df)
}

lsir_flow <- create_variable_flow(arrivals_floored, 2, 'avg_lsir')




# lsir_flow <- arrivals_floored %>% 
#   summarise(
#     avg_lsir = mean(lsir, na.rm = TRUE),
#     .by = c(facility_type, month_year)
#   ) %>%
#   arrange(month_year) %>% 
#   mutate(month_year=ymd(month_year)) %>% 
#   as.data.frame()
# 
# 
# View(lsir_flow)
# 
# ggplot(lsir_flow, aes(x = month_year, y = avg_lsir, colour = facility_type)) +
#   geom_point() +
#   geom_line()
# 
# 
# race_flow <- arrivals_floored %>%
#   summarise(
#     perc_black = round(mean(race == 'BLACK') * 100, 3),
#     .by = c(facility_type, month_year)
#   ) %>%
#   arrange(month_year) %>%
#   mutate(month_year=ymd(month_year)) %>%
#   as.data.frame()
# 
# ggplot(race_flow, aes(x = month_year, y = perc_black, colour = facility_type)) +
#   geom_point() +
#   geom_line()
# 
# 
# age_flow <- arrivals_floored  %>% 
#   summarise(
#     avg_age = mean(age, na.rm = TRUE),
#     .by = c(facility_type, month_year)
#   ) %>%
#   arrange(month_year) %>% 
#   mutate(month_year=ymd(month_year)) %>% 
#   as.data.frame()
# 
# ggplot(age_flow, aes(x = month_year, y = avg_age, colour = facility_type)) +
#   geom_point() +
#   geom_line()
# 
# 
# male_flow <- arrivals_floored  %>% 
#   summarise(
#     perc_male = round(mean(sex == 'MALE') * 100, 3),
#     .by = c(facility_type, month_year)
#   ) %>%
#   arrange(month_year) %>% 
#   mutate(month_year=ymd(month_year)) %>% 
#   as.data.frame()
# 
# ggplot(male_flow, aes(x = month_year, y = perc_male, colour = facility_type)) +
#   geom_point() +
#   geom_line()
