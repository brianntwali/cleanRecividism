
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
library("haven")


# Loading data ------------------------------------------------------------

# Brian's Path
# Brian
encripted_drive_path <- "/Volumes/Untitled/PA DOC/"
dropbox_drive_path <- "/Users/brianntwali/Dropbox/Ntwali Silveus/"

# Neil mac
# encripted_drive_path <- "/Users/silveus/Documents/Data/PA DOC/" 


demographics <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "demographics")

ccc_cohort <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_cohort")

ccc_moves <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_moves")

sentencing <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "sentencing")


# importing calendar file

individual_daily_data <- readRDS("/Volumes/Untitled/intermediate data/Main files/main_complete_df.rds")
individual_daily_data$m_yr<- mdy(individual_daily_data$m_yr) 
individual_daily_data <- individual_daily_data %>% 
  relocate(c(ID, loc), .before = 1) %>% 
  # spelling error 
  filter(facility_type != "UNKOWN") %>% 
  rename(month_year = m_yr) %>% 
  arrange(ID, month_year)


View(individual_daily_data)



# Define the cluster
cluster <- new_cluster(parallel::detectCores() - 2)

# Load necessary libraries on each worker
cluster_library(cluster, c('tidyverse', 'furrr'))

# Copy required objects to each worker
# ADD:'get_facility_region'
cluster_copy(cluster, c('get_program', 'save_intermediate_date', 'get_age', 'get_lsir', 'get_race', 'get_sex', 'get_facility_type', 'get_offense_code', 'ccc_moves', 'demographics', 'lsir', 'individual_daily_data', 'unique_facilities_df', 'sentencing_df'))



# Select columns and drop duplicates by control number status code, and date. 
# Sometimes there are multiple entry records if the resident is enrolled in multiple programs.

sentencing_df <- sentencing %>% 
  mutate(
    sentence_date = as.Date(sentence_date, format = '%Y%m%d')
  ) %>% 
  arrange(control_number, sentence_date)



# Creating individual ARRIVAL data ----------------------------------------



new_arrivals <- ccc_moves %>% 
  distinct(movement_id, .keep_all = TRUE) %>% 

  filter(status_code %in% c('INRS', 'TRRC')) %>% 
  select(c('control_number','status_date', 'location' = 'location_from_code'))

unique_fac_codes <- ccc_cohort %>% 
  distinct(center_code, .keep_all = TRUE) %>% 
  pull(center_code)

INRS_codes <- new_arrivals %>% 
  distinct(location, .keep_all = TRUE) %>% 
  pull(location)

# There are 23 codes missing from ccc_cohort that are in ccc_moves rows with INRS

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


get_program <- function(id, current_date) {
  form_current_date <- as.Date(strptime(current_date, format = '%Y-%m-%d'))
  
  # Filter rows based on control_number and date, then arrange by date
  date_filtered <- ccc_moves %>%
    filter(control_number == id & as.Date(status_date, format = '%Y-%m-%d') <= form_current_date) %>%
    arrange(desc(as.Date(status_date, format = '%Y-%m-%d')))
  
  nearest_movement_id <- date_filtered %>%
    arrange(desc(movement_id)) %>%
    distinct(control_number, .keep_all = TRUE) %>%
    pull(movement_id)
  
  if(length(nearest_movement_id) > 0) {
    # Filter to get all rows with the identified duplicate movement_id
    all_programs <- date_filtered %>%
      filter(movement_id == nearest_movement_id) %>%
      pull(program_code)
    
    return(all_programs)
  }

  else {
    return(character(0))
  }
}

View(ccc_moves %>% 
       filter(year(status_date) >= 2008) %>% 
       arrange(control_number, status_date, movement_id))

get_offense_code <- function(id, current_date) {
  form_current_date <- as.Date(strptime(current_date, format = '%Y-%m-%d'))
  offense_code <- sentencing_df %>% 
    filter(control_number == id & as.Date(sentence_date, format = '%Y-%m-%d') <= form_current_date) %>% 
    arrange(desc(sentence_date)) %>% 
    distinct(control_number, .keep_all = TRUE) %>% 
    pull(asca_category)
  if(length(offense_code) == 0) {
    return(NA)
  }
  return(offense_code)
}

# Filling new entrance dataframe ------------------------------------------

new_arrivals_filled <- new_arrivals %>%
  filter(!(location %in% disjointed)) %>% 
  rowwise() %>%
  partition(cluster) %>% 
  mutate(
    lsir = get_lsir(control_number, status_date),
    facility_type = get_facility_type(location),
    race = get_race(control_number),
    age = get_age(control_number, status_date),
    sex = get_sex(control_number),
    month = month.abb[month(status_date)],
    offense_code = get_offense_code(control_number, status_date),
    programs = list(get_program(control_number, status_date))
  )  %>%
  ungroup() %>% 
  collect()

new_arrivals_unnested <- new_arrivals_filled %>% 
  unnest(programs)

write.csv(new_arrivals_unnested, "arrivals_trends.csv")

View(new_arrivals_unnested)

saved_arrivals <- read.csv("arrivals_trends.csv", colClasses = c("control_number" = "character"))

saved_arrivals <- saved_arrivals[, -1] %>% 
  mutate(status_date = ymd(status_date))

View(saved_arrivals)

arrivals_floored <- saved_arrivals %>% 
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


create_program_count <- function(df, period_set) {
  binned_df <- df %>%
    mutate(
      period_group = case_when(
        period_set == 1 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "1 month", labels = FALSE),
        period_set == 3 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "3 months", labels = FALSE)
      ) 
    ) 
  
  dummy_df <- dummy_cols(binned_df, select_columns = 'programs', remove_first_dummy = FALSE)
  
  start_date <- as.Date("2008-01-01") 
  dummy_df <- dummy_df %>%
    arrange(period_group) %>%
    mutate(
      period_group = start_date + months((period_group - 1) * period_set)
    ) %>%
    group_by(period_group, location, facility_type) %>% 
    summarise(
      count = n(),
      avg_lsir = mean(lsir, na.rm = TRUE),
      count_black = sum(race == 'BLACK'),
      avg_age = mean(age, na.rm = TRUE),
      perc_male = round(mean(sex == 'MALE') * 100, 3),
      drug_offenses = sum(offense_code == 'Drugs', na.rm = TRUE),
      violent_offenses = sum(offense_code %in% c('Part I Violent', 'Other Violent')),
      property_offenses = sum(offense_code == 'Property', na.rm = TRUE),
      across(starts_with("programs_"), ~ sum(.x, na.rm = TRUE))
    ) %>% 
    relocate(location, .before = period_group) %>% 
    # rename(period_group = case_when(
    #   period_set == 1 ~ month,
    #   period_set == 3 ~ quarter
    # )) %>% 
    as.data.frame()
  return(dummy_df)
}

create_indiv_program_count <- function(df, period_set) {
  binned_df <- df %>%
    mutate(
      period_group = case_when(
        period_set == 1 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "1 month", labels = FALSE),
        period_set == 3 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "3 months", labels = FALSE)
      ) 
    ) 
  dummy_df <- dummy_cols(binned_df, select_columns = 'programs', remove_first_dummy = FALSE)
  start_date <- as.Date("2008-01-01") 
  dummy_df <- dummy_df %>%
    arrange(period_group) %>%
    mutate(
      period_group = start_date + months((period_group - 1) * period_set),
      year = year(period_group),
      quarter = quarter(period_group)
    ) %>%
    distinct(period_group, control_number, .keep_all = TRUE) %>%
    group_by(period_group, year, quarter, control_number, location, facility_type) %>% 
    summarise(
      count = n(),
      avg_lsir = mean(lsir, na.rm = TRUE),
      count_black = sum(race == 'BLACK'),
      avg_age = mean(age, na.rm = TRUE),
      perc_male = round(mean(sex == 'MALE') * 100, 3),
      drug_offenses = sum(offense_code == 'Drugs', na.rm = TRUE),
      violent_offenses = sum(offense_code %in% c('Part I Violent', 'Other Violent')),
      property_offenses = sum(offense_code == 'Property', na.rm = TRUE),
      across(starts_with("programs_"), ~ sum(.x, na.rm = TRUE))
    ) %>% 
    relocate(location, .before = period_group) %>% 
    relocate(control_number, .before = location) %>% 
    # rename(period_group = case_when(
    #   period_set == 1 ~ month,
    #   period_set == 3 ~ quarter
    # )) %>% 
    as.data.frame()
  return(dummy_df)
}


save_individual_dataframe <- function(input_df, input_period_set) {
   created_df <- create_indiv_program_count(df = input_df, period_set = input_period_set)
   created_df <- dummy_cols(created_df, 'facility_type', remove_first_dummy = TRUE, remove_selected_columns = TRUE)
   created_df <- created_df %>% 
     mutate(
       post_2013 = ifelse(year(period_group) >= 2013, 1, 0), 
       .after = period_group
     ) %>% 
     relocate(facility_type_CCF, .before = count)
   
   df_period = case_when(
     input_period_set == 1 ~ "month",
     input_period_set == 3 ~ "quarter",
   )

   write.csv(created_df, (paste0("individual_per_", df_period, ".csv")))
}


get_individual_quarter <- save_individual_dataframe(arrivals_floored, 3)

get_individual_month <- save_individual_dataframe(arrivals_floored, 1)



# Make this less redundant 


monthly_programs <- create_program_count(arrivals_floored, 1)

quarter_programs <- create_program_count(arrivals_floored, 3)

monthly_programs <- dummy_cols(monthly_programs, 'facility_type', remove_first_dummy = TRUE, remove_selected_columns = TRUE)

quarter_programs <- dummy_cols(quarter_programs, 'facility_type', remove_first_dummy = TRUE, remove_selected_columns = TRUE)


monthly_programs <- monthly_programs %>% 
  mutate(
    # Using month_year might be more clear 
    post_2013 = ifelse(year(period_group) >= 2013, 1, 0), 
    .after = period_group
  ) %>% 
  relocate(facility_type_CCF, .before = count)

View(monthly_programs)


quarter_programs <- quarter_programs %>% 
  mutate(
    # Using month_year might be more clear 
    post_2013 = ifelse(year(period_group) >= 2013, 1, 0), 
    .after = period_group
  ) %>% 
  relocate(facility_type_CCF, .before = count)

View(quarter_programs)




# Filling individual daily dataframe --------------------------------------


View(individual_daily_data)

save_intermediate_date <- function(df) {
  df <- df %>%
    rowwise() %>%
    partition(cluster) %>%
    mutate(
      offense_code = get_offense_code(ID, month_year),
      programs = list(get_program(ID, month_year))
    )  %>%
    ungroup() %>%
    collect()
  write_rds(df, "intermediate_daily_data.rds")
  return(df)
}

returned_intermediate_data <- save_intermediate_date(individual_daily_data)

returned_intermediate_data <- returned_intermediate_data %>%
  unnest(programs) 

View(returned_intermediate_data %>% 
       arrange(ID, month_year))

returned_intermediate_data <- returned_intermediate_data %>%
  mutate(
    month = month(month_year),
    quarter = ceiling(4*(month/12)),
    year = year(month_year)
  )


create_daily_var_counts <- function(df, period_set) {
  binned_df <- df %>%
    mutate(
      period_group = case_when(
        period_set == 1 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "1 month", labels = FALSE),
        period_set == 3 ~ cut.Date(as.Date(month_year, format = '%Y-%m-%d'), breaks = "3 months", labels = FALSE)
      )
    )
  dummy_df <- dummy_cols(binned_df, select_columns = 'programs', remove_first_dummy = FALSE)
  start_date <- as.Date("2008-01-01")
  dummy_df <- dummy_df %>%
    arrange(period_group) %>%
    mutate(
      period_group = start_date + months((period_group - 1) * period_set)
    ) %>%
    distinct(period_group, ID, .keep_all = TRUE) %>%
    group_by(period_group, year, quarter, month, ID, loc, facility_type) %>%
    summarise(
      count = n(),
      avg_lsir = mean(lsir, na.rm = TRUE),
      count_black = sum(race == 'BLACK'),
      avg_age = mean(age, na.rm = TRUE),
      perc_male = round(mean(sex == 'MALE') * 100, 3),
      drug_offenses = sum(offense_code == 'Drugs', na.rm = TRUE),
      violent_offenses = sum(offense_code %in% c('Part I Violent', 'Other Violent')),
      property_offenses = sum(offense_code == 'Property', na.rm = TRUE),
      across(starts_with("programs_"), ~ sum(.x, na.rm = TRUE))
    ) %>%
    relocate(loc, .before = period_group) %>%
    relocate(ID, .before = loc) %>%
    rename(location = loc) %>%
    # rename(period_group = case_when(
    #   period_set == 1 ~ month,
    #   period_set == 3 ~ quarter
    # )) %>%
    as.data.frame()
  return(dummy_df)
}


save_complete_daily_df <- function(input_df, input_period_set) {
  created_df <- create_daily_var_counts(df = input_df, period_set = input_period_set)
  created_df <- dummy_cols(created_df, 'facility_type', remove_first_dummy = TRUE, remove_selected_columns = TRUE)
  created_df <- created_df %>%
    mutate(
      post_2013 = ifelse(year(period_group) >= 2013, 1, 0),
      .after = period_group
    ) %>%
    relocate(facility_type_CCF, .before = count)

  df_period = case_when(
    input_period_set == 1 ~ "month",
    input_period_set == 3 ~ "quarter",
  )

  write_rds(created_df, (paste0("daily_data_per_", df_period, ".rds")))
  return(created_df)
}

get_daily_data_month <- save_complete_daily_df(returned_intermediate_data, 1)
get_daily_data_quarter <- save_complete_daily_df(returned_intermediate_data, 3)

# Should I make sure that month-year is included 
View(get_daily_data_quarter %>%  arrange(ID, period_group))


# Add county level characteristics ----------------------------------------

daily_facilities_df <- get_daily_data_month %>% 
       distinct(location)

facilities_data <- readRDS(paste0(dropbox_drive_path, "cleanRecividism/unique_facilities_list.rds"))

facilities_name_pop <- read_xlsx(paste0(dropbox_drive_path, "Facilities_population.xlsx"))

View(facilities_name_pop)

facilities_data <- facilities_data %>% 
  rename(location = center_code) 

daily_facilities_df <- left_join(daily_facilities_df, facilities_data, by = "location") 

daily_facilities_df <- daily_facilities_df %>% 
  mutate(facility = substring(facility, 5, nchar(facility)))
  
daily_facilities_df <- left_join(daily_facilities_df, facilities_name_pop, by = "location")

daily_facilities_df <- daily_facilities_df %>% 
  arrange(City)

write_csv(daily_facilities_df, "add_addresses.csv")



county_dt <- read_dta(paste0(dropbox_drive_path, "Secondary Data/county_characteristics.dta"))

View(county_dt)

names(county_dt)

county_dt <- county_dt %>% 
  select(year, countyfip, )

# Creating raw data flow --------------------------------------------------


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
 
    date_breaks_string <- "1 year"
    date_labels_string <- "%Y"
 
  plot <- ggplot(binned_df, aes(x = period_group, y = .data[[var_name]], colour = facility_type)) +
    geom_point() +
    geom_line() +
    scale_x_date(name = "Group Period", date_breaks = date_breaks_string, 
                 date_labels = date_labels_string) 
  
  print(plot)
  
  return(binned_df)
}

# lsir_flow <- create_variable_flow(arrivals_floored, 6, 'avg_lsir')
# 
# View(lsir_flow)
# 
# race_flow <- create_variable_flow(arrivals_floored, 3, 'perc_black')



# Adding Offenses

View(unique_offenses <- sentencing %>% 
  distinct(asca_category))

count_offenses <- sentencing %>% 
  group_by(asca_category) %>% 
  summarise(
    count = n()
  ) 

View(count_offenses)

# 1519 instances, I should check other 'false' sentencing
nrow(sentencing %>% 
       filter(min_expir_date == '00000000'))

sentencing_df <- sentencing %>% 
  mutate(
    sentence_date = as.Date(sentence_date, format = '%Y%m%d')
  ) %>% 
  arrange(control_number, sentence_date)

View(sentencing_df %>% 
       filter(asca_category == "Part I Violent"))

View(ccc_moves %>% 
       filter(control_number == '001165') %>% 
       arrange(status_date, movement_id))

# Gauge the magnitude 


