# 2. Begin constructing a "facility level" data set.  Monthly snapshot of census at each facility.  
# Average lsir, percent black, and average age, count, anything else that I am forgetting 
# in the demographic dataset. This will take some doing, but I also would like 
# to create a binary for whether the facility has anyone enrolled in various programs. 
# You can find this in the program code and program description columns in the ccc_moves dataset. 
# So there would be a dummy for residential substance abuse treatment (code RSAT) 
# that would be 1 if there is someone at that facility enrolled in that program. 
# For now, I think we should do the snapshot at the beginning of each month, but we may change that later.

# install.packages("schoolmath")
# install.packages("stringr")
# install.packages("gtsummary")
# install.packages("multidplyr")
# install.packages("furrr")

library("multidplyr")
library("furrr")
library("parallel")
library("gtsummary")
library("stringr")
library("schoolmath")
library("data.table")
library("readxl")
library("dplyr")
library("tidyr")
library("lubridate")

library("fastDummies")
library("ggplot2")
library("readr")



# Loading data ------------------------------------------------------------

# partially complete analysis file


# Brian's Path
encripted_drive_path <- "/Volumes/Untitled/PA DOC/"
encripted_drive_path_2 <- "/Users/brianntwali/Dropbox/Ntwali Silveus/"
# Neil's PC (UPDATE #2)
# encripted_drive_path <- "E:/PA DOC/"
# encripted_drive_path_2 <- "E:/PA DOC/"

calendar_file <- read.csv(paste0(encripted_drive_path_2, "cleanRecividism/calendar_file_use.csv"),
                          colClasses = c("ID" = "character"), check.names = FALSE)

# Set the first column as row names
rownames(calendar_file) <- calendar_file[, 1]
# Remove the first column from the DataFrame
calendar_file <- calendar_file[, -1]

demographics <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "demographics")

ccc_cohort <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_cohort")

ccc_moves <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_moves")

sentencing <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "sentencing")

lsir <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "lsir")

num_cores <- detectCores() - 1 


# Creating base dataframe for analysis ------------------------------------



# function to get location for one ID and one date and enter into add_df 
get_id_m_yr_data <- function(input_id, input_cal_file, input_date) {
  # Extract the value
  value <- as.numeric(input_cal_file[input_id, input_date])
  
  # Check if the value is positive
  if (!is.na(value) && value > 0) {
    # Create a DataFrame with the necessary information
    add_df <- data.frame(m_yr = input_date, loc = input_cal_file[input_id, input_date], ID = input_id, stringsAsFactors = FALSE)
    return(add_df)
  } else {
    # Return an empty DataFrame if the value is not positive
    return(data.frame())
  }
}

# # Test the function
# test_df1 <- get_id_m_yr_data('069573', calendar_file, '01142008')
# 
# # View the result
# View(test_df1)


# function to create a particular month-year's associated rows
collect_m_yr_data <- function(m_yr_rows, ids, cal_file) {
  list_ids_dfs <- mclapply(ids, get_id_m_yr_data, input_cal_file = cal_file, input_date = m_yr_rows)
  fill_df_for_m_yr <- do.call(rbind, list_ids_dfs)
}

# test
# test_list_ids <- rownames(calendar_file)
# test_df2 <- collect_m_yr_data('01142008', test_list_ids, calendar_file)

# function to create range of years to operate on
create_yrs <- function() {
  date_sequence <- seq(as.Date("2008-01-01"), as.Date("2020-12-12"), by = "month")
   # Format the dates as MMDDYYYY
  formatted_dates <- format(date_sequence, "%m%d%Y")
  return(formatted_dates)
}

# main function to populate the month-year dataframe
pop_m_yr_df <- function(og_cal_file) {
  years <- create_yrs()
  extracted_ids <- rownames(og_cal_file)
  list_of_dfs <- mclapply(years, collect_m_yr_data, ids = extracted_ids, cal_file = og_cal_file)
  m_yr_df <- do.call(rbind, list_of_dfs) 
  return(m_yr_df)
}

# run main function
m_yr_df <- pop_m_yr_df(calendar_file[1:10,])
View(m_yr_df)




# Functions to add data ---------------------------------------------------

get_age <- function(id, current_date) {
  form_current_date <- as.Date(strptime(current_date, format = '%m%d%Y'))
  id_dob <- demographics %>% 
    filter(control_number == id) %>%
    distinct(control_number, .keep_all = TRUE) %>% 
    pull(dob)
  age_in_days <- form_current_date - as.Date(id_dob, format = '%Y-%m-%d')
  age_in_years <- as.numeric(age_in_days) / 365.25
  return(age_in_years)
  }

get_lsir <- function(id, current_date) {
  form_current_date <- as.Date(strptime(current_date, format = '%m%d%Y'))
  current_lsir <- lsir %>% 
    filter(control_number == id &  as.Date(test_date, format = '%Y-%m-%d') < form_current_date) %>%
    arrange(desc(as.Date(test_date, format = '%Y-%m-%d'))) %>% 
    distinct(control_number, .keep_all = TRUE) %>% 
      # this might throw errors if there are no 'previous' lsir scores)
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

get_facility_region <- function(loc) {
  reg_code <- unique_facilities_df %>% 
    filter(center_code == loc) %>% 
    pull(region_code)
  return(reg_code)
}



# I am unsure if this will all work well if the dates are alreay floored 

unique_programs <- ccc_moves %>% 
  distinct(program_code) %>% 
  filter(!is.na(program_code) & !(program_code == 'NULL')) %>% 
  pull(program_code)

get_program <- function(id, current_date) {
  form_current_date <- as.Date(strptime(current_date, format = '%m%d%Y'))
  
  # Filter rows based on control_number and date, then arrange by date
  date_filtered <- ccc_moves %>% 
    filter(control_number == id & as.Date(status_date, format = '%Y-%m-%d') < form_current_date) %>%
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
  # add condition for when program is associated with unique movement id
  
  else {
    return(character(0)) 
    }
}

# Test
print(get_program('001165', '10042005'))
print(get_program('004042', '12162010'))
print(get_program('004042', '19162010'))


View(ccc_moves %>% 
       filter(control_number == '001165') %>% 
       arrange(status_date, movement_id) %>% 
       select(control_number, status_date, movement_id, program_code, program_description, status_code)) 


# Adding further data to df -----------------------------------------------

# Define the cluster
cluster <- new_cluster(parallel::detectCores() - 2)

# Load necessary libraries on each worker
cluster_library(cluster, c('tidyverse', 'furrr'))

# Copy required objects to each worker
cluster_copy(cluster, c('get_age', 'get_lsir', 'get_race', 'get_sex', 'get_facility_type', 'get_facility_region', 'get_program', 'ccc_moves', 'demographics', 'lsir', 'm_yr_df', 'unique_facilities_df'))



View(m_yr_df)

df_complete <- m_yr_df %>% 
  rowwise() %>%
  partition(cluster) %>% 
  mutate(
    lsir = get_lsir(ID, m_yr),
    age = get_age(ID, m_yr),
    race = get_race(ID),
    sex = get_sex(ID),
    facility_type = get_facility_type(loc),
    facility_region = get_facility_region(loc),
    programs = list(get_program(ID, m_yr))
  )  %>%
  ungroup() %>% 
  collect()


View(df_complete)


# Creating a data frame with new columns for each unique program code initialized to 0
new_columns <- setNames(as.data.frame(matrix(0, nrow = nrow(df_complete), ncol = length(unique_programs))),
                        paste0("prog_", unique_programs))

# Binding the new columns to the original data frame
df_complete_program <- bind_cols(df_complete, new_columns)


View(df_complete_program)

# Populating the columns with dummy variables
df_complete_program <- df_complete_program %>%
  rowwise() %>%
  mutate(across(starts_with("prog_"), ~{
    programs <- get_program(ID, m_yr)
    prefixed_programs <- paste0("prog_", programs)
    if (cur_column() %in% prefixed_programs) 1 else 0
  })) %>%
  ungroup()


nrow(df_complete_program)


# Summary statistics by facility type 

comparison <- df_complete %>% 
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

comparison_sum <- comparison %>% 
  select(!c(loc, m_yr)) %>%
  tbl_summary(by = facility_type)

comparison_sum
# adding program (incomplete)

View(comparison)




# adding offense code bucket (problem with not enough sentencings?) (incomplete)


# adding # of times prisoner was incarcerated (incomplete)



# Beginning facility level analysis ---------------------------------------

# Average lsir, percent black, and average age, % male, region, dummy CCF, count, 

# Create dummy variables 
df_complete_dummy <- dummy_cols(
  df_complete,
  select_columns = c("facility_type", "facility_region"),
  remove_first_dummy = TRUE
  # remove_selected_columns = TRUE (Removes the original columns after creating the dummy variables)
)

View(df_complete_dummy)


facility_analysis <- df_complete_dummy %>% 
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

nrow(facility_analysis)

write.csv(facility_analysis, "working_facility_file.csv")

View(facility_analysis)

# check if the count is higher during certain months
# add count per facility




# not using rowwise() produces inaccurate results


