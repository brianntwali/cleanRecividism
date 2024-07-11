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
  list_ids_dfs <- lapply(ids, get_id_m_yr_data, input_cal_file = cal_file, input_date = m_yr_rows)
  fill_df_for_m_yr <- Reduce(rbind, list_ids_dfs)
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
  list_of_dfs <- lapply(years, collect_m_yr_data, ids = extracted_ids, cal_file = og_cal_file)
  m_yr_df <- Reduce(rbind, list_of_dfs) 
  return(m_yr_df)
}

# run main function
test_df3 <- pop_m_yr_df(calendar_file)
View(test_df3)




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

unique_facilities <- ccc_cohort %>% 
  distinct(facility, .keep_all = TRUE) %>% 
  select(center_code, facility, region_code)


get_facility_type <- function(loc) {
  facility <- unique_facilities %>% 
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
  reg_code <- unique_facilities %>% 
    filter(center_code == loc) %>% 
    pull(region_code)
  return(reg_code)
}


# Adding further data to df -----------------------------------------------

df_complete <- test_df3 %>% 
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


View(df_complete)


# adding program (incomplete)


unique_programs <- ccc_moves %>% 
  distinct(program_code) %>% 
  pull(program_code)

length(unique_programs)

get_program <- function(id, current_date) {
  
}


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

View(facility_analysis)

write.csv(facility_analysis, "working_facility_file.csv")


# check if the count is higher during certain months
# add count per facility




# not using rowwise() produces inaccurate results
# df_with_age <- test_df3 %>% 
#   rowwise() %>%
#   mutate(
#     age = get_age(ID, m_yr),
#     lsir = get_lsir(ID, m_yr)
#   )  %>%
#   ungroup()

# tests
# print(get_lsir('079755', '01012008'))
# print(get_lsir('094683', '01012008'))
# 
# df_with_lsir <- df_with_age %>% 
#   rowwise() %>%
#   mutate(
#     lsir = get_lsir(ID, m_yr)
#   )  %>%
#   ungroup()
# 
# View(df_with_lsir)

# adding race (later -> % black)



# df_with_race <- df_with_age %>% 
#   rowwise() %>%
#   mutate(
#     race = get_race(ID)
#   )  %>%
#   ungroup()
# 
# View(df_with_race)


# adding sex
# (later -> % male) (20,000 records, duplicate entries and all, are female in demographic)


# df_with_sex <- df_with_race %>% 
#   rowwise() %>%
#   mutate(
#     sex = get_sex(ID)
#   )  %>%
#   ungroup()
# 
# 
# View(df_with_sex)


# Adding CCC/CCF and region



# df_with_CCF_region <- df_with_sex %>% 
#   rowwise() %>%
#   mutate(
#     facility_type = get_facility_type(loc),
#     facility_region = get_facility_region(loc)
#   )  %>%
#   ungroup()

# 
# View(df_with_CCF_region)


