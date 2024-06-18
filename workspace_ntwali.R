# working R file for cleaning PA DOC data by BRIAN NTWALI

library("data.table")
library("readxl")
library("dplyr")
library("tidyr")
library("lubridate")

library("fastDummies")
library("ggplot2")
library(readr)


# Loading data ------------------------------------------------------------

# Corrected file path with proper separator
encripted_drive_path <- "/Volumes/Untitled/PA DOC/"

# Verify the correct file path
file_path <- paste0(encripted_drive_path, "2021-22_Silveus_deidentified_prison_spells.csv")
print(file_path)  # This should print the full file path

# Read the CSV file using the corrected file path
movements <- read.csv(file_path)


movements <- read.csv("/Volumes/Untitled/PA DOC/2021-22_Silveus_deidentified_prison_spells.csv")

# Change column name; BRIAN (June 6th 2024):this has been changed so this instruction is no longer necessary 6/3/2024
#movements <- movements %>% rename("control_number" = "ï..control_number" )

demographics <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "demographics")

ccc_cohort <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_cohort")

ccc_moves <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_moves")

sentencing <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "sentencing")

lsir <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "lsir")


# Select columns and drop duplicates by control number status code, and date. 
# Sometimes there are multiple entry records if the resident is enrolled in multiple programs.

cc_counts_df <- ccc_moves %>% 
  # BRIAN (June 6th 2024): added 'movement_id'
  select(c('control_number', 'bed_date', 'status_code', 'status_description','status_date', 'location_to_code', 'location_from_code', 'movement_id')) %>% 
  distinct(control_number, status_code, status_date, location_from_code, .keep_all = TRUE) %>% 
  # BRIAN (June 6th 2024): check for duplicate movement IDs
  distinct(movement_id, .keep_all = TRUE)

# transform into data.table for efficiency
cc_counts_dt <- as.data.table(cc_counts_df)


# Creating Functions ------------------------------------------------------

# Function to increment a date string by 1 day and return as a date string in MMDDYYYY format
increment_date <- function(date_string) {
  date <- as.Date(strptime(date_string, format = "%m%d%Y"))
  incremented_date <- date + 1
  incremented_date_string <- format(incremented_date, "%m%d%Y")
  return(incremented_date_string)
}

# Tests for function
# example_date_string <- "01312008"
# new_date_string <- increment_date(example_date_string)
# print(new_date_string)


compare_dates <- function(date_one, date_two) {
  mod_date_one <- as.Date(strptime(date_one, format = '%m%d%Y'))
  mod_date_two <- as.Date(strptime(date_two, format = '%m%d%Y'))
  if (mod_date_one > mod_date_two) {
    result <- TRUE
  }
  else {
    result <- FALSE
  }
  return(result)
}

# Tests for function
# compare_dates('01082020', '02012028')
# compare_dates('12082020', '02012028')

get_prev_status_date <- function(ID, max_date) {
  temp_dt <- setorder(cc_counts_dt, -status_date) 
  max_date <- as.Date(strptime(max_date, format = '%m%d%Y'))
  # select move records with associated parameters
  status_input <- temp_dt[(control_number == ID & status_date < max_date), status_date][1]
  
  if (anyNA(status_input)) { 
    status_input <- NULL 
  } 
  else {
    status_input <- format(status_input, '%m%d%Y')
  }
  return(status_input)
}


# Tests for function
print(get_prev_status_date('004037', '10022012'))
# print(get_prev_status_date('004037', '05032011'))

get_next_status_date <- function(ID, min_date) {
  temp_dt <- setorder(cc_counts_dt, status_date)
  min_date <- as.Date(strptime(min_date, format = '%m%d%Y'))
  # select move records with associated parameters
  status_input <- temp_dt[(control_number == ID & status_date > min_date), status_date][1]
  
  if (anyNA(status_input)) { 
    status_input <- NULL 
  } 
  
  else {
    status_input <- format(status_input, "%m%d%Y")
  }
  return(status_input)
}


# Tests for function
# get_next_status_date('003134', '06212010')
# get_next_status_date('003134', '11162010')

check_mov_IDs <- function(ID, check_date) {
  temp_dt <- setorder(cc_counts_dt, -movement_id)
  # creating a vector of the associated movement IDs
  mov_IDs <- temp_dt[(control_number == ID & status_date == as.Date(strptime(check_date, format = '%m%d%Y'))), movement_id]
}

# Test for function
# print(check_mov_IDs('003636', '10042012'))
# print(check_mov_IDs('003636', '11062012'))


check_status_and_ID <- function(ID, check_date, mov_ID) {
  temp_dt <- cc_counts_dt
  # select status of the move record with associated parameters
  status_input <- temp_dt[(control_number == ID & status_date  == as.Date(strptime(check_date, format = '%m%d%Y')) & movement_id == mov_ID), status_code][1]
  # the resident is no longer in a CCC/CCF (dead, in correctional facility, escaped or paroled)
  if(status_input %in% c('ESCP', 'DECN', 'PTST', 'DC2P', 'SENT', 'DECX', 'DECA', 'DECS', 'PTST', 'TRSC')) {
    loc_code <- -1
  }
  # code for temporary leave
  else if (status_input %in% c('TTRN', 'DPWT', 'HOSP', 'AWOL', 'ATA')) {
    loc_code <- -2
  }
  # all other codes do not result in a change of residence
  else {
    loc_code <- temp_dt[(control_number == ID & status_date  == as.Date(strptime(check_date, format = '%m%d%Y')) & movement_id == mov_ID), location_from_code][1]
  }
}


# Test for function
# print(check_status_and_ID('003636', '10042012', '753517'))
# print(check_status_and_ID('003636', '12262012', '767244'))

# Main function

populate_IDs <- function(ID, checking_date, ending_date) {
  
  created_dt <- data.table(ID = ID, stringsAsFactors = FALSE)
  
  row.names(created_dt) <- created_dt$ID
  
  print(paste0('working on ID: ', ID))
  
  # checking_date should be '01012008'
  check_date <- checking_date
  
  # print(paste0('The checking date is: ', check_date))
  
  # ending_date should be '01012021'
  end_date <- ending_date 
  
  # print(paste0('The ending date is: ', end_date))
  
  prev_status_date <- get_prev_status_date(ID, check_date)
  
  # print(paste0('The previous status date is: ', prev_status_date))
  
  next_status_date <- get_next_status_date(ID, check_date)
  
  # print(paste0('The next status date is: ', next_status_date))
  
  # Updating center 
  
  # should it be -5?? Or 0?	
  current_loc <- -5
  
  while (compare_dates(end_date, check_date)) {
    if (!is.null(prev_status_date)) {
      check_mov_IDs_res <- check_mov_IDs(ID, prev_status_date)
      current_loc <- check_status_and_ID(ID, prev_status_date, check_mov_IDs_res[1])
      # print(paste0('There is a previous status date. The current location is: ', current_loc))
    }
    
    # To fill remaining days with appropriate code after an ID has no more 
    # remaining records after 'check_date'
    if(is.null(get_next_status_date(ID, check_date))) {
      while (compare_dates(end_date, check_date)) {
        created_dt[ID, check_date] <- current_loc
        check_date <- increment_date(check_date)
        # print(paste0('There is no next status date. The current date is: ', check_date))
      }
    }
    
    else {
      while (compare_dates(next_status_date, check_date) && compare_dates(end_date, check_date)) {
        created_dt[ID, check_date] <- current_loc
        check_date <- increment_date(check_date)
        # print(paste0('There is a next status date. The current date is: ', check_date))
      }
      
      # update prev_status_date to check_date 
      prev_status_date <- check_date
      # print(paste0('Updating the previous status date... It is now: ', prev_status_date))
      
      
      # update next_status_date with get_next_status_date( )
      next_status_date <- get_next_status_date(ID, check_date)
      # print(paste0('Updating the next status date... It is now: ', next_status_date))
      
    }
  }
  # print(paste0('Final previous date for ID ', ID, ' is ', prev_status_date))
  
  return(as.data.table(created_dt))
}




# Test for main function --------------------------------------------------

unique_IDs_2 <- cc_counts_df %>% 
  filter(control_number %in% c('002632','003134', '003226', '003636', '004037', '002459')) %>% 
  distinct(control_number) %>% 
  arrange(desc(control_number)) %>% 
  pull(control_number)

unique_IDs_2 <- na.omit(unique_IDs_2)

# There are 6 unique IDs
length(unique_IDs_2)


# date_sequence_2 <- seq(as.Date("2009-01-01"), as.Date("2013-01-01"), by = "day")
# 
# # Format the dates as MMDDYYYY
# formatted_dates_2 <- format(date_sequence_2, "%m%d%Y")
# 
# # There are 730 days 
# length(formatted_dates_2)


list_of_dfs_2 <- lapply(unique_IDs_2, populate_IDs, checking_date = '01012009', ending_date = '01012013')
final_df_2 <- Reduce(rbind, list_of_dfs)

View(final_df_2)


# Creating target dataframe -----------------------------------------------

unique_IDs <- cc_counts_df %>%
  distinct(control_number) %>%
  arrange(desc(control_number)) %>%
  pull(control_number)

unique_IDs <- na.omit(unique_IDs)

# There are 80408 unique IDs
length(unique_IDs)

list_of_dfs <- lapply(unique_IDs, populate_IDs, checking_date = '01012008', ending_date = '01012021')
final_df <- Reduce(rbind, list_of_dfs)


# target_df_2 <- data.frame(ID = unique_IDs_2, stringsAsFactors = FALSE)
# 
# # Add columns for each date
# for (date in formatted_dates_2) {
#   target_df_2[[date]] <- NA # Initialize each column with NA 
# }
# 
# # Set row names to IDs for easier reference
# row.names(target_df_2) <- target_df_2$ID
# 
# target_df_2 <- populate_IDs(target_df_2, unique_IDs_2, '01012009', '01012013')
# 
# View(target_df_2)
# 
# 
# write.csv(target_df_2, 'attemp1.csv')
