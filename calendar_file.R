# working R file for creating the calendar file

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

# Precompute sorted data tables
cc_counts_dt_sorted_desc <- setorder(copy(cc_counts_dt), -status_date)
cc_counts_dt_sorted_asc <- setorder(copy(cc_counts_dt), status_date)

get_prev_status_date <- function(ID, max_date) {
  max_date <- as.Date(strptime(max_date, format = '%m%d%Y'))
  # select move records with associated parameters
  status_input <- cc_counts_dt_sorted_desc[(control_number == ID & status_date < max_date), status_date][1]
  
  if (anyNA(status_input)) { 
    status_input <- NULL 
  } 
  else {
    status_input <- format(status_input, '%m%d%Y')
  }
  return(status_input)
}


# Tests for function
# print(get_prev_status_date('004037', '10022012'))
# print(get_prev_status_date('004037', '05032011'))

get_next_status_date <- function(ID, min_date) {
  min_date <- as.Date(strptime(min_date, format = '%m%d%Y'))
  # select move records with associated parameters
  status_input <- cc_counts_dt_sorted_asc[(control_number == ID & status_date > min_date), status_date][1]
  
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

# Precompute sorted data tables
cc_counts_dt_sorted_id_desc <- setorder(copy(cc_counts_dt), -movement_id)

check_mov_IDs <- function(ID, check_date) {
  # creating a vector of the associated movement IDs
  mov_IDs <- cc_counts_dt_sorted_id_desc[(control_number == ID & status_date == as.Date(strptime(check_date, format = '%m%d%Y'))), movement_id]
}

# Test for function
# print(check_mov_IDs('003636', '10042012'))
# print(check_mov_IDs('003636', '11062012'))

# print(unique(cc_counts_df$status_code))
# count(is.na(cc_counts_df$status_code))

# Questions: UDSC, "TRGH" * "PTCE" *  
# "ESCP" * "ERR" * (714 days, 3 errors)  "TRTC" *  

# check_status_and_ID <- function(ID, check_date, mov_ID) {
#   temp_dt <- cc_counts_dt
#   # select status of the move record with associated parameters
#   status_input <- temp_dt[(control_number == ID & status_date  == as.Date(strptime(check_date, format = '%m%d%Y')) & movement_id == mov_ID), status_code][1]
#   # the resident is now in correctional facility (1)
#   if(status_input == 'TRSC') {
#     loc_code <- -1
#   }
#   # the resident is no longer in a CCC/CCF (dead, in correctional facility, escaped or paroled) (6)
#   else if(status_input %in% c('ESCP', 'DECN', 'DECX', 'DECA', 'DECS', 'ABSC')) {
#     loc_code <- -2
#   }
#   # code for temporary leave (13)
#   else if (status_input %in% c('TTRN', 'TRRC', 'DBOA', 'AWDT', 'DPWT', 'HOSP', 'AWOL', 'ATA', 'AWTR', 'AWDN', 'AWNR', 'PEND', 'PREJ', 'PWTH', 'DC2P')) {
#     loc_code <- -3
#   }
#   # code for release to street (2)
#   else if (status_input %in% c('SENC', 'PTST')) {
#   }
#   # all other codes do not result in a change of residence
#   # including INRS, TRRC, PRCH, RTRS, DPWF (5)
#   else {
#     loc_code <- temp_dt[(control_number == ID & status_date  == as.Date(strptime(check_date, format = '%m%d%Y')) & movement_id == mov_ID), location_from_code][1]
#   }
# }

check_status_and_ID <- function(ID, check_date, mov_ID) {
  temp_dt <- cc_counts_dt
  # select status of the move record with associated parameters
  status_input <- temp_dt[(control_number == ID & status_date  == as.Date(strptime(check_date, format = '%m%d%Y')) & movement_id == mov_ID), status_code][1]
  
  if (length(status_input) == 0 || is.na(status_input)) {
    print("status_input is NA or length 0")
    return(NA)
  }
  
  # the resident is now in correctional facility (1)
  if(status_input == 'TRSC') {
    loc_code <- -1
  }
  # the resident is no longer in a CCC/CCF (dead, in correctional facility, escaped or paroled) (6)
  else if(status_input %in% c('ESCP', 'DECN', 'DECX', 'DECA', 'DECS', 'ABSC')) {
    loc_code <- -2
  }
  # code for temporary leave (13)
  else if (status_input %in% c('TTRN', 'DBOA', 'AWDT', 'DPWT', 'HOSP', 'AWOL', 'ATA', 'AWTR', 'AWDN', 'AWNR', 'PEND', 'PREJ', 'PWTH', 'DC2P')) {
    loc_code <- -3
  }
  # code for release to street (2)
  else if (status_input %in% c('SENC', 'PTST')) {
    loc_code <- -4
  }
  # all other codes do not result in a change of residence
  # including INRS, TRRC, PRCH, RTRS, DPWF, 'TRRC' (5)
  else {
    loc_code <- temp_dt[(control_number == ID & status_date  == as.Date(strptime(check_date, format = '%m%d%Y')) & movement_id == mov_ID), location_from_code][1]  
  }
  
  if (length(loc_code) == 0 || is.na(loc_code)) {
    print("loc_code is NA or length 0")
    loc_code <- NA
  }
  
  return(loc_code)
}


# Test for function
# print(check_status_and_ID('003636', '10042012', '753517'))
# print(check_status_and_ID('003636', '12262012', '767244'))

# Main function

# populate_IDs <- function(ID, check_date, end_date) {
#   
#   
#   created_df <- data.frame(ID = ID, stringsAsFactors = FALSE)
#   
#   row.names(created_df) <- created_df$ID
#   
#   # Convert checking_date and ending_date to Date objects
#   
#   print(paste0('working on ID: ', ID))
#   
#   prev_status_date <- get_prev_status_date(ID, check_date)
#   
#   next_status_date <- get_next_status_date(ID, check_date)
#   
#   # Updating center 
#   
#   # should it be -5?? Or 0?	
#   current_loc <- -5
#   
#   while (compare_dates(end_date, check_date)) {
#     if (!is.null(prev_status_date)) {
#       check_mov_IDs_res <- check_mov_IDs(ID, prev_status_date)
#       current_loc <- check_status_and_ID(ID, prev_status_date, check_mov_IDs_res[1])
#     }
#     
# 
#     # To fill remaining days with appropriate code after an ID has no more 
#     # remaining records after 'check_date'
#     if(is.null(get_next_status_date(ID, check_date))) {
#       while (compare_dates(end_date, check_date)) {
#         created_df[ID, check_date] <- current_loc
#         check_date <- increment_date(check_date)
#       }
#     }
#     
#     else {
#       while (compare_dates(next_status_date, check_date) && compare_dates(end_date, check_date)) {
#         created_df[ID, check_date] <- current_loc
#         check_date <- increment_date(check_date)
#       }
#       
#       # update prev_status_date to check_date 
#       prev_status_date <- check_date
#       
#       # update next_status_date with get_next_status_date( )
#       next_status_date <- get_next_status_date(ID, check_date)
#     }
#   }
#   print("Done!")
#   return(created_df)
# }

populate_IDs <- function(ID, check_date, end_date) {
  tryCatch({
    created_df <- data.frame(ID = ID, stringsAsFactors = FALSE)
    row.names(created_df) <- created_df$ID
    
    prev_status_date <- get_prev_status_date(ID, check_date)
    next_status_date <- get_next_status_date(ID, check_date)
    current_loc <- -5
    
    while (compare_dates(end_date, check_date)) {
      if (!is.null(prev_status_date)) {
        check_mov_IDs_res <- check_mov_IDs(ID, prev_status_date)
        if (length(check_mov_IDs_res) == 0) {
          stop("check_mov_IDs_res is empty")
        }
        current_loc <- check_status_and_ID(ID, prev_status_date, check_mov_IDs_res[1])
      }
      
      if (is.null(current_loc)) {
        stop("current_loc is NULL")
      }
      
      if (is.null(get_next_status_date(ID, check_date))) {
        while (compare_dates(end_date, check_date)) {
          created_df[ID, check_date] <- current_loc
          check_date <- increment_date(check_date)
        }
      } else {
        while (compare_dates(next_status_date, check_date) && compare_dates(end_date, check_date)) {
          created_df[ID, check_date] <- current_loc
          check_date <- increment_date(check_date)
        }
        prev_status_date <- check_date
        next_status_date <- get_next_status_date(ID, check_date)
      }
    }
    return(created_df)
  }, error = function(e) {
    cat("Error in processing ID:", ID, "Error message:", e$message, "\n")
    return(NULL)
  })
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

unique_IDs_2

# apply
#  user  system elapsed 
# 14.847   0.650  15.517 (Jun 18th 2:50 pm)
# user  system elapsed 
# 12.180   0.558  12.780 (Jun 18th 3:00 pm)
 system.time({
  list_of_dts_2 <- lapply(unique_IDs_2, populate_IDs, check_date = '01012008', end_date = '01012021')
 final_dt_2 <- Reduce(rbind, list_of_dts_2)
 })

 View(final_dt_2)
 
 write.csv(final_dt_2, 'attemp1.csv')
# mclapply
#  user  system elapsed (Jun 18th 2:50 pm)
# 16.442   0.867   9.109 
# user  system elapsed 
# 13.912   0.761   7.721 (Jun 18th 3:00 pm)
numberOfCores <- detectCores()
system.time({
  list_of_dts_2 <- mclapply(unique_IDs_2, populate_IDs, check_date = '01012008', end_date = '01012021')
  final_dt_2 <- Reduce(rbind, list_of_dts_2)
})

View(final_dt_2)
# 
# 
# # Creating target dataframe -----------------------------------------------
# 
# unique_IDs <- cc_counts_df %>%
#   distinct(control_number) %>%
#   arrange(desc(control_number)) %>%
#   pull(control_number)
# 
# unique_IDs <- na.omit(unique_IDs)
# 
# # There are 80408 unique IDs
# length(unique_IDs)
# 
# list_of_dfs <- lapply(unique_IDs, populate_IDs, check_date = '01012008', end_date = '01012021')
# final_df <- Reduce(rbind, list_of_dfs)
# 
# 
# # write.csv(target_df_2, 'attemp1.csv')

unique_IDs <- ccc_moves %>%
  distinct(control_number) %>%
  arrange(desc(control_number)) %>%
  pull(control_number)

sampling <- sample(unique_IDs, 3000)

sampling <- sort(sampling)

list_of_dts <- lapply(sampling, populate_IDs, check_date = '01012008', end_date = '01012021')
calendar_file_use <- Reduce(rbind, list_of_dts)


write_csv(calendar_file_use, "calendar_file_use.csv")


View(calendar_file_use)

# Set the first column as row names
rownames(calendar_file_use) <- calendar_file_use[, 1]
# Remove the first column from the DataFrame
calendar_file_use <- calendar_file_use[, -1]


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
test_df4 <- pop_m_yr_df(calendar_file_use)
View(test_df4)
