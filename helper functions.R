

# Creating Functions


# Calendar functions ------------------------------------------------------


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


check_mov_IDs <- function(ID, check_date) {
  # creating a vector of the associated movement IDs
  mov_IDs <- cc_counts_dt_sorted_id_desc[(control_number == ID & status_date == as.Date(strptime(check_date, format = '%m%d%Y'))), movement_id]
}

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


# Facility level analysis functions --------------------------------------


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


get_program <- function(id, current_date) {
  
}





