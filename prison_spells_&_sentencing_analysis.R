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


movements <- as.data.table(movements)
movements <- movements[order(control_number, move_date)]

# Creating a sample of random IDs and filtering data tables for these IDs

unique_IDs <- ccc_moves %>%
    distinct(control_number) %>%
    arrange(desc(control_number)) %>%
    pull(control_number)

# Creating Functions ------------------------------------------------------


isolate_move_descripts <- function(df_name, move_descript_vector){
  temp_df <- df_name
  temp_df$temp <- ifelse(temp_df$move_description %in% move_descript_vector, 1, 0)
  temp_df <- temp_df %>% group_by(control_number) %>% mutate(case = max(temp))
  result_df <- temp_df %>% filter(case == 1) %>% arrange(control_number, ymd(move_date)) %>% select(c('control_number', 'move_date', 'move_description', 'move_code', 'sentence_status_code', 'sentence_status_description'))
  return(result_df)
}

isolate_sent_descripts <- function(df_name, sent_descript_vector){
  temp_df <- df_name
  temp_df$temp <- ifelse(temp_df$sentence_status_description %in% sent_descript_vector, 1, 0)
  temp_df <- temp_df %>% group_by(control_number) %>% mutate(case = max(temp))
  result_df <- temp_df %>% filter(case == 1) %>% arrange(control_number, ymd(move_date)) %>% select(c('control_number', 'move_date', 'move_description', 'move_code', 'sentence_status_code', 'sentence_status_description'))
  return(result_df)
}

increment_date <- function(date_string) {
  date <- as.Date(strptime(date_string, format = "%m%d%Y"))
  incremented_date <- date + 1
  incremented_date_string <- format(incremented_date, "%m%d%Y")
  return(incremented_date_string)
}
# Function to increment a date string by 1 day and return as a date string in MMDDYYYY format

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

cc_counts_df <- ccc_moves %>% 
  select(c('control_number', 'bed_date', 'status_code', 'status_description','status_date', 'location_to_code', 'location_from_code', 'movement_id')) %>% 
  distinct(control_number, status_code, status_date, location_from_code, .keep_all = TRUE) %>% 
  distinct(movement_id, .keep_all = TRUE)

# transform into data.table for efficiency
cc_counts_dt <- as.data.table(cc_counts_df)

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

# Precompute sorted data tables
cc_counts_dt_sorted_id_desc <- setorder(copy(cc_counts_dt), -movement_id)

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

# Main function

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


# Creating dataframe to fill later ----------------------------------------

sampling <- sample(unique_IDs, 10)

sampling <- sort(sampling)

list_of_dts <- lapply(sampling, populate_IDs, check_date = '01012008', end_date = '01012021')
calendar_file <- Reduce(rbind, list_of_dts)

View(calendar_file)

write_csv(calendar_file, "attempt2.csv")

# Compare to movement file 

sample_movements  <- movements %>% 
  filter(control_number %in% sampling)

View(sample_movements)


# Testing move_descriptions and sentence_status_descriptions --------------



# ENTRY: get all move descriptions of initial SCI entry 
# (and check if these occur anywhere else, i.e. when they recidivate)
# Answer: there are only 500 instances of "Add    -  Court Commitment", "Add    -  Administrative" 
# and "Add    -  Detetioner" in the general df that weren't the first instances. 

entry_descripts <- movements %>% 
  arrange(control_number, move_date) %>% 
  distinct(control_number, .keep_all = TRUE) # %>% 

View(entry_descripts)

unique_entry_descripts <- entry_descripts %>% 
  # move_code could be used for brevity
  distinct(move_description, .keep_all = TRUE) %>% 
  pull(move_description)

# check if nrows(entry_descripts) is equal to the nrows of movement filtered for those (unique) status descriptions 
# (i.e. if this is a first entry code or not - and honestly in case we can only observe first entries)

print(unique_entry_descripts)

unique_entry_descripts <- unique_entry_descripts[! unique_entry_descripts %in% c("Change -  Status Change", "Delete -  Discharge/Delete", 
                                                                                 "Delete -  Administrative", "Change -  To Other Institution Or CCC", 
                                                                                 "Add    -  Parole Violator","Add    -  Other - Use Sparingly",
                                                                                 "Add    -  WRIT/ATA (>1Year)", "Change -  Send Temporary Transfer",
                                                                                 "Add    -  Out-Of-State Probation/Parole Violator","Add    -  County Transfer", "")]

num_entry_descripts <- movements %>% 
  filter(move_description %in% unique_entry_descripts)

nrow(num_entry_descripts)

nrow(entry_descripts)


# DEPARTURE 

unique_depart_descripts <- movements %>% 
  arrange(control_number, desc(move_date)) %>% 
  distinct(control_number, .keep_all = TRUE) %>% 
  distinct(sentence_status_description, .keep_all = TRUE) %>%   
  pull(sentence_status_description)

print(unique_depart_descripts)

entry_dt <- isolate_move_descripts(movements, unique_entry_descripts)

senc_dt <- isolate_sent_descripts(movements, c("Sentence Complete"))

nrow(senc_dt)

nrow(departing_dt)

View(cc_counts_dt %>% 
       arrange(control_number, status_date, desc(movement_id)))

depart_descripts <- movements %>% 
  arrange(control_number, desc(move_date)) %>% 
  distinct(control_number, .keep_all = TRUE) %>% 
  distinct(sentence_status_description, .keep_all = TRUE) %>%   
  pull(sentence_status_description)

depart_descripts

View(ccc_moves %>% 
       arrange(status_date, movement_id) %>% 
       filter(control_number == '270198'))
      
class(movements$move_code)

movements_violations <- movements %>% 
  filter(move_description == 'Add    -  Parole Violator')

nrow(movements_violations)

unique_movements_violations <- movements_violations %>% 
  distinct(control_number, .keep_all = TRUE)

nrow(unique_movements_violations)

View(unique_movements_violations)

View(movements)

# data set where only control numbers that include 
cc_movements_preceding <- movements %>% 
  group_by(control_number) %>%
  filter(!is.na(as.numeric(lag(move_to_location))) | !is.na(as.numeric(move_to_location))) %>%
  ungroup()

View(cc_movements_preceding) 
     # %>% 
     #   distinct(control_number, .keep_all = TRUE))

cc_movements_violations <- cc_movements_preceding %>% 
  filter(move_description == 'Add    -  Parole Violator') 

View(cc_movements_violations)

overall_avg_num_violations <- movements_violations %>% 
  summarize(
    num_violations = mean(n()),
    .by = control_number
  ) %>% 
  summarize(
    avg_num_violations = mean(num_violations)
  )


overall_avg_num_violations

# Checking if the # of IDs is equal in prison spells and ccc_moves
# Answer: All IDs in ccc_moves are present in prison_spells except for 13 IDs

unique_IDs_moves <- ccc_moves %>% 
  distinct(control_number) %>% 
  arrange(desc(control_number)) %>% 
  pull(control_number)

unique_IDs_spells <- movements %>% 
  distinct(control_number) %>% 
  arrange(desc(control_number)) %>% 
  pull(control_number)

length(unique_IDs_moves)

length(unique_IDs_spells)

disjoint <- setdiff(unique_IDs_spells, unique_IDs_moves)

length(disjoint)


# Exploring "Sentencing" file ---------------------------------------------

# Findings: There 6129 unique IDs that received more than one sentence and of these
# the average time between sentences was 6 years (~ 2256.56 days)

sentencing <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "sentencing")

nrow(sentencing)

sentencing <- sentencing %>% 
  mutate(sentence_date = as.Date(sentence_date, format = "%Y%m%d"),
       sentence_date = format(sentence_date, "%d/%m/%Y")) # %>% 
  # mutate(sentence_date = dmy(sentence_date))

View(sentencing)

sentencing$sentence_date <- dmy(sentencing$sentence_date)

more_sentencing <- sentencing %>% 
  group_by(control_number) %>% 
  filter(n() > 1) %>% 
  arrange(sentence_date) %>%
  mutate(time_to_next_sentence = lead(sentence_date) - sentence_date) %>%
  ungroup() %>% 
  arrange(control_number)
  
View(more_sentencing)

count_more_sentencing <- more_sentencing %>% 
  distinct(control_number) %>% 
  nrow()

print(count_more_sentencing)

overall_avg_time_to_next_sentence <- more_sentencing %>%
  summarize(avg_time_to_next_sentence = mean(time_to_next_sentence, na.rm = TRUE))

print(overall_avg_time_to_next_sentence)

min_time_to_next_sentence <- more_sentencing %>%
  summarize(min_time_to_next_sentence = min(time_to_next_sentence, na.rm = TRUE))

print(min_time_to_next_sentence)
