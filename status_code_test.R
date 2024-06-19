library("data.table")
library("readxl")
library("dplyr")
library("tidyr")
library("lubridate")

library("fastDummies")
library("ggplot2")
library("readr")


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

names(movements)

demographics <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "demographics")

ccc_cohort <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_cohort")

ccc_moves <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_moves")

sentencing <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "sentencing")

lsir <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "lsir")



# Preparing data frame -----------------------------------------------------


# Select columns and create month and year of arrival variables.
cc_counts_df <- ccc_moves %>% 
  # BRIAN (June 6th 2024): added 'movement_id'
  select(c('control_number', 'bed_date', 'status_code', 'status_description','status_date', 'location_to_code', 'location_from_code', 'movement_id'))  %>% 
# Drop duplicates by control number status code, and date. 
# Sometimes there are multiple entry records if the resident is enrolled in multiple programs.
  distinct(control_number, status_code, status_date, location_from_code, .keep_all = TRUE) %>% 
  # BRIAN (June 6th 2024): check for duplicate movement IDs
  distinct(movement_id, .keep_all = TRUE)




# If control number (resident) has that status code then keep it, then, for each resident, it gives a listing of their "total history"
view_status_case <- function(df_name, status_code_str){
  temp_df <- df_name
  temp_df$temp <- ifelse(temp_df$status_code == status_code_str, 1, 0)
  temp_df <- temp_df %>% group_by(control_number) %>% mutate(case = max(temp))
  result_df <- temp_df %>% filter(case == 1) %>% arrange(control_number, ymd(status_date)) %>% select(c('control_number', 'status_code', 'status_description', 'bed_date', 'status_date', 'location_from_code', 'location_to_code'))
  View(result_df)
  return(result_df)
}


get_status_code <- function(ID, check_date, mov_ID) {
  temp_dt <- as.data.table(cc_counts_df)
  # select status of the move record with associated parameters
  status_input <- temp_dt[(control_number == ID & status_date  == as.Date(strptime(check_date, format = '%m%d%Y')) & movement_id == mov_ID), status_code][1]
  return(status_input)
}

# Precompute sorted data tables
cc_counts_df$status_date <- as.Date(cc_counts_df$status_date, format = "%Y-%m-%d")
cc_counts_dt_2 <- as.data.table(cc_counts_df)
cc_counts_dt_sorted_desc <- setorder(copy(cc_counts_dt_2), -status_date)
cc_counts_dt_sorted_asc <- setorder(copy(cc_counts_dt_2), status_date)
cc_counts_dt_sorted_id_desc <- setorder(copy(cc_counts_dt_2), -movement_id)


check_mov_IDs <- function(ID, check_date) {
  # creating a vector of the associated movement IDs
  mov_IDs <- cc_counts_dt_sorted_id_desc[(control_number == ID & status_date == as.Date(strptime(check_date, format = '%m%d%Y'))), movement_id]
}

get_prev_status_date <- function(ID, max_date) {
  max_date <- as.Date(strptime(max_date, format = '%m%d%Y'))
  # select move records with associated parameters
  status_input <- cc_counts_dt_sorted_desc[(control_number == ID & status_date < max_date), status_date][1]
  
  if (anyNA(status_input)) { 
    status_input <- NA 
  } else {
    status_input <- format(status_input, '%m%d%Y')
  }
  
  # print(paste0('The previous status date is ', status_input))
  return(status_input)
}

get_prev_status_code <- function(ID, check_date) {
  check_date <- format(check_date, '%m%d%Y')
  prev_status_date <- get_prev_status_date(ID, check_date)
  
  # Return NA if prev_status_date is NA
  if (is.na(prev_status_date)) {
    return(NA)
  }
  
  # Select move records with associated parameters
  check_mov_IDs_res <- check_mov_IDs(ID, prev_status_date)
  
  status_input <- cc_counts_dt_sorted_desc[(control_number == ID & status_date == as.Date(strptime(prev_status_date, format = '%m%d%Y')) & movement_id == check_mov_IDs_res[1]), status_code][1]
  
  if (anyNA(status_input)) { 
    status_input <- NA 
  }
  
  return(status_input)
}

get_next_status_date <- function(ID, min_date) {
  min_date <- as.Date(strptime(min_date, format = '%m%d%Y'))
  # select move records with associated parameters
  status_input <- cc_counts_dt_sorted_desc[(control_number == ID & status_date > min_date), status_date][1]
  if (anyNA(status_input)) { 
    status_input <- NULL 
  } 
  else {
    status_input <- format(status_input, '%m%d%Y')
  }
  return(status_input)
}


get_next_status_code <- function(ID, check_date) {
  check_date <- format(check_date, '%m%d%Y')
  next_status_date <-  get_next_status_date(ID, check_date)
  # select move records with associated parameters
  check_mov_IDs_res <- check_mov_IDs(ID, next_status_date)
  status_input <- cc_counts_dt_sorted_desc[(control_number == ID & status_date == as.Date(strptime(next_status_date, format = '%m%d%Y')) & movement_id == check_mov_IDs_res[-1]), status_code][1]
  
  if (anyNA(status_input)) { 
    status_input <- NULL 
  } 
  return(status_input)
}




UDSC_df <- head(view_status_case(cc_counts_df, 'UDSC'), 20)

print(nrow(UDSC_df))

# UDSC_df$status_date <- as.Date(UDSC_df$status_date, format = "%Y-%m-%d")

UDSC_plot <- UDSC_df %>% 
  rowwise() %>%
  mutate(prev_code = get_prev_status_code(control_number, status_date), next_code = get_next_status_code(control_number, status_date))
  # select(control_number, status_code, prev_code, next_code, status_date)

View(UDSC_plot)
# Observing Transfer to County

# view_status_case(cc_counts_df, 'TRTC')
# 
# center_change <-  cc_counts_df %>% 
#   filter(location_to_code != 'NULL')
# 
# center_change_codes <- unique(center_change$status_code)
# 
# center_change_codes 
# 
# 
# 
# 
# 
# # Of the 34 unique codes, we are interested in 4 arrival codes: 
# # INRS - In Residence
# # TRRC - Transfer Received
# # RTRS - Return to Residence
# # DPWF - Return from DPW
# 
# cc_arrivals <- cc_counts_df %>% 
#   filter(status_code == 'INRS' | status_code == 'TRRC' | status_code == 'RTRS' | status_code == 'DPWF') %>% 
#   mutate(month_year = floor_date(status_date,"month")) %>%  
#   # Replaced the following: group_by(month_year, location_from_code) %>% summarise(arrivals=n(), .groups = 'drop') %>% ungroup() 
#   summarize(
#     arrivals = n(),
#     .by = c(month_year, location_from_code)
#   ) %>% 
#   # BRIAN (June 6th 2024): arrange does not seem to do anything. Perhaps this is because it is not a tibble anymore?
#   # arrange(desc(location_from_code)) %>% 
#   complete(month_year, location_from_code, fill = list(arrivals = 0)) %>%  
#   as.data.frame()
# 
# 
# # Adding transfer out, parole, etc by CCC/CCF month
# 
# departure_codes <- c('TRGH','PTST','DECN','ABSC','TRSC','UDSC','TTRN','DPWT','DC2P','SENC','ESCP','HOSP','AWOL','ATA','DECX','DECA','DECS','TRTC')
# 
# cc_departures <- cc_counts_df %>% 
#   filter(status_code  %in% departure_codes) %>% 
#   mutate(month_year=floor_date(status_date, unit='month')) %>% 
#   # Replaced the following: group_by(month_year, location_from_code)%>% summarise(departures=n(), .groups = 'drop') %>% ungroup() %>%
#   summarize (
#     departures = n(),
#     .by = c(month_year, location_from_code)
#   ) %>% 
#   complete(month_year, location_from_code, fill = list(departures=0))  %>%  
#   as.data.frame()
# 
# 
# # Merge departures and arrivals. Subtract to have net flow
# cc_flows <- left_join(cc_arrivals,cc_departures, by = c('month_year'='month_year', 'location_from_code'='location_from_code')) %>% 
#   replace_na(list(departures=0, arrivals=0))
# cc_flows <- cc_flows %>% 
#   mutate(net_arrivals = arrivals - departures)
# 
# cc_flows_by_month <- cc_flows %>% 
#   group_by(month_year) %>%
#   summarise(arrivals = sum(arrivals),departures = sum(departures), net_arrivals = sum(net_arrivals)) %>%
#   arrange(month_year) %>% mutate(month_year = ymd(month_year))
# 
# # Plot of net flow
# ggplot(data=cc_flows_by_month %>% 
#          filter(year(month_year) >= 2008 & year(month_year) < 2020), aes(x = month_year, y = arrivals)) + 
#   geom_point() + geom_line() + 
#   labs(title = "Arrivals to Pennsylvania Halfway Houses by Month", y='Arrivals', x='') +
#   scale_x_date(date_breaks = '1 year', date_labels = "%b-%Y") + 
#   theme(axis.text.x = element_text(angle = 90))
