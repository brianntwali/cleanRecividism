#install.packages("reshape2")
# library("reshape2")
library("data.table")
library("readxl")
library("dplyr")
library("tidyr")
library("lubridate")

library("fastDummies")
library("ggplot2")
library("readr")


# Loading data ------------------------------------------------------------

# Corrected file path with print(proper separator
#encripted_drive_path <- "/Volumes/Untitled/PA DOC/"
#Neil's path
encripted_drive_path <- "C:/Users/silveus/Documents/Data/PA DOC/"

movements <- read.csv(paste0(encripted_drive_path,"2021-22_Silveus_deidentified_prison_spells.csv"))

demographics <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "demographics")

ccc_cohort <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_cohort")

ccc_moves <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_moves")

sentencing <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "sentencing")

lsir <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "lsir")



# Comparing movements and ccc_moves ---------------------------------------

movements <- as.data.table(movements)
movements <- movements[control_number %in% c('001165', '001660', '002459', '002632'), !"control_inmate_mask"][order(control_number, move_date)]

ccc_moves <- as.data.table(ccc_moves)
ccc_moves <- ccc_moves[control_number %in% c('001165', '001660', '002459', '002632'), !c("control_inmate_mask", "movement_sequence_number", "region_from")][order(control_number, status_date, movement_id)]

View(movements)
View(ccc_moves)

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

# General functions -------------------------------------------------------


# If control number (resident) has that status code then keep it, then, for each resident, it gives a listing of their "total history"
isolate_status_case <- function(df_name, status_code_str){
  temp_df <- df_name
  temp_df$temp <- ifelse(temp_df$status_code == status_code_str, 1, 0)
  temp_df <- temp_df %>% group_by(control_number) %>% mutate(case = max(temp))
  result_df <- temp_df %>% filter(case == 1) %>% arrange(control_number, ymd(status_date)) %>% select(c('control_number', 'status_code', 'movement_id', 'bed_date', 'status_date', 'location_from_code'))
  return(result_df)
}

get_dt_input <- function(input_status_code) {
  dt_input <- isolate_status_case(cc_counts_df, input_status_code)
  dt_input <- setDT(dt_input)[order(control_number, status_date, movement_id)]
  suppressWarnings(
    dt_input$bed_date <- dt_input$bed_date %>% 
      as.numeric() %>%  
      as.Date(origin=as.Date("1899-12-30"))
  )
  return(dt_input)
}

refine_dt_per_ID  <- function(unique_id, dt_input, status_input) {
  dt_input <- dt_input[(control_number == unique_id)]
  dt_input <- dt_input[, 
    prev_code := paste0(shift(status_code, type = "lag"))][, 
    next_code := paste0(shift(status_code, type = "lead"))][, 
    duration_days := as.numeric(shift(status_date, type = "lead") - status_date) / 86400][, 
    center_changed := ifelse(shift(location_from_code, type = "lag") == location_from_code & shift(location_from_code, type = "lead") == location_from_code, "no", "yes")][, 
    bed_date_changed := ifelse(shift(bed_date, type = "lag") == shift(bed_date, type = "lead"), "no", "yes")
  ]
dt_input <- dt_input[(status_code == status_input)]
  
  
  return(dt_input)
}

get_unique_IDs <- function(dt_input) {
  dt_input <- dt_input[order(-control_number)]
  unique_IDs_code <- dt_input[, unique(control_number)]
  unique_IDs_code <- na.omit(unique_IDs_code)
  return(unique_IDs_code)
}

create_final_code_dt <- function(input_code) {
  
  input_dt <- get_dt_input(input_code)
  unique_IDs_code <- get_unique_IDs(input_dt)
  list_of_code_dts <- lapply(unique_IDs_code, refine_dt_per_ID, dt_input = input_dt, status_input = input_code)
  final_code_dt <- Reduce(rbind, list_of_code_dts)
  
  print("Proportion of bed date change: ")
  print(prop.table(table(final_code_dt$bed_date_changed)))
  
  print("Proportion of center change: ")
  print(prop.table(table(final_code_dt$center_changed)))
  
  print("Proportion of previous code: ")
  print(prop.table(table(final_code_dt$prev_code)))
  
  print("Proportion of next code: ")
  print(prop.table(table(final_code_dt$next_code)))
  
  # 167 days between PTCE and next status code
  print("Average # of days to next status change: ")
  print(
    mean(final_code_dt$duration_days, na.rm = TRUE)
  )
  return(final_code_dt)
}


# Results -----------------------------------------------------------------

# Observing Parole to Street (PTST) 
# There are 83264 records with this code (of which 57057 are unique)

final_PTST_dt <- create_final_code_dt('PTST')

nrow(final_PTST_dt)

nrow(cc_counts_df %>% 
       filter(status_code == 'PTST'))

View(final_PTST_dt)

unique_final_PTST_dt <- final_PTST_dt %>% 
  distinct(control_number, .keep_all = TRUE)

nrow(unique_final_PTST_dt)

# Observing Sentence Complete (SENC)
# There are 10127 records with this code (of which 10002 are unique)

final_SENC_dt <- create_final_code_dt('SENC')

nrow(final_SENC_dt)

unique_final_SENC_dt <- final_SENC_dt %>% 
  distinct(control_number, .keep_all = TRUE)

nrow(unique_final_SENC_dt)

# Observing Parole to Center (PTCE)

final_PTCE_dt <- create_final_code_dt('PTCE')

# Observing Discharge to parole (DC2P)

final_DC2P_dt <- create_final_code_dt('DC2P')

# View(final_DC2P_dt)
# View(isolate_status_case(cc_counts_df, 'DC2P'))

# Observing Unsuccessful Discharge (UDSC)

final_UDSC_dt <- create_final_code_dt('UDSC')
 
# Observing Transfer to County (TRTC)

final_TRTC_dt <- create_final_code_dt('TRTC')

# Observing Transfer Receiver (TRRC)

final_TRRC_dt <- create_final_code_dt('TRRC')


# Observing Transfer to State Correctional Institution (TRSC)

final_TRSC_dt <- create_final_code_dt('TRSC')

# View(final_TRSC_dt)
# nrow(
#   final_TRSC_dt
# )

# Observing Awaiting Transfer (AWTR)

final_AWTR_dt <- create_final_code_dt('AWTR')

# nrow(
#   final_AWTR_dt[prev_code == 'PTST']
# )

# Observing Awaiting Transfer Denied (AWDN)

final_AWDN_dt <- create_final_code_dt('AWDN')

# nrow(
#   final_AWDN_dt
# )

#  (TTRN)

final_TTRN_dt <- create_final_code_dt('TTRN')

# (AWDT)

final_AWDT_dt <- create_final_code_dt('AWDT')

# (ERR) - only 3 instances

final_ERR_dt <- create_final_code_dt('ERR')

nrow(final_ERR_dt)

# (DBOA)

final_DBOA_dt <- create_final_code_dt('DBOA')
nrow(final_DBOA_dt)

# (TRGH)

final_TRGH_dt <- create_final_code_dt('TRGH')

# Observing Pending (PEND)

final_PEND_dt <- create_final_code_dt('PEND')

# 
# PTCE_dt_long <- melt(PTCE_dt, id = "status_code")
# 
# # There are 623 PTCE code entries
# nrow(PTCE_dt_long)
# 
# 
# PTCE_plot_long <- PTCE_dt_long %>% 
#   ggplot(aes(x = variable, fill = value)) +
#   geom_bar() 
# 
# PTCE_plot_long
