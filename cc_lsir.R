library("stringr")
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

ccc_cohort <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_cohort")

ccc_moves <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "ccc_moves")

lsir <- read_xlsx(paste0(encripted_drive_path,"2021-22_Silveus_deidentified.xlsx"),sheet = "lsir")


# a time series graph of lsir for CCCs compared CCFs, specifically looking 
# for a jump or a change in trend for CCFs around 2013. Note, we could do 
# this anytime! I would take all the new "in-residence" codes from the ccc_moves
# file and go find the lsir associated. Then, average the lsir by month and 
# whether it was a ccc or ccf. This would be the average risk score of newly
# entering residents by month.

INRS_moves <- ccc_moves %>% 
  distinct(movement_id, .keep_all = TRUE) %>% 
  filter(status_code == 'INRS') %>% 
  select(c('control_number', 'status_date', 'location_from_code')) 
  
 
View(INRS_moves)

get_lsir_2 <- function(id, current_date) {
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

unique_facilities_codes <- ccc_cohort %>% 
  distinct(center_code, .keep_all = TRUE) %>% 
  pull(center_code)

INRS_codes <- INRS_moves %>% 
  distinct(location_from_code, .keep_all = TRUE) %>% 
  pull(location_from_code)

# There are 16 codes missing from ccc_cohort that are in ccc_moves
# There amount to 946 of 157462 rows in INRS_moves (hence we can't determine
# if they are a CCC or CCF) 

disjoint1 <- setdiff(INRS_codes, unique_facilities_codes)



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

INRS_with_lsir <- INRS_moves %>%
  filter(!(location_from_code %in% disjoint1)) %>% 
  rowwise() %>%
  mutate(
    lsir = get_lsir_2(control_number, status_date),
    type = get_facility_type(location_from_code)
  )  %>%
  ungroup()


View(INRS_with_lsir)     

lsir_flow <- INRS_with_lsir %>% 
  mutate(
    month_year = floor_date(status_date, unit = 'month')
  ) %>% 
  filter(year(month_year) > 2007 & year(month_year) < 2021) %>% 
  summarise(
    avg_lsir = mean(lsir, na.rm = TRUE),
    .by = c(type, month_year)
  ) %>%
  arrange(month_year) %>% 
  mutate(month_year=ymd(month_year)) %>% 
  as.data.frame()


View(lsir_flow)

ggplot(lsir_flow, aes(x = month_year, y = avg_lsir, colour = type)) +
  geom_point() +
  geom_line()
