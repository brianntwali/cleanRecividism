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

new_arrivals <- ccc_moves %>% 
  distinct(movement_id, .keep_all = TRUE) %>% 
  filter(status_code == 'INRS') %>% 
  select(c('control_number','status_date',  'location' = 'location_from_code'))
  

View(new_arrivals)

unique_fac_codes <- ccc_cohort %>% 
  distinct(center_code, .keep_all = TRUE) %>% 
  pull(center_code)

INRS_codes <- new_arrivals %>% 
  distinct(location, .keep_all = TRUE) %>% 
  pull(location)

# There are 23 codes missing from ccc_cohort that are in ccc_moves rows with INRS


disjointed <- setdiff(INRS_codes, unique_fac_codes)

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


new_arrivals_filled <- new_arrivals %>%
  filter(!(location %in% disjoint1)) %>% 
  rowwise() %>%
  mutate(
    lsir = get_lsir(control_number, status_date),
    age = get_age(control_number, status_date),
    race = get_race(control_number),
    sex = get_sex(control_number),
    facility_type = get_facility_type(location)
  )  %>%
  ungroup()

View(new_arrivals_filled)


lsir_flow <- new_arrivals_filled %>% 
  mutate(
    month_year = floor_date(status_date, unit = 'month')
  ) %>% 
  filter(year(month_year) > 2007 & year(month_year) < 2021) %>% 
  summarise(
    avg_lsir = mean(lsir, na.rm = TRUE),
    .by = c(facility_type, month_year)
  ) %>%
  arrange(month_year) %>% 
  mutate(month_year=ymd(month_year)) %>% 
  as.data.frame()


View(lsir_flow)

ggplot(lsir_flow, aes(x = month_year, y = avg_lsir, colour = facility_type)) +
  geom_point() +
  geom_line()


race_flow <- new_arrivals_filled %>% 
  mutate(
    month_year = floor_date(status_date, unit = 'month')
  ) %>% 
  filter(year(month_year) > 2007 & year(month_year) < 2021) %>% 
  summarise(
    perc_black = round(mean(race == 'BLACK') * 100, 3), 
    .by = c(facility_type, month_year)
  ) %>%
  arrange(month_year) %>% 
  mutate(month_year=ymd(month_year)) %>% 
  as.data.frame()

ggplot(race_flow, aes(x = month_year, y = perc_black, colour = facility_type)) +
  geom_point() +
  geom_line()


age_flow <- new_arrivals_filled %>% 
  mutate(
    month_year = floor_date(status_date, unit = 'month')
  ) %>% 
  filter(year(month_year) > 2007 & year(month_year) < 2021) %>% 
  summarise(
    avg_age = mean(age, na.rm = TRUE),
    .by = c(facility_type, month_year)
  ) %>%
  arrange(month_year) %>% 
  mutate(month_year=ymd(month_year)) %>% 
  as.data.frame()

ggplot(age_flow, aes(x = month_year, y = avg_age, colour = facility_type)) +
  geom_point() +
  geom_line()


male_flow <- new_arrivals_filled %>% 
  mutate(
    month_year = floor_date(status_date, unit = 'month')
  ) %>% 
  filter(year(month_year) > 2007 & year(month_year) < 2021) %>% 
  summarise(
    perc_male = round(mean(sex == 'MALE') * 100, 3),
    .by = c(facility_type, month_year)
  ) %>%
  arrange(month_year) %>% 
  mutate(month_year=ymd(month_year)) %>% 
  as.data.frame()

ggplot(male_flow, aes(x = month_year, y = perc_male, colour = facility_type)) +
  geom_point() +
  geom_line()
