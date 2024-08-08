# Brian's Github Path
# encripted_drive_path <- 
# Neil's PC Github Path
github_path <- "C:/Users/silveus/Documents/github projects/cleanRecividism/"

setwd(github_path)

# Pull functions from helper functions.R --------------------------------------------------
source("helper functions.R", echo = TRUE)

# Creating calendar file --------------------------------------------------



print(head(unique_IDs, 10))

# Create a cluster with the number of available cores
num_cores <- detectCores() - 1
cl <- makeCluster(num_cores)

#define populate_IDs2 with dates hard coded
populate_IDs2 <- function(x){
  return(populate_IDs(x, check_date = '01012008', end_date = '01012021'))
}

# Check if the dataset is available on all worker nodes. Use for diagnostics
check_dataset <- function(dataset) {
  exists(dataset, where = .GlobalEnv)
}

# Export the required libraries and functions to each worker node

# Export functions and required variables to the cluster
export_fns <- list("check_dataset","populate_IDs2","populate_IDs","get_prev_status_date","get_next_status_date","check_mov_IDs","check_status_and_ID","compare_dates","increment_date")
export_datasets <- prelim_datasets

# Define a function to load packages
load_packages <- function(packages) {
  lapply(packages, library, character.only = TRUE)
}

#  List of packages to export to cluster
export_packages <- list("parallel","gtsummary", "stringr", "schoolmath", "data.table", "readxl", "dplyr", "tidyr", "lubridate", "fastDummies", "ggplot2", "readr")

# Export packages to cluster
clusterCall(cl,load_packages,export_packages)

# Export datasets and functions to cluster
clusterExport(cl, c(export_datasets,export_fns))

#Run paralleled code to populate_IDs
system.time({
  batch_size <- 1000
  num_batches <- ceiling(length(unique_IDs)/batch_size)
  
  for (i in seq_len(num_batches)){
    start_index <- (i-1)*batch_size+1
    end_index <- min(i*batch_size, length(unique_IDs))
    subset_ids <- unique_IDs[start_index:end_index]
    
    result_subset <- parLapply(cl, subset_ids, populate_IDs2) 
    result_subset <- do.call(rbind,result_subset)
    saveRDS(result_subset, file = paste0("C:/Users/silveus/Documents/PA DOC/intermediate data/result_",start_index,"_to_",end_index,".rds"))  
  }
})
# end cluster to free up cores
stopCluster(cl)

# Combine saved results into a single object if needed
final_calendar_file <- data.frame()
for (i in seq_len(num_batches)) {
  start_index <- (i - 1) * batch_size + 1
  end_index <- min(i*batch_size, length(unique_IDs))
  result_subset <- readRDS(paste0("C:/Users/silveus/Documents/PA DOC/intermediate data/result_",start_index,"_to_",end_index,".rds"))
  final_calendar_file <- rbind(final_calendar_file, result_subset)
}



# Set the first column as row names
rownames(final_calendar_file) <- final_calendar_file[, 1]
# Remove the first column from the dataframe
final_calendar_file <- final_calendar_file[, -1]
saveRDS(final_calendar_file,"C:/Users/silveus/Documents/PA DOC/intermediate data/calander_file.rds")

# Creating main dataframe -------------------------------------------------

if (exists("final_calendar_file")==0) {
  print("Loading final_calendar_file")
  final_calendar_file <- readRDS("C:/Users/silveus/Documents/PA DOC/intermediate data/calander_file.rds")  
}else{
  print("final_calendar_file already exists")
}



unique_facilities <- ccc_cohort %>% 
  distinct(facility, .keep_all = TRUE) %>% 
  select(center_code, facility, region_code)

#Create test dataset
final_calendar_file_test <-final_calendar_file %>% head(11)

#Run paralleled code to create main_df
system.time({
  batch_size <- 1000
  num_batches <- ceiling(nrow(final_calendar_file)/batch_size)
  
  for (i in seq_len(num_batches)){
    start_index <- (i-1)*batch_size+1
    end_index <- min(i*batch_size, nrow(final_calendar_file))
    subset_ids <- final_calendar_file[start_index:end_index,]
    
    result_subset <- pop_m_yr_df(subset_ids)
    saveRDS(result_subset, file = paste0("C:/Users/silveus/Documents/PA DOC/intermediate data/main_df_",start_index,"_to_",end_index,".rds"))  
  }
})

# Combine saved results into a single object if needed
main_df <- data.frame()
for (i in seq_len(num_batches)) {
  start_index <- (i - 1) * batch_size + 1
  end_index <- min(i*batch_size, nrow(final_calendar_file))
  result_subset <- readRDS(paste0("C:/Users/silveus/Documents/PA DOC/intermediate data/main_df_",start_index,"_to_",end_index,".rds"))
  main_df <- rbind(main_df, result_subset)
}



#main_df <- pop_m_yr_df(final_calendar_file)
#system.time({
#  main_df <- pop_m_yr_df(final_calendar_file)
#})
saveRDS(main_df,"C:/Users/silveus/Documents/PA DOC/intermediate data/main_df.rds")
print("Function 2 Complete")



if (exists("main_df")==0) {
  print("Loading main_df")
  main_df <- readRDS(paste0(encripted_drive_path,"intermediate data/main_df.rds"))  
}else{
  print("main_df already exists")
}
# Make complete dataset by getting age, LSIR, RACE, SEX, facilty TYPE, and Region


#Parallelized version. Finicky. requires doParallel and foreach packages.
# registerDoParallel(cl)
# main_test_df <- main_df %>% head(100)
# system.time({
# main_df_complete <- foreach(i = 1:nrow(main_test_df), .combine = 'rbind', .packages = c('dplyr')) %dopar% {
#   row <- main_test_df[i, ]
#   
#   row$age <- get_age(row$ID, row$m_yr)
#   row$lsir <- get_lsir(row$ID, row$m_yr)
#   row$race <- get_race(row$ID)
#   row$sex <- get_sex(row$ID)
#   row$facility_type <- get_facility_type(row$loc)
#   row$facility_region <- get_facility_region(row$loc)
#   
#   return(row)
# }
# })


#create complete main df in batches
system.time({
  batch_size <- 1000
  num_batches <- ceiling(nrow(main_df)/batch_size)
  
  for (i in seq_len(num_batches)){
    start_index <- (i-1)*batch_size+1
    end_index <- min(i*batch_size, nrow(main_df))
    subset_ids <- main_df[start_index:end_index,]
    temp_filepath <- paste0(encripted_drive_path, "intermediate data/main_complete_df_",start_index,"_to_",end_index,".rds")
    
    #check to see if intermediate file already exists
    if (file.exists(temp_filepath)) {
      print(paste0("main_complete_df_",start_index,"_to_",end_index,".rds exists. Skip"))
    }
    else{
      print(paste0("Working on ",start_index," to ",end_index))
      result_subset <- main_df  %>%
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
      saveRDS(result_subset, file = temp_filepath)
      print(paste0("main_complete_df_",start_index,"_to_",end_index,".rds completed"))
    }
  }
})

# Combine saved results into a single object if needed
main_df <- data.frame()
for (i in seq_len(num_batches)) {
  start_index <- (i - 1) * batch_size + 1
  end_index <- min(i*batch_size, nrow(main_df))
  result_subset <- readRDS(paste0(encripted_drive_path,"intermediate data/main_complete_df_",start_index,"_to_",end_index,".rds"))
  main_df_complete <- rbind(main_df, result_subset)
}


main_test_df <- main_df[1:1000,]
system.time({
main_df_complete <- main_test_df  %>%
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

})

#main_df_complete <- parLapply(cl,main_test_df,get_demos_fn)
saveRDS(main_df_complete,"C:/Users/silveus/Documents/PA DOC/intermediate data/main_complete_df.rds")
print("Function 3 Complete")

get_comparisons <- function(dataset){
  return(dataset %>% 
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
             across(starts_with("facility_region_"), first))
  )
}

system.time({
  main_comparison_df <- parLapply(cl,main_df_complete,get_comparisons)
})
saveRDS(main_comparison_df,"C:/Users/silveus/Documents/PA DOC/intermediate data/main_comparison_df.rds")
print("Function 4 Complete")
# main_comparison_df <- main_df_complete %>% 
#   group_by(loc, m_yr) %>% 
#   summarize(
#     avg_lsir = mean(lsir, na.rm = TRUE),
#     avg_age = mean(age, na.rm = TRUE),
#     perc_black = round(mean(race == 'BLACK') * 100, 3), 
#     perc_male = round(mean(sex == 'MALE') * 100, 3),
#     pop_count = n(),
#     facility_type = first(facility_type), 
#     facility_region = first(facility_region),  
#     across(starts_with("facility_type_"), first),  
#     across(starts_with("facility_region_"), first)
#   )
# print("Function 4 Complete")


main_comparison_summary <- main_comparison_df %>% 
  select(!c(loc, m_yr)) %>%
  tbl_summary(by = facility_type)

main_comparison_summary
print("Function 5 Complete")

# Creating facility level dummy variables ---------------------------------
# 
# main_df_with_dummy <- dummy_cols(
#   main_df_complete,
#   select_columns = c("facility_type", "facility_region"),
#   remove_first_dummy = TRUE
#   # remove_selected_columns = TRUE (Removes the original columns after creating the dummy variables)
# )
# 
# 
# main_facility_analysis <- main_df_with_dummy %>% 
#   group_by(loc, m_yr) %>% 
#   summarize(
#     avg_lsir = mean(lsir, na.rm = TRUE),
#     avg_age = mean(age, na.rm = TRUE),
#     perc_black = round(mean(race == 'BLACK') * 100, 3), 
#     perc_male = round(mean(sex == 'MALE') * 100, 3),
#     pop_count = n(),
#     facility_type = first(facility_type), 
#     facility_region = first(facility_region),  
#     across(starts_with("facility_type_"), first),  
#     across(starts_with("facility_region_"), first)
#   )


# num_workers <- getDoParWorkers()
# 
# if (num_workers > 1) {
#   print(paste("Active cluster with", num_workers, "workers."))
# } else {
#   print("No active cluster.")
# }
# 
# if (exists("cl") && !is.null(cl) && inherits(cl, "cluster")) {
#   print("Active cluster detected.")
# } else {
#   print("No active cluster detected.")
# }
# 
# if (exists("cl") && inherits(cl, "cluster")) {
#   str(cl)
# }
# 
