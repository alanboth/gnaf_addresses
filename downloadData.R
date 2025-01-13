#!/usr/bin/env Rscript

# load libraries and functions --------------------------------------------
suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(dplyr))

options(dplyr.summarise.inform = FALSE) # make dplyr stop blabbing about summarise


# extract the database name that was passed in from the command line
option_list = list(
  make_option(c("-y", "--year"), type="numeric", default=NULL, 
              help="year to process", metavar="character")
); 

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

yearToProcess<-opt$year
# yearToProcess=2021

cat(paste0("Running downloadData.R for ",yearToProcess,"\n"))


files_df <- read.csv("data_locations.csv")

if (!is.null(yearToProcess)) {
  files_df %>%
    filter(year==yearToProcess)
}





# Download files ----------------------------------------------------------

for (i in 1:nrow(files_df)) {
  file_name_current <- paste0("data/",files_df[i,]$file_name)
  url_current <- files_df[i,]$url
  # if the file doesn't exist download it
  if (!file.exists(file_name_current)) {
    download.file(url_current, file_name_current, method="wget")
  }
}


