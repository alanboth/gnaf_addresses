#!/usr/bin/env Rscript

# load libraries and functions --------------------------------------------
suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(sf))

options(dplyr.summarise.inform = FALSE) # make dplyr stop blabbing about summarise

# ensures the geometry column is called 'geom'
renameGeometryColumn <- function(df) {
  if (
    intersect(colnames(df),
              c("GEOM", "GEOMETRY", "geometry")) %>% length() > 0
  ) {
    st_geometry(df) <- "geom"
  }
  return(df)
}

# c      = character
# i      = integer
# n      = number
# d      = double
# l      = logical
# f      = factor
# D      = date
# T      = date time
# t      = time
# ?      = guess
# _ or - = skip

# ADDRESS_SITE_GEOCODE tables
address_geocode_df <- tribble(
  ~column_name              , ~data_type,
  "ADDRESS_SITE_GEOCODE_PID", "c",
  "DATE_CREATED"            , "D",
  "DATE_RETIRED"            , "D",
  "ADDRESS_SITE_PID"        , "c",
  "GEOCODE_SITE_NAME"       , "c",
  "GEOCODE_SITE_DESCRIPTION", "c",
  "GEOCODE_TYPE_CODE"       , "c",
  "RELIABILITY_CODE"        , "n",
  "BOUNDARY_EXTENT"         , "n",
  "PLANIMETRIC_ACCURACY"    , "n",
  "ELEVATION"               , "n",
  "LONGITUDE"               , "n",
  "LATITUDE"                , "n"
)

address_geocode_datatypes <- address_geocode_df %>%
  pull(data_type) %>%
  paste(collapse = '')

# ADDRESS_DETAIL tables
address_detail_df <- tribble(
  ~column_name           , ~data_type,
  "ADDRESS_DETAIL_PID"   , "c"       ,
  "DATE_CREATED"         , "D"       ,
  "DATE_LAST_MODIFIED"   , "D"       ,
  "DATE_RETIRED"         , "D"       ,
  "BUILDING_NAME"        , "c"       ,
  "LOT_NUMBER_PREFIX"    , "c"       ,
  "LOT_NUMBER"           , "c"       ,
  "LOT_NUMBER_SUFFIX"    , "c"       ,
  "FLAT_TYPE_CODE"       , "c"       ,
  "FLAT_NUMBER_PREFIX"   , "c"       ,
  "FLAT_NUMBER"          , "n"       ,
  "FLAT_NUMBER_SUFFIX"   , "c"       ,
  "LEVEL_TYPE_CODE"      , "c"       ,
  "LEVEL_NUMBER_PREFIX"  , "c"       ,
  "LEVEL_NUMBER"         , "n"       ,
  "LEVEL_NUMBER_SUFFIX"  , "c"       ,
  "NUMBER_FIRST_PREFIX"  , "c"       ,
  "NUMBER_FIRST"         , "n"       ,
  "NUMBER_FIRST_SUFFIX"  , "c"       ,
  "NUMBER_LAST_PREFIX"   , "c"       ,
  "NUMBER_LAST"          , "n"       ,
  "NUMBER_LAST_SUFFIX"   , "c"       ,
  "STREET_LOCALITY_PID"  , "c"       ,
  "LOCATION_DESCRIPTION" , "c"       ,
  "LOCALITY_PID"         , "c"       ,
  "ALIAS_PRINCIPAL"      , "c"       ,
  "POSTCODE"             , "c"       ,
  "PRIVATE_STREET"       , "c"       ,
  "LEGAL_PARCEL_ID"      , "c"       ,
  "CONFIDENCE"           , "n"       ,
  "ADDRESS_SITE_PID"     , "c"       ,
  "LEVEL_GEOCODED_CODE"  , "n"       ,
  "PROPERTY_PID"         , "c"       ,
  "GNAF_PROPERTY_PID"    , "c"       ,
  "PRIMARY_SECONDARY"    , "c"
)
address_detail_datatypes <- address_detail_df %>%
  pull(data_type) %>%
  paste(collapse = '')

# extract the database name that was passed in from the command line
option_list = list(
  make_option(c("-y", "--year"), type="numeric", default=NULL, 
              help="year to process", metavar="character")
); 

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

yearToProcess<-opt$year
# yearToProcess=2021

cat(paste0("Running convertData.R for ",yearToProcess,"\n"))


files_df <- read.csv("data_locations.csv") %>%
  mutate(input_file=paste0("data/",file_name)) %>%
  mutate(output_file=paste0("output/", gsub(".zip",".sqlite",file_name)))
  
# removing the files that have already been generated and files that haven't
# been downloaded
files_df_filtered <- files_df %>%
  filter(input_file%in%list.files(path="data",full.names=T)) %>%
  filter(!output_file%in%list.files(path="output",full.names=T))


if (!is.null(yearToProcess)) {
  files_df_filtered %>%
    filter(year==yearToProcess)
}




for (i in 1:nrow(files_df_filtered)) {
  # i=1
  file_name_current <- files_df_filtered$input_file[i]
  file_name_final   <- files_df_filtered$output_file[i]
  crs_current       <- files_df_filtered$epsg[i]
  
  
  geocode_psv_files <- unzip(file_name_current,list=T) %>%
    filter(grepl("*ADDRESS_SITE_GEOCODE_psv*.psv",Name)) %>%
    pull(Name)
  
  geocode_combined <- NULL
  
  # loop through all the geocode files and add them to geocode_combined
  for (i in 1:length(geocode_psv_files)) {
    geocode_current <- read_delim(unz(file_name_current, geocode_psv_files[i]),
                                  delim="|",
                                  col_names=TRUE,
                                  col_types=address_geocode_datatypes)
    geocode_combined <- bind_rows(
      geocode_combined,
      geocode_current
    )
  }
  rm(geocode_current)
  
  address_psv_files <- unzip(file_name_current,list=T) %>%
    filter(grepl("*ADDRESS_DETAIL_psv*.psv",Name)) %>%
    pull(Name)
  
  address_combined <- NULL
  
  # loop through all the address details files and add them to address_combined
  for (i in 1:length(address_psv_files)) {
    address_current <- read_delim(unz(file_name_current, address_psv_files[i]),
                                  delim="|",
                                  col_names=TRUE,
                                  col_types=address_detail_datatypes)
    address_combined <- bind_rows(
      address_combined,
      address_current
    )
  }
  rm(address_current)
  
  addresses_final <- inner_join(
    address_combined,
    geocode_combined%>%dplyr::select(-DATE_CREATED,-DATE_RETIRED),
    by="ADDRESS_SITE_PID") %>%
    st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = crs_current) %>%
    renameGeometryColumn()
  rm(geocode_combined,address_combined)
  
  colnames(addresses_final) <- tolower(colnames(addresses_final))
  
  st_write(addresses_final, file_name_final, append=F)
  
  
}


