# This script will pick a symbol and create market indicators for it

library(dplyr)
library(dbplyr)


# Source custom functions
source('./src/indicators/create_adx_indicators.R')
source('./src/indicators/create_bbands_indicators.R')
source('./src/indicators/create_rsi_indicators.R')


# Mode can be used to alter sleep duration or other effects
mode <- 'normal'

# Connect to DB
db_con <- tryCatch(
  expr = {
    DBI::dbConnect(
      drv = RPostgres::Postgres(),
      dbname = Sys.getenv('POSTGRES_DB'),
      host = Sys.getenv('POSTGRES_HOST'),
      port = Sys.getenv('POSTGRES_PORT'),
      user = Sys.getenv('POSTGRES_USER'),
      password = Sys.getenv('POSTGRES_PASSWORD')
    )
  },
  error = function(e) e
)

if(inherits(db_con, 'error')){
  message('Error connecting to database:', db_con,
          'Sleeping before trying again.')
  Sys.sleep(60 * 5)
  stop('Done sleeping. Ending process.')
}


# Ensure prerequisite tables exist in database
if(
  !DBI::dbExistsTable(db_con, 'symbols') |
  !DBI::dbExistsTable(db_con, 'daily_ohlc')
) {
  
  # Don't leave connection open while not using it
  DBI::dbDisconnect(db_con)
  message('Missing one or more required tables. Sleeping for an hour.')
  Sys.sleep(60 * 60)
  stop('Done sleeping. Ending process.')
  
}

# Get list of current relevant symbols that have ohlc data
symbol_list <- db_con %>% 
  tbl('symbols') %>% 
  inner_join(
    db_con %>% 
      tbl('symbols') %>% 
      group_by(fetcher_name) %>% 
      summarise(update_timestamp = max(update_timestamp, na.rm = TRUE)),
    by = c('fetcher_name', 'update_timestamp')
  ) %>% 
  inner_join(
    db_con %>% 
      tbl('daily_ohlc') %>% 
      select(symbol) %>% 
      distinct(),
    by = 'symbol'
  ) %>% 
  select(symbol) %>% 
  distinct() %>%
  collect() %>% 
  pull(symbol)

message('Symbol data retrieved.')

# Determine which symbol to update
if(!DBI::dbExistsTable(db_con, 'indicators')) {
  message('First process run. Picking random symbol.')
  picked <- sample(x = symbol_list, size = 1)
} else {
  
  # If table does exist already, pick symbol based on existing data
  indicator_stats <- db_con %>% 
    tbl('indicators') %>% 
    filter(symbol %in% !!symbol_list) %>%
    group_by(symbol) %>%
    summarise(
      max_update = max(update_timestamp, na.rm = TRUE)
    ) %>%
    collect()
  
  if(any(!symbol_list %in% indicator_stats$symbol)) {
    
    # If indicator data exists, but not all symbols are present, pick random missing symbol
    picked <- sample(
      x = symbol_list[!symbol_list %in% indicator_stats$symbol],
      size = 1
    )
    message('Not all symbols present in table. Picked ', 
            picked, ' randomly from missing symbols.')
    
  } else {
    
    # If all symbols are present, pick one with oldest update timestamp
    picked <- indicator_stats %>% 
      slice_min(
        order_by = max_update,
        n = 1
      ) %>% 
      pull(symbol) %>% 
      sample(size = 1)
    
    mode <- 'expand'
    
    message('All symbols present in table. Picked ', 
            picked, ' from least recently oldest symbols. ',
            'Will try to expand existing data.')
    
  }
  
} # end of symbol picker


# Create indicator data for selected symbol
message('Creating indicators for symbol: ', picked, '.')
df <- bind_rows(
  create_adx_indicators(
    db.con = db_con, 
    symbol = picked,
    n.args = c(5,10,25,50,100)
  ),
  create_bbands_indicators(
    db.con = db_con, 
    symbol = picked,
    n.args = c(5,10,25,50,100,200,300)
  ),
  create_rsi_indicators(
    db.con = db_con, 
    symbol = picked,
    n.args = c(5,10,25,50,100,200,300)
  )
)
update_timestamp <- lubridate::floor_date(Sys.time(), unit = 'seconds')
df <- mutate(df, update_timestamp = update_timestamp)
message('Finished creating indicators for symbol: ', picked, '.')

# Create indicators table if needed
if(!DBI::dbExistsTable(db_con, 'indicators')) {
  
  df %>% 
    DBI::dbCreateTable(
      conn = db_con,
      name = 'indicators',
      fields = .
    )
  
  new_df <- df
  
} else {
  
  # Check for duplicate data before writing to indicators table
  existing_df <- db_con %>% 
    tbl('indicators') %>% 
    filter(symbol == !!picked) %>% 
    select(symbol, timestamp, indicator) %>% 
    collect()
  
  new_df <- anti_join(
    df,
    existing_df,
    by = c('symbol', 'timestamp', 'indicator')
  )
  
}

# If there is new data, append table and set sleep timer
sleep_time <- 5
if(nrow(new_df) > 0) {
  new_df %>% 
    DBI::dbAppendTable(
      conn = db_con,
      name = 'indicators',
      value = .
    )
} else if(mode == 'expand') {
  sleep_time <- 60 * 15
}

# Whether there is new data or not, update the symbol's update_timestamp column
DBI::dbExecute(
  conn = db_con,
  statement = glue::glue(
    'UPDATE indicators ',
    'SET update_timestamp = \'{update_timestamp}\'' ,
    'WHERE symbol = \'{picked}\''
  )
)

# Log before sleeping and looping again
message('Wrote ', nrow(new_df), ' new records to indicators table. ',
        'Sleeping for ', sleep_time, ' seconds.')

DBI::dbDisconnect(db_con)
Sys.sleep(sleep_time)
