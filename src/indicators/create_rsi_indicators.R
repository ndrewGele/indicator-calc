create_rsi_indicators <- function(db.con, symbol, n.args) {
  
  require(dplyr)
  require(dbplyr)
  
  df <- db.con %>% 
    tbl('daily_ohlc') %>% 
    filter(symbol == !!symbol) %>% 
    select(symbol, timestamp, close) %>% 
    arrange(timestamp) %>% 
    collect()
  
  indicators_df <- purrr::map_dfc(
    .x = n.args,
    .f = function(x) {
      if(nrow(df) > x) {
        df %>% 
          pull(close) %>% 
          TTR::RSI(n = x) %>% 
          tibble(rsi = .) %>% 
          rename_with(.fn = paste0, .cols = everything(), '_', x) %>% 
          return()
      } else {
        data.frame(bad_rsi_ = rep(NA,nrow(df))) %>% 
          rename_with(.fn = paste0, .cols = everything(), '_', x) %>% 
          return()
      }
    }
  )
  
  res <- df %>% 
    select(symbol, timestamp) %>% 
    bind_cols(indicators_df) %>% 
    tidyr::pivot_longer(
      cols = contains('rsi_'),
      names_to = 'indicator'
    ) %>% 
    filter(!is.na(value))
  
  return(res)
  
}
