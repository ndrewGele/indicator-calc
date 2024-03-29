create_adx_indicators <- function(db.con, symbol, n.args) {
  
  require(dplyr)
  require(dbplyr)
  
  df <- db.con %>% 
    tbl('daily_ohlc') %>% 
    filter(symbol == !!symbol) %>% 
    select(symbol, timestamp, high, low, close) %>% 
    arrange(timestamp) %>% 
    collect()
  
  indicators_df <- purrr::map_dfc(
    .x = n.args,
    .f = function(x) {
      if(nrow(df) > x*2) {
        df %>% 
          select(high, low, close) %>% 
          TTR::ADX(n = x) %>% 
          as_tibble() %>% 
          select(dx = DX, adx = ADX) %>%
          rename_with(.fn = paste0, .cols = everything(), '_', x) %>% 
          return()
      } else {
        data.frame(bad_adx_ = rep(NA,nrow(df))) %>% 
          rename_with(.fn = paste0, .cols = everything(), '_', x) %>% 
          return()
      }
    }
  )
  
  res <- df %>% 
    select(symbol, timestamp) %>% 
    bind_cols(indicators_df) %>% 
    tidyr::pivot_longer(
      cols = contains('dx_'),
      names_to = 'indicator'
    ) %>% 
    filter(!is.na(value))
  
  return(res)
  
}
