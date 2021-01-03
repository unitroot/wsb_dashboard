# update reddit data using python scraper

updateReddit <- function(nSub = 20) {
  
  require(TTR)
  require(dplyr)
  require(arrow)
  require(reticulate)
  require(tibble)
  
  # get ticker most recent ticker list
  dfTickers <- TTR::stockSymbols(exchange = c("NASDAQ", "NYSE", "AMEX")) %>% 
    dplyr::select(Symbol, Name) %>% 
    dplyr::rename(ticker = Symbol, name = Name) %>% 
    dplyr::mutate(handle = paste0("$", ticker)) %>% 
    dplyr::arrange(ticker)

  arrow::write_feather(dfTickers, "www/tickers.feather")
  
  # Python Call
  ## fail safe
  reticulate::use_condaenv("r-reticulate")
  if (!reticulate::py_available()) {reticulate::py_config()}
  if (!reticulate::py_available()) {return()}
  
  ##
  #reticulate::py_run_file("scraper.py")
  
  reticulate::py_run_string(paste0("n_sub = ", nSub))
  reticulate::source_python("scraper.py")
  #reticulate::py_run_string("scrape_wsb(n_sub)")
  
  dfSubs <- tibble::tibble(arrow::read_feather("www/sub_data.feather"))
  dfComs <- tibble::tibble(arrow::read_feather("www/com_data.feather"))
  return()
}