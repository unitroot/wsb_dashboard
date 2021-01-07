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
    dplyr::arrange(ticker)
  dfTickers$name <- gsub("\\,? Inc.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\,? LP.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\,? Ltd.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\,? ASA.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\,? plc.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\s?\\-? Common.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\s?\\-? Ordinary.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\s?\\-? Warrant.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\sCorp.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\s\\- American Dep.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\sHoldings.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\sGroup.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\sCompany.*", "", dfTickers$name)
  dfTickers$name <- gsub("\\sLimited.*", "", dfTickers$name)
  dfTickers$name <- gsub("^The\\s", "", dfTickers$name)
  
  arrow::write_feather(dfTickers, "www/tickers.feather")
  
  # Python Call
  ## fail safe
  if (!reticulate::py_available()) {reticulate::py_config()}
  if (!reticulate::py_available()) {return()}
  reticulate::py_discover_config()
  
  reticulate::py_run_string(paste0("n_sub = ", nSub))
  reticulate::source_python("scraper.py")
  reticulate::py_run_string("scrape_wsb(n_sub)")
  reticulate::py_run_string("scrape_subs(1000)")
 
  return()
}

parseReddit <- function() {
  dfSubs <- tibble::tibble(arrow::read_feather("www/sub_data.ft"))
  dfComs <- tibble::tibble(arrow::read_feather("www/com_data.ft"))
  
  # filter out bot-posts
  dfComs <- dfComs[!dfComs$author %in% c("WSBVoteBot","AutoModerator", "pickbot"),]
  
  # parse tickers
  dfComs <- dplyr::mutate(dfComs, lazy = stringr::str_extract_all(dfComs$body, paste0("\\b(", paste(dfTickers$ticker, collapse = "|"), ")\\b")) %>% 
                            purrr::map_chr(toString))
  dfComs <- dplyr::mutate(dfComs, strict = stringr::str_extract_all(dfComs$body, paste0("\\$\\b(", paste(dfTickers$ticker, collapse = "|"), ")\\b")) %>% 
                            purrr::map_chr(toString))
  dfComs <- dfComs[rowSums(dfComs[,c("strict", "lazy")] == "") < 2, ] %>% 
    dplyr::mutate(strict = gsub("\\$", "", strict)) %>% 
    dplyr::mutate(strict = gsub("\\s", "", strict)) %>% 
    dplyr::mutate(lazy = gsub("\\s", "", lazy)) 
  
  dfSubs <- dplyr::mutate(dfSubs, lazy = stringr::str_extract_all(dfSubs$title, paste0("\\b(", paste(dfTickers$ticker, collapse = "|"), ")\\b")) %>% 
                            purrr::map_chr(toString))
  dfSubs <- dplyr::mutate(dfSubs, strict = stringr::str_extract_all(dfSubs$title, paste0("\\$\\b(", paste(dfTickers$ticker, collapse = "|"), ")\\b")) %>% 
                            purrr::map_chr(toString))
  dfSubs <- dfSubs[rowSums(dfSubs[,c("strict", "lazy")] == "") < 2, ] %>% 
    dplyr::mutate(strict = gsub("\\$", "", strict)) %>% 
    dplyr::mutate(strict = gsub("\\s", "", strict)) %>% 
    dplyr::mutate(lazy = gsub("\\s", "", lazy)) 
  
  
  return()
}
