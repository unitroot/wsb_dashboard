# update reddit data using python scraper

updateReddit <- function(nSub = 20) {
  
  require(TTR)
  require(dplyr)
  require(arrow)
  require(reticulate)
  require(tibble)
  require(tm)
  
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

getSubs <- function(nSub = 20) {
  # Python Call
  ## fail safe
  if (!reticulate::py_available()) {reticulate::py_config()}
  if (!reticulate::py_available()) {return()}
  reticulate::py_discover_config()
  
  reticulate::py_run_string(paste0("n_sub = ", nSub))
  reticulate::source_python("scraper.py")
  reticulate::py_run_string("scrape_subs(n_sub)")
}

getComs <- function(sDate = Sys.Date()) {
  if (!inherits(sDate, "Date")) {stop("sDate must be a Date object.")}
  # Python Call
  ## fail safe
  if (!reticulate::py_available()) {reticulate::py_config()}
  if (!reticulate::py_available()) {return()}
  reticulate::py_discover_config()
  
  reticulate::py_run_string(paste0("start_date = ", as.character(sDate, "%Y-%m-%d")))
  reticulate::source_python("scraper.py")
  reticulate::py_run_string("scrape_coms(start_date)")
}

parseReddit <- function() {
  dfTickers <- tibble::tibble(arrow::read_feather("www/tickers.feather"))
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
    dplyr::mutate(lazy = gsub("\\s", "", lazy)) %>% 
    dplyr::mutate(created = as.Date(lubridate::with_tz(as.POSIXct(created, tz = "UTC"), "EST")))
  
  dfSubs <- dplyr::mutate(dfSubs, lazy = stringr::str_extract_all(dfSubs$title, paste0("\\b(", paste(dfTickers$ticker, collapse = "|"), ")\\b")) %>% 
                            purrr::map_chr(toString))
  dfSubs <- dplyr::mutate(dfSubs, strict = stringr::str_extract_all(dfSubs$title, paste0("\\$\\b(", paste(dfTickers$ticker, collapse = "|"), ")\\b")) %>% 
                            purrr::map_chr(toString))
  dfSubs <- dfSubs[rowSums(dfSubs[,c("strict", "lazy")] == "") < 2, ] %>% 
    dplyr::mutate(strict = gsub("\\$", "", strict)) %>% 
    dplyr::mutate(strict = gsub("\\s", "", strict)) %>% 
    dplyr::mutate(lazy = gsub("\\s", "", lazy)) %>% 
    dplyr::mutate(created = as.Date(lubridate::with_tz(as.POSIXct(created, tz = "UTC"), "EST")))
  
  # build time series
  xtsComScore <- dfComs[,c("created", "score", "lazy")] %>% 
    tidyr::separate_rows(lazy, sep = ",") %>% 
    dplyr::group_by(created, lazy) %>% 
    dplyr::summarise(score = sum(score, na.rm = TRUE)) %>% 
    tidyr::spread(lazy, score) %>% 
    as.data.frame()
  xtsComScore <- xts::xts(xtsComScore[,-1], order.by = xtsComScore[,1])
  
  xtsSubScore <- dfSubs[,c("created", "ncomms", "lazy")] %>% 
    tidyr::separate_rows(lazy, sep = ",") %>% 
    dplyr::group_by(created, lazy) %>% 
    dplyr::summarise(ncomms = sum(ncomms, na.rm = TRUE)) %>% 
    tidyr::spread(lazy, ncomms) %>% 
    as.data.frame()
  xtsSubScore <- xts::xts(xtsSubScore[,-1], order.by = xtsSubScore[,1])
  
  xtsComSent <- dfComs[,c("created", "vader", "lazy")] %>% 
    tidyr::separate_rows(lazy, sep = ",") %>% 
    dplyr::group_by(created, lazy) %>% 
    dplyr::summarise(score = mean(vader, na.rm = TRUE)) %>% 
    tidyr::spread(lazy, score) %>% 
    as.data.frame()
  xtsComSent <- xts::xts(xtsComSent[,-1], order.by = xtsComSent[,1])
  
  xtsSubSent <- dfSubs[,c("created", "vader", "lazy")] %>% 
    tidyr::separate_rows(lazy, sep = ",") %>% 
    dplyr::group_by(created, lazy) %>% 
    dplyr::summarise(score = mean(vader, na.rm = TRUE)) %>% 
    tidyr::spread(lazy, score) %>% 
    as.data.frame()
  xtsSubSent <- xts::xts(xtsSubSent[,-1], order.by = xtsSubSent[,1])
  
  # build word cloud matrices
  sUnique <- dfSubs[,c("lazy")] %>% 
    tidyr::separate_rows(lazy, sep = ",")  
  sUnique <- stringr::str_sort(unique(as.character(sUnique$lazy)))
  
  lWords <- list()
  
  dfCorpus <- dfSubs[,c("lazy", "title")] %>% 
    dplyr::rename(body = title) %>% 
    rbind(dfComs[,c("lazy", "body")]) %>% 
    dplyr::mutate(body = iconv(body, "utf-8", "ascii", sub = ""))
  
  ## iterate through unique tickers
  for (sTick in sUnique) {
    dfTemp <- dfCorpus[grepl(sTick, dfCorpus$lazy),"body"] %>% 
      unlist(use.names = FALSE) %>% 
      tm::VectorSource() %>% 
      tm::Corpus() %>% 
      tm::tm_map(tolower) %>% 
      tm::tm_map(removePunctuation) %>% 
      tm::tm_map(removeNumbers) %>% 
      tm::tm_map(removeWords, tm::stopwords("en")) %>%  
      tm::TermDocumentMatrix() %>% 
      as.matrix() %>% 
      rowSums() %>% 
      sort(decreasing = TRUE) 
    
    dfTemp <- dfTemp[!names(dfTemp) %in% tolower(sTick)]
      
    dfTemp <- data.frame(word = names(dfTemp), freq = as.numeric(dfTemp))[1:100,]
    eval(parse(text = paste0("lWords$", sTick, " <- dfTemp")))
  }
  
  lData <- list(xtsComScore, xtsSubScore, xtsComSent, xtsSubSent, lWords)
  save(lData, file = "www/lData.Rda")
}
