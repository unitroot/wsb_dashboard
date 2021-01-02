# Wallstreetbets Sentiment Dashboard

R Shiny App based on a Python Scraper and Vader Sentiment Analyzer

## Python Components

scraper.py --- Python script executing the reddit API call, used in reticulate call from scheduler
scraper_notebook.py --- Jupyter notebook version of scraper.py

### Python Requirements
Python 3.6+
pkgs: datetime, praw, pandas, pyarrow, nltk
additional: VADER lexicon (>nltk.download('vader_lexicon')) 


## R Components

app.R --- main shiny app file
utlity.R --- utility functions for scheduler

### R Requirements
R 3.6+
pkgs: xts, zoo, reticulate, tidyverse, quantmod, highcharter, DT