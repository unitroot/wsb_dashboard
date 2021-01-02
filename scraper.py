# WSB SCRAPER

## 1) Load packages
import datetime
import pyarrow
import praw
import pandas as pd
import nltk
# nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer


## 2) Set up Reddit API
# DISCLAIMER: this is not my API key, I have it from some tutorial, but can't find the link anymore. Sorry!
reddit = praw.Reddit(client_id='dwvhQN_PoUCoAw',
                     client_secret='X8N_SZUsiI-CNVIYLToBFFQ-cYE',
                     user_agent='news on hooks')

## 3) Retrieve data
### 3.1) Retrieve submissions
submissions = reddit.subreddit('wallstreetbets').new(limit = 20)
### 3.2) Parse submissions
sub_data = pd.DataFrame(columns = ['id', 'title', 'author', 'score', 'flair', 'ncomms', 'created', 'body', 'vader'])
for sub in submissions:
    sub_data = sub_data.append(
    pd.Series(
    [
        sub.id, 
        sub.title, 
        sub.author,
        sub.score,
        sub.link_flair_text, 
        sub.num_comments,
        sub.created_utc,
        sub.selftext,
        'N/A'
    ], 
    index = ['id', 'title', 'author', 'score', 'flair', 'ncomms', 'created', 'body', 'vader']
    ),
        ignore_index = True
    )
### 3.3) Retrieve comment forrest
com_data =  pd.DataFrame(columns = ['id', 'subid', 'parentid', 'author', 'score', 'created', 'body', 'vader'])
for s_id in sub_data.id:
    sub = reddit.submission(id=s_id)
    # print(sub.title)
    sub.comments.replace_more(limit=None)
    comment_queue = sub.comments[:]  # Seed with top-level
    while comment_queue:
        comment = comment_queue.pop(0)
        com_data = com_data.append(
        pd.Series(
        [
            comment.id, 
            comment.link_id,
            comment.parent_id,
            comment.author,
            comment.score,
            comment.created_utc,
            comment.body,
            'N/A'
        ],
            index = ['id', 'subid', 'parentid', 'author', 'score', 'created', 'body', 'vader']
        ),
            ignore_index = True
        )
        comment_queue.extend(comment.replies)       
com_data

## 4) VADER Sentiment Analyzer
### 4.1) Assign SIA shortcut
sia = SentimentIntensityAnalyzer()
### 4.2) Define custom valence dictionary
wsb_lingo = {
    # words to neutralize
    'retard': 0,
    'yolo': 0,
    'yolod': 0,
    'yoloed': 0,
    'yoloing': 0,
    'fuck': 0,
    'fucks': 0,
    'fucked': 0,
    'fucking': 0,
    'shit': 0,
    'fag': 0, 
    'autist': 0,
    # positive valence
    'bull': 1.5,
    'call': 1.5,
    'calls': 1.5,
    'long': 1.5,
    'buy': 1,
    'buys': 1,
    'buying': 1,
    'hold': 0.5,
    # negative valence
    'bear': -1.5,
    'sell': -1.5,
    'selling': -1.5,
    'sells': -1.5,
    'puts': -1.5,
    'short': -1.5,
    'shorts': -1.5,
    'shorting': -1.5,
    'put': -1,
    'wife': -1,
    "wife's": -1,
    'boyfriend': -1,
    'gay': -0.5, # sad
    'moon': -0.5, # debatable, but mostly used sarcastically
    # emoji mapping, which get translated to text by VADER
    'fire': 0,
    'rainbow': -1.5,
    'gem stone': 1,
    'raising hand': 1,
    'rocket': 1.5
}
sia.lexicon.update(wsb_lingo)
### 4.3) Iterate over submissions and comments
for row in sub_data.index:
    sub_data.loc[row, 'vader'] = sia.polarity_scores(sub_data.loc[row, 'title'])['compound']
for row in com_data.index:
    com_data.loc[row, 'vader'] = sia.polarity_scores(com_data.loc[row, 'body'])['compound']

## 5) Store Data
### 5.1) Data type fixing
sub_data = sub_data.astype({'id': 'str', # required str conversion due to hidden PRAW-specific dtypes incompatibility with feather
                            'title': 'str',
                            'author': 'str',
                            'score': 'int32', 
                            'flair': 'str',
                            'ncomms': 'int32', 
                            'created': 'datetime64[s]',
                            'body': 'str',
                            'vader': 'float32'})
com_data = com_data.astype({'id': 'str',
                            'subid': 'str',
                            'parentid': 'str',
                            'author': 'str',
                            'score': 'int32', 
                            'created': 'datetime64[s]',
                            'body': 'str',
                            'vader': 'float32'})
### 5.2) Save history
sub_data.to_feather(f'www/history/sub_data_{datetime.datetime.now():%Y%m%d_%H%M}.ft')
com_data.to_feather(f'www/history/com_data_{datetime.datetime.now():%Y%m%d_%H%M}.ft')
### 5.3) Splice history
sub_wc = pd.read_feather('www/sub_data.ft')
com_wc = pd.read_feather('www/com_data.ft')
sub_data = pd.merge(sub_data, sub_wc, how = 'outer')
com_data = pd.merge(com_data, com_wc, how = 'outer')
### 5.4) Save working copy
sub_data.to_feather('www/sub_data.ft')
com_data.to_feather('www/com_data.ft')

# See you in R! 