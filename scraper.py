# WSB SCRAPER
## Load packages
import datetime
import pyarrow
import numpy
import praw
import pandas as pd
import nltk
# nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer 
sia = SentimentIntensityAnalyzer()
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
        'autists': 0,
        # positive valence
        'bull': 1.5,
        'tendie': 1.5,
        'tendies': 1.5,
        'call': 1.5,
        'calls': 1.5,
        'long': 1.5,
        'buy': 1,
        'buys': 1,
        'buying': 1,
        'moon': 1.5, 
        'mooning': 1.5,
        'gainz': 1,
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
        'bagholder': -1.5,
        'put': -1,
        'wife': -1,
        "wife's": -1,
        'boyfriend': -1,
        'gay': -0.5, # sad
        # emoji mapping, which get translated to text by VADER
        'fire': 1,
        'rainbow bear': -1.5,
        'gem stone raising hand': 1,
        'rocket': 1.5
    }
sia.lexicon.update(wsb_lingo)

# Set up Reddit API
# DISCLAIMER: this is not my API key, I have it from some tutorial, but can't find the link anymore. Sorry!
reddit = praw.Reddit(client_id='dwvhQN_PoUCoAw',
                    client_secret='X8N_SZUsiI-CNVIYLToBFFQ-cYE',
                    user_agent='news on hooks')

# SCRAPE SUBS AND COMMENTS
def scrape_wsb(n_sub = 20):

    ## Retrieve data
    ### Retrieve submissions
    submissions = reddit.subreddit('wallstreetbets').new(limit = n_sub)
    ### Parse submissions
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
    ### Retrieve comment forrest
    com_data =  pd.DataFrame(columns = ['id', 'subid', 'parentid', 'author', 'score', 'created', 'body', 'vader'])
    for s_id in sub_data.id:
        sub = reddit.submission(id=s_id)
        sub.comments.replace_more(limit=None)
        comment_queue = sub.comments[:]
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

    ## VADER Sentiment Analyzer
    for row in sub_data.index:
        sub_data.loc[row, 'vader'] = sia.polarity_scores(sub_data.loc[row, 'title'])['compound']
    for row in com_data.index:
        com_data.loc[row, 'vader'] = sia.polarity_scores(com_data.loc[row, 'body'])['compound']

    ## Store Data
    ### Data type fixing
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
    ### Save history
    sub_data.to_feather(f'www/history/sub_data_{datetime.datetime.now():%Y%m%d_%H%M}.ft')
    com_data.to_feather(f'www/history/com_data_{datetime.datetime.now():%Y%m%d_%H%M}.ft')
    ### Splice history
    sub_wc = pd.read_feather('www/sub_data.ft')
    com_wc = pd.read_feather('www/com_data.ft')
    sub_key = pd.merge(sub_data, sub_wc, how = 'outer')
    com_key = pd.merge(com_data, com_wc, how = 'outer')
    sub_key = pd.merge(sub_data['id'], sub_wc['id'], how = 'outer').drop_duplicates()
    sub_key = pd.merge(sub_key, sub_data, how = 'outer', on = 'id').dropna(how = 'any')
    sub_data = pd.merge(sub_key, pd.merge(sub_key[pd.isnull(sub_key['title'])]['id'], sub_wc, how = 'outer', on = 'id'), how = 'outer')
    com_key = pd.merge(com_data['id'], com_wc['id'], how = 'outer').drop_duplicates()
    com_key = pd.merge(com_key, com_data, how = 'outer', on = 'id').dropna(how = 'any')
    com_data = pd.merge(com_key, pd.merge(com_key[pd.isnull(com_key['subid'])]['id'], com_wc, how = 'outer', on = 'id'), how = 'outer')
    ### Save working copy
    sub_data.to_feather('www/sub_data.ft')
    com_data.to_feather('www/com_data.ft')

# SCRAPE SUBS
def scrape_subs(n_sub = 20):

    # Retrieve submissions
    submissions = reddit.subreddit('wallstreetbets').new(limit = n_sub)
    # Parse submissions
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

    # VADER Sentiment Analyzer
    # Iterate over submissions and comments
    for row in sub_data.index:
        sub_data.loc[row, 'vader'] = sia.polarity_scores(sub_data.loc[row, 'title'])['compound']

    # Store Data
    # Data type fixing
    sub_data = sub_data.astype({'id': 'str', # required str conversion due to hidden PRAW-specific dtypes incompatibility with feather
                                'title': 'str',
                                'author': 'str',
                                'score': 'int32', 
                                'flair': 'str',
                                'ncomms': 'int32', 
                                'created': 'datetime64[s]',
                                'body': 'str',
                                'vader': 'float32'})

    # Save history
    sub_data.to_feather(f'www/history/sub_data_{datetime.datetime.now():%Y%m%d_%H%M}.ft')
    # Splice history
    sub_wc = pd.read_feather('www/sub_data.ft')
    sub_key = pd.merge(sub_data, sub_wc, how = 'outer')
    sub_key = pd.merge(sub_data['id'], sub_wc['id'], how = 'outer').drop_duplicates()
    sub_key = pd.merge(sub_key, sub_data, how = 'outer', on = 'id').dropna(how = 'any')
    sub_data = pd.merge(sub_key, pd.merge(sub_key[pd.isnull(sub_key['title'])]['id'], sub_wc, how = 'outer', on = 'id'), how = 'outer')

    # Save working copy
    sub_data.to_feather('www/sub_data.ft')


# SCRAPE COMMENTS
def scrape_coms(start_date = '1900-01-01'):

    # get subs
    sub_data = pd.read_feather('www/sub_data.ft')
    sub_data = sub_data[sub_data.created > start_date]

    # Retrieve comment forrests
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

    # VADER Sentiment Analyzer
    # Iterate over submissions and comments
    for row in com_data.index:
        com_data.loc[row, 'vader'] = sia.polarity_scores(com_data.loc[row, 'body'])['compound']

    # Store Data
    # Data type fixing
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
    # Save history
    com_data.to_feather(f'www/history/com_data_{datetime.datetime.now():%Y%m%d_%H%M}.ft')
    # Splice history
    com_wc = pd.read_feather('www/com_data.ft')
    com_key = pd.merge(com_data, com_wc, how = 'outer')
    com_key = pd.merge(com_data['id'], com_wc['id'], how = 'outer').drop_duplicates()
    com_key = pd.merge(com_key, com_data, how = 'outer', on = 'id').dropna(how = 'any')
    com_data = pd.merge(com_key, pd.merge(com_key[pd.isnull(com_key['subid'])]['id'], com_wc, how = 'outer', on = 'id'), how = 'outer')
    ### 5.4) Save working copy
    com_data.to_feather('www/com_data.ft')

def scrape_sub_light_date(date = '2021-01-01'):
    
    # Retrieve submissions
    submissions = reddit.subreddit('wallstreetbets').new(limit = n_sub)
    # Parse submissions
    sub_data = pd.DataFrame(columns = ['id', 'title', 'author', 'score', 'flair', 'ncomms', 'created', 'vader'])
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
            'N/A'
        ], 
        index = ['id', 'title', 'author', 'score', 'flair', 'ncomms', 'created', 'body', 'vader']
        ),
            ignore_index = True
        )

    # VADER Sentiment Analyzer
    # Iterate over submissions and comments
    for row in sub_data.index:
        sub_data.loc[row, 'vader'] = sia.polarity_scores(sub_data.loc[row, 'title'])['compound']


    # Store Data
    # Data type fixing
    sub_data = sub_data
    sub_data = sub_data.astype({'id': 'str', # required str conversion due to hidden PRAW-specific dtypes incompatibility with feather
                                'title': 'str',
                                'author': 'str',
                                'score': 'int32', 
                                'flair': 'str',
                                'ncomms': 'int32', 
                                'created': 'datetime64[s]',
                                'vader': 'float32'})

    # Save history
    sub_data.to_feather(f'www/history/sub_data_light_date_{date}.ft')
    # Splice history
    sub_wc = pd.read_feather('www/sub_data_light.ft')
    sub_key = pd.merge(sub_data, sub_wc, how = 'outer')
    sub_key = pd.merge(sub_data['id'], sub_wc['id'], how = 'outer').drop_duplicates()
    sub_key = pd.merge(sub_key, sub_data, how = 'outer', on = 'id').dropna(how = 'any')
    sub_data = pd.merge(sub_key, pd.merge(sub_key[pd.isnull(sub_key['title'])]['id'], sub_wc, how = 'outer', on = 'id'), how = 'outer')

    # Save working copy
    sub_data.to_feather('www/sub_data.ft')
    

# See you in R! 
