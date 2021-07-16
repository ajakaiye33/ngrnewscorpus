"""A script for scraping news sites and writing latest articles to
json and S3.
"""

import sys
import pandas as pd
import json
from time import mktime
from datetime import datetime
import configparser
import boto3
import feedparser as fp
import newspaper
from newspaper import Article
import jsonlines
import nltk
nltk.download('punkt')


data = {}
data["newspapers"] = {}


def parse_config(fname):
    # Loads the JSON files with news sites
    with open(fname, "r") as data_file:
        cfg = json.load(data_file)

    for company, value in cfg.items():
        if "link" not in value:
            raise ValueError(f"Configuration item {company} missing obligatory 'link'.")

    return cfg


def _handle_rss(company, value, count, limit):
    """If a RSS link is provided in the JSON file, this will be the first
    choice.

    Reason for this is that, RSS feeds often give more consistent and
    correct data.

    If you do not want to scrape from the RSS-feed, just leave the RSS
    attr empty in the JSON file.
    """

    fpd = fp.parse(value["rss"])
    print(f"Downloading articles from {company}")
    news_paper = {"rss": value["rss"], "link": value["link"], "articles": []}
    for entry in fpd.entries:
        # Check if publish date is provided, if no the article is
        # skipped.  This is done to keep consistency in the data and to
        # keep the script from crashing.
        if not hasattr(entry, "published"):
            continue
        if count > limit:
            break
        article = {}
        article["link"] = entry.link
        date = entry.published_parsed
        article["published"] = datetime.fromtimestamp(mktime(date)).isoformat()
        try:
            content = Article(entry.link)
            content.download()
            content.parse()
        except Exception as err:
            # If the download for some reason fails (ex. 404) the
            # script will continue downloading the next article.
            print(err)
            print("continuing...")
            continue
        article["title"] = content.title
        article["text"] = content.text
        news_paper["articles"].append(article)
        print(f"{count} articles downloaded from {company}, url: {entry.link}")
        count = count + 1
    return count, news_paper


def _handle_fallback(company, value, count, limit):
    """This is the fallback method if a RSS-feed link is not provided.

    It uses the python newspaper library to extract articles.

    """

    print(f"Building site for {company}")
    paper = newspaper.build(value["link"], memoize_articles=True)
    news_paper = {"link": value["link"], "articles": []}
    none_type_count = 0
    run_date = datetime.now()
    for content in paper.articles:
        if count > limit:
            break
        try:
            content.download()
            content.parse()
            content.nlp()
        except Exception as err:
            print(err)
            print("continuing...")
            continue
        # Again, for consistency, if there is no found publish date the
        # article will be skipped.
        #
        # After 10 downloaded articles from the same newspaper without
        # publish date, the company will be skipped.
        if content.publish_date is None:
            print(f"{count} Article has date of type None...")
            none_type_count = none_type_count + 1
            if none_type_count > 10:
                print("Too many noneType dates, aborting...")
                none_type_count = 0
                break
            count = count + 1
            continue
        article = {
            "title": content.title,
            "text": content.text,
            "summary": content.summary,
            "link": content.url,
            "published": content.publish_date.strftime("%m/%d/%Y"),
            "scraped_date": run_date.strftime("%m/%d/%Y"),
            "keywords": content.keywords,
        }
        news_paper["articles"].append(article)
        print(
            f"{count} articles downloaded from {company} using newspaper, url: {content.url}"
        )
        count = count + 1
        none_type_count = 0
    return count, news_paper


def run(config, limit=5):
    """Take a config object of sites and urls, and an upper limit.

    Iterate through each news company.

    Write result to scraped_articles.json.
    """

    for company, value in config.items():
        count = 1
        if "rss" in value:
            count, news_paper = _handle_rss(company, value, count, limit)
        else:
            count, news_paper = _handle_fallback(company, value, count, limit)
        data["newspapers"][company] = news_paper
    export_file = "./docs/scraped_articles.json"
    #updatejson = "./docs/testy.jsonl"
    cleaned_data = "./docs/bigdata.csv"
    filtered_data = "./docs/historical_corpus.csv"
    summary_news = "./docs/todays-news-summary.json"

    # Finally it saves the articles as a JSON-file.
    try:
        with open(export_file, "w") as outfile:
            json.dump(data, outfile, indent=2)

        with open('./docs/scraped_articles.json', 'r') as f:
            prety_data = json.load(f)

        see_data = []
        summary_data = []
        # filter data

        for newspaperz in prety_data['newspapers']:
            for j in prety_data['newspapers'][newspaperz]['articles']:
                headline_text = {}
                headline_text['title'] = j.get('title')
                headline_text['text'] = j.get('text')
                headline_text['summary'] = j.get('summary')
                headline_text['link'] = j.get('link')
                headline_text['published'] = j.get('published')
                headline_text['scraped_date'] = j.get('scraped_date')
                headline_text['keywords'] = j.get('keywords')
                see_data.append(headline_text)
        #print(see_data)

#         with open(updatejson, mode='a') as f:
#             for entry in see_data:
#                 json.dump(entry, f)
#                 f.write('\n')
        
        #our_data = pd.DataFrame(see_data)
        our_data = pd.read_csv('https://raw.githubusercontent.com/ajakaiye33/ngrnewscorpus/main/docs/bigdata.csv')
        our_data = our_data[our_data['scraped_date'] == our_data['published']].drop_duplicates(subset=['title'], keep='last')
        our_data.to_csv(filtered_data, mode="a", header=False, index=False)
        
        for i in prety_data['newspapers']:
            for h in prety_data['newspapers'][i]['articles']:
                summary_text = {}
                summary_text['headline'] = h.get('title')
                summary_text['summarized_story'] = h.get('summary')
                summary_data.append(summary_text)

        with open(summary_news, "w") as juju:
            json.dump(summary_data, juju, indent=2)

    except Exception as err:
        print(err)


def main():

    fname = './NewCoy.json'
    try:
        config = parse_config(fname)
    except Exception as err:
        sys.exit(err)
    run(config)


if __name__ == "__main__":
    main()
