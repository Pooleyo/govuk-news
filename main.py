import feedparser
import requests
from bs4 import BeautifulSoup
import plotly.express as px
import pandas as pd
import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from wordcloud import WordCloud
import matplotlib.pyplot as plt

Base = declarative_base()

class Organisation(Base):
    __tablename__ = 'organisations'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    articles = relationship('Article', back_populates='organisation')

class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True)
    feed_id = Column(String, unique=True, nullable=False)
    title = Column(String, nullable=False)
    link = Column(String, nullable=False)
    summary = Column(String)
    updated = Column(DateTime)
    body_text = Column(String)
    organisation_id = Column(Integer, ForeignKey('organisations.id'))
    organisation = relationship('Organisation', back_populates='articles')

# Ensure data directory exists
data_dir = 'data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Create SQLite engine
engine = create_engine(f'sqlite:///{data_dir}/gov_uk_news.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

def parse_feed(url):
    """
    Parses an Atom feed and extracts relevant information.

    Args:
      url: The URL of the Atom feed.

    Returns:
      A list of dictionaries, where each dictionary represents a feed entry.
    """
    feed = feedparser.parse(url)
    entries = []

    for entry in feed.entries:
        entries.append({
            'id': entry.id,
            'title': entry.title,
            'link': entry.link,
            'summary': entry.summary,
            'updated': datetime.strptime(entry.updated, "%Y-%m-%dT%H:%M:%S%z")
        })

    return entries

def get_article_details(url):
    """
    Fetches an article from a given URL and extracts body text and organisation.

    Args:
      url: The URL of the article.

    Returns:
      A dictionary containing the body text and organisation.
    """
    details = {'body_text': None, 'organisation': None}

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract body text (this might need adjustment based on the website structure)
        try:
            details['body_text'] = soup.find('div',
                                             class_='gem-c-govspeak').get_text(
                                                 strip=True)
        except:
            print(f"Error extracting body text from {url}")

        # Extract organisation (this assumes the organisation is in a meta tag)
        try:
            details[
                'organisation'] = soup.find(
                    'meta', {
                        'name': 'govuk:primary-publishing-organisation'
                    })['content']
        except:
            print(f"Error extracting organisation from {url}")

        return details

    except requests.exceptions.RequestException as e:
        print(f"Error fetching article: {e}")
        return None

def create_organisation_plot():
    """
    Creates a bar plot of the number of articles per organisation using data from the database.
    """
    session = Session()

    # Query the database to get the count of articles per organisation
    org_counts = session.query(Organisation.name, func.count(Article.id)).join(Article).group_by(Organisation.name).all()

    session.close()

    # Convert the query result to a pandas Series
    org_counts_series = pd.Series(dict(org_counts))

    # Sort the Series in descending order
    org_counts_series = org_counts_series.sort_values(ascending=False)

    # Create bar chart using Plotly Express with beautiful settings and sorted data
    fig = px.bar(x=org_counts_series.index,
                 y=org_counts_series.values,
                 labels={'x': 'Organisation', 'y': 'Number of Articles'},
                 title='Number of Articles per Organisation',
                 template="plotly_white",  # Use a clean template
                 color=org_counts_series.index,  # Color bars by organization
                 color_discrete_sequence=px.colors.qualitative.Pastel,  # Use a pastel color palette
                 )

    fig.update_layout(
        title_x=0.5,  # Center the title
        xaxis_tickangle=-45,  # Rotate x-axis labels for better readability
        bargap=0.1,  # Adjust gap between bars
        showlegend=False,  # Hide the legend (as it's the same as x-axis)
    )

    fig.show()

def create_daily_releases_plot():
    """
    Creates a line plot of the total releases per day.
    """
    session = Session()

    # Query the database to get the count of articles per day
    daily_counts = session.query(func.date(Article.updated), func.count(Article.id)).group_by(func.date(Article.updated)).all()

    session.close()

    # Convert the query result to a pandas DataFrame
    df = pd.DataFrame(daily_counts, columns=['Date', 'Count'])
    df['Date'] = pd.to_datetime(df['Date'])

    # Create line chart using Plotly Express
    fig = px.line(df, x='Date', y='Count',
                  labels={'Count': 'Number of Releases'},
                  title='Total Releases per Day',
                  template="plotly_white")

    fig.update_layout(
        title_x=0.5,  # Center the title
    )

    fig.show()

def create_daily_releases_by_org_plot():
    """
    Creates a line plot of the total releases per day, colored by organisation.
    """
    session = Session()

    # Query the database to get the count of articles per day per organisation
    daily_org_counts = session.query(func.date(Article.updated), Organisation.name, func.count(Article.id))\
        .join(Organisation)\
        .group_by(func.date(Article.updated), Organisation.name)\
        .all()

    session.close()

    # Convert the query result to a pandas DataFrame
    df = pd.DataFrame(daily_org_counts, columns=['Date', 'Organisation', 'Count'])
    df['Date'] = pd.to_datetime(df['Date'])

    # Create line chart using Plotly Express
    fig = px.line(df, x='Date', y='Count', color='Organisation',
                  labels={'Count': 'Number of Releases'},
                  title='Total Releases per Day by Organisation',
                  template="plotly_white")

    fig.update_layout(
        title_x=0.5,  # Center the title
    )

    fig.show()

def create_hourly_releases_plot():
    """
    Creates a bar plot of the total releases by hour of the day.
    """
    session = Session()

    # Query the database to get the count of articles by hour
    hourly_counts = session.query(func.extract('hour', Article.updated), func.count(Article.id))\
        .group_by(func.extract('hour', Article.updated))\
        .all()

    session.close()

    # Convert the query result to a pandas DataFrame
    df = pd.DataFrame(hourly_counts, columns=['Hour', 'Count'])
    df['Hour'] = df['Hour'].astype(int)

    # Create a DataFrame with all 24 hours
    all_hours = pd.DataFrame({'Hour': range(24)})
    
    # Merge with the existing data, filling missing values with 0
    df = all_hours.merge(df, on='Hour', how='left').fillna(0)
    df = df.sort_values('Hour')

    # Convert Hour to time range
    df['Time Range'] = df['Hour'].apply(lambda x: f"{x:02d}:00 - {(x+1)%24:02d}:00")

    # Create bar chart using Plotly Express
    fig = px.bar(df, x='Time Range', y='Count',
                 labels={'Count': 'Number of Releases', 'Time Range': 'Hour of Day'},
                 title='Total Releases by Hour of Day',
                 template="plotly_white")

    fig.update_layout(
        title_x=0.5,  # Center the title
        xaxis=dict(tickangle=45),  # Rotate x-axis labels by 45 degrees
        bargap=0.1,  # Adjust gap between bars
    )

    fig.show()

def create_wordcloud():
    """
    Creates a wordcloud of the body text in each article.
    """
    session = Session()

    # Query the database to get all body texts
    body_texts = session.query(Article.body_text).all()

    session.close()

    # Combine all body texts into a single string
    text = ' '.join([body[0] for body in body_texts if body[0] is not None])

    # Create and generate a word cloud image
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)

    # Display the generated image
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title('Word Cloud of Article Body Texts')
    plt.show()
    
if __name__ == "__main__":
    feed_url = "https://www.gov.uk/search/news-and-communications.atom"
    entries = parse_feed(feed_url)

    total_articles = len(entries)
    existing_articles = 0
    new_articles = 0
    partial_failure_count = 0

    session = Session()

    for entry in entries:
        print(f"Title: {entry['title']}")
        print(f"Link: {entry['link']}")
        print(f"Summary: {entry['summary']}")
        print(f"Updated: {entry['updated']}")

        # Check if article already exists in database
        existing_article = session.query(Article).filter_by(feed_id=entry['id']).first()
        if existing_article:
            print("Article already exists in database. Skipping...")
            existing_articles += 1
            continue

        new_articles += 1
        details = get_article_details(entry['link'])
        if details:
            if details['body_text'] is None or details['organisation'] is None:
                partial_failure_count += 1
            print(f"Body Text: {details['body_text'][:100] if details['body_text'] else 'Not available'}...")
            print(f"Organisation: {details['organisation'] if details['organisation'] else 'Not available'}")

            # Get or create organisation
            if details['organisation']:
                organisation = session.query(Organisation).filter_by(name=details['organisation']).first()
                if not organisation:
                    organisation = Organisation(name=details['organisation'])
                    session.add(organisation)
            else:
                organisation = None

            # Create new article
            new_article = Article(
                feed_id=entry['id'],
                title=entry['title'],
                link=entry['link'],
                summary=entry['summary'],
                updated=entry['updated'],
                body_text=details['body_text'],
                organisation=organisation
            )
            session.add(new_article)
        else:
            partial_failure_count += 1
            # Create new article with null values for failed parsing
            new_article = Article(
                feed_id=entry['id'],
                title=entry['title'],
                link=entry['link'],
                summary=entry['summary'],
                updated=entry['updated'],
                body_text=None,
                organisation=None
            )
            session.add(new_article)

        print("-" * 20)

    session.commit()
    session.close()

    print("\nSummary:")
    print(f"Total articles in feed: {total_articles}")
    print(f"Articles already in database: {existing_articles}")
    print(f"New articles added: {new_articles}")
    print(f"Articles with some parsing failure: {partial_failure_count}")

    # Create and show the plots
    create_organisation_plot()
    create_daily_releases_plot()
    create_daily_releases_by_org_plot()
    create_hourly_releases_plot()
    create_wordcloud()