import logging
from datetime import datetime

import requests
from BeautifulSoup import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Integer, Numeric, SmallInteger, String)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


Base = declarative_base()


class ScraperRun(Base):
    __tablename__ = 'scraper_run'

    run_id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    movies = relationship('MovieRating')


class MovieMixin(object):
    name = Column(String)
    rating = Column(Numeric)
    rank = Column(SmallInteger)
    link = Column(String, primary_key=True)
    year = Column(SmallInteger)
    release_date = Column(Date)
    number_of_votes = Column(Integer)


class Movie(Base, MovieMixin):
    __tablename__ = 'data'


class MovieRating(Base, MovieMixin):
    __tablename__ = 'movie_rating'

    scraper_run = Column(Integer, ForeignKey('scraper_run.run_id'),
                         nullable=False, primary_key=True)


def _get_db_session():
    engine = create_engine('sqlite:///data.sqlite')
    Base.metadata.create_all(engine)
    return sessionmaker(engine)()


def _get_movie_dict(session, tr):
    ir = tr.find('span', {'name': 'ir'})
    title_a = tr.find('td', {'class': 'titleColumn'}).find('a')
    logging.warning('Processing %s...', title_a.text)
    link = 'http://akas.imdb.com{}'.format(title_a['href'].split('?')[0])
    movie_response = session.get(link)
    soup = BeautifulSoup(movie_response.content)
    release_date_string = soup.find(
        id='overview-top').find('meta', itemprop='datePublished')['content']
    date_templates = {
        2: '%Y-%m-%d',
        1: '%Y-%m',
        0: '%Y',
    }
    date_template = date_templates[release_date_string.count('-')]
    release_date = datetime.strptime(release_date_string, date_template).date()
    return {
        'name': title_a.text,
        'rating': ir['data-value'],
        'rank': tr.find('span', {'name': 'rk'})['data-value'],
        'link': link,
        'year': release_date.year,
        'release_date': release_date,
        'number_of_votes': tr.find('span', {'name': 'nv'})['data-value'],
    }


def _get_movie_dicts():
    session = requests.session()
    response = session.get('http://akas.imdb.com/chart/top')
    soup = BeautifulSoup(response.content)
    tbody = soup.find('tbody', {'class': 'lister-list'})
    for tr in tbody.findAll('tr'):
        yield _get_movie_dict(session, tr)


def main():
    session = _get_db_session()
    session.query(Movie).delete()
    scraper_run = ScraperRun(timestamp=datetime.now())
    session.add(scraper_run)
    session.commit()
    for movie_dict in _get_movie_dicts():
        session.add(Movie(**movie_dict))
        session.add(MovieRating(scraper_run=scraper_run.run_id, **movie_dict))
    session.commit()


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s')
    main()
