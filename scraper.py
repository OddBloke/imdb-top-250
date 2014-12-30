from datetime import datetime

import requests
from BeautifulSoup import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy import Column, Date, Integer, Numeric, SmallInteger, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class Movie(Base):
    __tablename__ = 'data'

    name = Column(String)
    rating = Column(Numeric)
    rank = Column(SmallInteger)
    link = Column(String, primary_key=True)
    year = Column(SmallInteger)
    release_date = Column(Date)
    number_of_votes = Column(Integer)


def _get_db_session():
    engine = create_engine('sqlite:///data.sqlite')
    Base.metadata.create_all(engine)
    return sessionmaker(engine)()


def _get_movie_dict(tr):
    ir = tr.find('span', {'name': 'ir'})
    title_a = tr.find('td', {'class': 'titleColumn'}).find('a')
    rd = tr.find('span', {'name': 'rd'})
    release_date = datetime.strptime(rd['data-value'], '%Y-%m-%d').date()
    return {
        'name': title_a.text,
        'rating': ir['data-value'],
        'rank': ir.text.strip('.'),
        'link': 'http://akas.imdb.com{}'.format(title_a['href'].split('?')[0]),
        'year': rd.text.strip('()'),
        'release_date': release_date,
        'number_of_votes': tr.find('strong', {'name': 'nv'})['data-value'],
    }


def _get_movie_dicts():
    response = requests.get('http://akas.imdb.com/chart/top')
    soup = BeautifulSoup(response.content)
    tbody = soup.find('tbody', {'class': 'lister-list'})
    for tr in tbody.findAll('tr'):
        yield _get_movie_dict(tr)


def main():
    session = _get_db_session()
    for movie_dict in _get_movie_dicts():
        movie = Movie(**movie_dict)
        session.add(movie)
    session.commit()


if __name__ == "__main__":
    main()
