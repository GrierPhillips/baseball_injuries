'''
get_games.py: This script is intended as a test for collecting game data for a
    given day.
'''
import calendar
import multiprocessing as mp
import os
import re

from bs4 import BeautifulSoup
import requests


class GetGames(object):
    '''
    GetGames is a class that will allow for the collection of game data from
    the mlb gd2 database for a given time frame. Dates must be entered in
    ISO 8601 format (YYYY-MM-DD).
    '''
    def __init__(self):
        self.year = None
        self.month = None
        self.home = 'http://gd2.mlb.com/components/game/mlb/'

    def get_year(self, year):
        '''
        Given a year in the form YYYY, collect all of the games that took place
        during that year.

        Args:
            year (string): Year in format 'YYYY'.
        '''
        year = str(year)
        self.year = year
        for month in range(1, 13):
            month = format(month, '02d')
            self.month = month
            month = year + '-' + month
            self.get_month(month)

    def get_month(self, month):
        '''
        Given a date in the form 'YYYY-MM', collect all of the games that took
        place during that month.

        Args:
            month (string): Date in the format 'YYYY-MM'.
        '''
        date = month.split('-')
        if not self.year:
            self.year = date[0]
            self.month = date[1]
        days = calendar.monthrange(int(self.year), int(self.month))[1]
        for day in range(1, days + 1):
            self.get_day('-'.join(date) + '-' + format(day, '02d'))

    def get_day(self, date):
        '''
        Given a date in the form 'YYYY-MM-DD', collect all of the games that
        occurred on that day.

        Args:
            date (string): Date in the format 'YYYY-MM-DD' (ISO 8601 format).
        '''
        date = date.split('-')
        if not self.month:
            self.year = date[0]
            self.month = date[1]
        day = date[2]
        url = self.home + 'year_{}/month_{}/day_{}/'.format(
            self.year, self.month, day
        )
        day_page = requests.get(url)
        day_soup = BeautifulSoup(day_page.text, 'html.parser')
        games = day_soup.find_all('a', text=re.compile('gid\w*/'))
        games = [url + game['href'] for game in games]
        # for game in games:
        #     game_url = url + game
        #     self.get_game(game_url)
        self.parallel_get_games(games)

    @staticmethod
    def get_game(game_url):
        '''
        Given a game_url retrieve the data for all players and innings.

        Args:
            game_url (string): The url for the game to be retrieved.
        '''
        players = requests.get(game_url + 'players.xml')
        players = BeautifulSoup(players.text, 'lxml')
        innings = requests.get(game_url + 'inning/inning_all.xml')
        innings = BeautifulSoup(innings.text, 'lxml')
        directory = '/'.join(game_url.split('/')[-5:])
        os.makedirs(directory, mode=0o777, exist_ok=True)
        with open(directory + '/players.xml', 'w') as file_obj:
            file_obj.write(players.prettify())
        with open(directory + '/inning_all.xml', 'w') as file_obj:
            file_obj.write(innings.prettify())

    def parallel_get_games(self, games):
        '''
        Method for getting a list of games using all available processors.
        '''
        pool = mp.Pool()
        pool.map_async(self.get_game, games)
        pool.close()
        pool.join()
