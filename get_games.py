"""
Module for collecting historic MLB game data.

This module contains methods for collecting MLB game data for the pitch/fx era
in a variety of time frames.
"""
import calendar
import multiprocessing as mp
import os
import re
import threading
import time

from bs4 import BeautifulSoup
import requests
from stem import Signal
from stem.control import Controller


class GetGames(object):
    """
    Collect pitch by pitch data from MLB games listed on gd2.mlb.com.

    This class will allow for the collection of game data from the mlb gd2
    database for a given time frame. Dates must be entered in
    ISO 8601 format (YYYY-MM-DD). This module requires that TOR be running
    and properly configured to execute properly.
    """

    def __init__(self):
        """Initialize GetGames object."""
        self.sessions = []
        self.setup_sessions()
        self.setup_proxies()
        self.year = None
        self.month = None
        self.home = 'http://gd2.mlb.com/components/game/mlb/'
        self.tasks = mp.Queue()
        # self.requests = 0

    def setup_sessions(self):
        """Setup as many requests Sessions as there are cores available."""
        for _ in range(mp.cpu_count()):
            self.sessions.append(requests.Session())

    def setup_proxies(self):
        """Setup proxies for all sessions."""
        all_proxies = [9050] + list(range(9052, 9071))
        for index, session in enumerate(self.sessions):
            proxy = all_proxies[index]
            proxies = {
                'http': 'socks5://127.0.0.1:{}'.format(proxy),
                'https': 'socks5://127.0.0.1:{}'.format(proxy)
            }
            setattr(session, 'proxies', proxies)

    @staticmethod
    def renew_connection():
        """Change tor exit node. This will allow cycling of IP addresses."""
        with Controller.from_port(port=9051) as controller:
            controller.authenticate(password="password")
            controller.signal(Signal.NEWNYM)

    # def get_site(self, url, session_num=0):
    #     """
    #     Retrieve html from a site using a specific requests.Session object.
    #
    #     Args:
    #         url (string): Website to pull html from.
    #
    #     Kwargs:
    #         session_num (int): Index of self.sessions to use for request.
    #
    #     Returns:
    #         html (string): Html of desired site.
    #     """
    #     html = self.sessions[session_num].get(url).text
    #     # try:
    #     #     html = self.sessions[session_num].get(url).text
    #     # except:
    #     #     self.renew_connection()
    #     #     html = self.sessions[session_num].get(url).text
    #     # self.requests += 1
    #     # print(self.requests, url)
    #     # if self.requests > 200:
    #     #     self.requests = 0
    #     #     self.renew_connection()
    #     return html

    def get_year(self, year):
        """
        Collect all games from a given year.

        Given a year in the form YYYY, collect all of the games that took place
        during that year.

        Args:
            year (string): Year in format 'YYYY'.
        """
        year = str(year)
        self.year = year
        for month in range(1, 13):
            month = format(month, '02d')
            self.month = month
            month = year + '-' + month
            self.get_month(month)

    def get_month(self, month):
        """
        Collect all game links for a given month.

        Given a month in the format 'YYYY-MM', collect all of the games that
        occurred in that month.

        Args:
            month (string): Month in the format 'YYYY-MM' (ISO 8601 format).
        """
        date = month.split('-')
        if not self.year:
            self.year = date[0]
            self.month = date[1]
        days = calendar.monthrange(int(self.year), int(self.month))[1]
        days = [format(day, '02d') for day in range(1, days + 1)]
        days = ['-'.join(date) + '-' + day for day in days]
        threads = []
        for index, day in enumerate(days):
            index = index % mp.cpu_count()
            thread = threading.Thread(
                target=self.get_day,
                args=(day, index, )
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

    def get_day(self, date, session_num):
        """
        Collect all games from a given day.

        Given a date in the form 'YYYY-MM-DD', collect all of the games that
        occurred on that day.

        Args:
            date (string): Date in the format 'YYYY-MM-DD' (ISO 8601 format).
            session_num (int): Integer representing the session number to use
                for making requests.
        """
        session = self.sessions[session_num]
        date = date.split('-')
        if not self.month:
            self.year = date[0]
            self.month = date[1]
        day = date[2]
        url = self.home + 'year_{}/month_{}/day_{}/'.format(
            self.year, self.month, day
        )
        day_page = session.get(url)
        day_soup = BeautifulSoup(day_page.text, 'html.parser')
        games = day_soup.find_all('a', text=re.compile('gid\w*/'))
        games = [url + game['href'] for game in games]
        for game in games:
            if os.path.exists('/'.join(game.split('/')[-5:])):
                continue
            self.tasks.put(game)

    def _get_game(self, session_num):
        """
        Given a game_url retrieve the data for all players and innings.

        Args:
            session_num (int): Integer representing the session number to use
                for making requests.
        """
        while True:
            game_url = self.tasks.get()
            if not game_url:
                break
            session = self.sessions[session_num]
            players = session.get(game_url + 'players.xml')
            players = BeautifulSoup(players, 'lxml')
            innings = session.get(game_url + 'inning/inning_all.xml')
            innings = BeautifulSoup(innings, 'lxml')
            directory = '/'.join(game_url.split('/')[-5:])
            os.makedirs(directory, mode=0o777, exist_ok=True)
            with open(directory + '/players.xml', 'w') as file_obj:
                file_obj.write(players.prettify())
            with open(directory + '/inning_all.xml', 'w') as file_obj:
                file_obj.write(innings.prettify())

    # def get_games_concurrent(self, games):
    #     threads = []
    #     for index, game in enumerate(games):
    #         index = index % 8
    #         thread = threading.Thread(
    #             target=self.get_game,
    #             args=(game, index, )
    #         )
    #         threads.append(thread)
    #         thread.start()
    #     for thread in threads:
    #         thread.join()

    def _get_games(self):
        """
        Retrieve data for all games in self.Tasks.

        Using all available processes, retrieve and store inning and player
        data for all games in the tasks Queue.
        """
        for _ in range(mp.cpu_count()):
            self.tasks.put(None)
        workers = []
        for _ in range(mp.cpu_count()):
            worker = mp.Process(target=self._get_game, args=(_,))
            workers.append(worker)
            worker.start()
        for worker in workers:
            worker.join()
        for worker in workers:
            worker.terminate()

    def get_all_years(self):
        """Retrieve data for all games beginning in 2007."""
        curr_year = int(time.strftime('%Y', tuple=None))
        for year in range(2007, curr_year + 1):
            self.get_year(str(year))
            self._get_games()
