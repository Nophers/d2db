import asyncio
import aiohttp
from time import time
import codecs
import json
from pandas import json_normalize
import csv
import os
import mariadb

class Config(object):

    def __init__(self):
        self.config = self._get_config()

    def _get_config(self):
        with open('config.json', 'r') as config_file:
            return json.load(config_file)

config = Config()

rowc = 1
DATABASE_NAME = 'bot'


class Database:
    
    def __init__(self):
        self.db = mariadb.connect(
            user=config.config['user'],
            host=config.config['host'],
            password=config.config['password'],
            port=3306,
            database=config.config['database']
            )

        self.c = self.db.cursor(buffered=True)
        # self.c.execute('CREATE DATABASE IF NOT EXISTS {}'.format(DATABASE_NAME))
        # self.db.database = DATABASE_NAME
        # self.c.execute('SET AUTOCOMMIT=1')
        # self.c.execute(
        #     'ALTER DATABASE {} CHARACTER SET utf8 COLLATE utf8_unicode_ci;'.format(DATABASE_NAME))  # no idea what that does but y not


db = Database().c


class Config:

    def __init__(self):
        self.API_KEY = ''
        self.CSV_PATH = 'data/'
        self.CACHE_PATH = 'cache/'
        self.LEAGUE_ID = 29000022  # legends
        self.seasons = ['2015-07','2015-08','2015-09','2015-10','2015-11','2015-12','2016-01','2016-02','2016-03','2016-04','2016-05','2016-06','2016-07','2016-08','2016-09','2016-10','2016-11','2016-12','2017-01','2017-02','2017-03','2017-04','2017-05','2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01','2018-02','2018-03','2018-04','2018-05','2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01','2020-02','2020-03','2020-04','2020-05','2020-06','2020-07','2020-08','2020-09','2020-10','2020-11','2020-12','2021-01','2021-02','2021-03','2021-04','2021-05',
                        '2021-06','2021-07'
                        ] ,'2021-08','2021-09','2021-10','2021-11','2021-12'#'2022-01']
        self.API_URL = f'https://api.clashofclans.com/v1/leagues/{self.LEAGUE_ID}/seasons/'
        
    async def setup(self):
        print('Setting up...')
        if not 'cache' in os.listdir():
            os.mkdir('cache')
            print('[+] + cache')
        if not 'data' in os.listdir():
            os.mkdir('data')
            print('[+] + data')
        await asyncio.sleep(1)


config = Config()


class Data:

    def __init__(self):
        ...

    async def csvit(self,data,season):
        global rowc
        print('Converting to readable json...')
        s = time()
        with codecs.open(config.CACHE_PATH + season + '.json', 'w', encoding="utf-8") as dataset:
            dataset.write(str(json.dumps(data)))
        with open(config.CACHE_PATH + season + '.json') as data_file:
            d=json.load(data_file)
        df = json_normalize(d, 'items') # .assign(**d[''])
        if df.empty:
            print("[-] No users found. Deleting files...")
            os.remove(config.CACHE_PATH + season + '.json')
            return
        #with codecs.open(config.CSV_PATH + season + '.csv', 'w', encoding="utf-8") as dataset:
            #writer = csv.writer(dataset)
            #writer.writerow(["tag", "name", "expLevel", "trophies", "attackWins", "defenseWins", "rank", "clantag", "clanname", "clanbadgeUrl"])
        db.execute(f'''CREATE TABLE IF NOT EXISTS `{season}` (
            tag TEXT,
            name TEXT,
            expLevel TEXT,
            trophies TEXT,
            attackWins TEXT,
            defenseWins TEXT,
            rank TEXT,
            clantag TEXT,
            clanname TEXT,
            clanbadgeUrl TEXT)''')
        db.execute(f'ALTER TABLE `{season}` CHANGE `name` `name` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL;')
        db.execute(f'ALTER TABLE `{season}` CHANGE `clanname` `clanname` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL;')
        for tag, name, expLevel, trophies, attackWins, defenseWins, rank, clantag, clanname, clanbadgeUrl in zip(
                df['tag'],df['name'],df['expLevel'],df['trophies'],df['attackWins'],df['defenseWins'],df['rank'],df['clan.tag'],df['clan.name'], df['clan.badgeUrls.large']):
                    db.execute(f'INSERT INTO `{season}` VALUES (?,?,?,?,?,?,?,?,?,?)',(str(tag) if tag else 'N/A', str(name) if name else 'N/A', str(expLevel) if expLevel else 'N/A', str(trophies) if trophies else 'N/A', str(attackWins) if attackWins else 'N/A', str(defenseWins) if defenseWins else 'N/A', str(rank) if rank else 'N/A', str(clantag) if clantag else 'N/A', str(clanname) if clanname else 'N/A', str(clanbadgeUrl) if clanbadgeUrl else 'N/A'))
                    print(f'USER: {rowc}, SEASON: {season}')
                    rowc += 1
        print('Deleting cached file...')
        os.remove(config.CACHE_PATH + season + '.json')
        e = time()
        print(f'[+] Done (took {round(e-s,3)}s)')

    async def fetchrecords(self, headers={"Accept": "application/json", "authorization": f"Bearer {config.API_KEY}"}):
        for season in config.seasons:
            print('Fetching users...')
            async with aiohttp.ClientSession() as session:
                s = time()
                async with session.get(config.API_URL + season, headers=headers) as r:
                    r = await r.json()
                    e = time()
                    try:
                        print(f'[+] Found {len(r["items"])} users (took {round(e-s,3)}s)')
                    except:
                        print("[-] No proper json response received. Perhaps your API key is wrong?")
                    await self.csvit(r, season)


data = Data()


async def main():
    await config.setup()
    t = asyncio.create_task(data.fetchrecords())
    await t
    await asyncio.sleep(0.1)


if __name__ == '__main__':
    asyncio.run(main())
