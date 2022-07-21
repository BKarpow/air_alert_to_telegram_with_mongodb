import re
import telebot
import time
from configparser import ConfigParser
from requests import get
from requests.exceptions import HTTPError
from loguru import logger
from pathlib import Path
from pymongo import MongoClient
from os.path import dirname


class AirMongo:

    def __init__(self):
        self.mongo = None
        self._mongo_db = None
        self.collection = None
        self.format_time = "%d.%m.%Y %H:%M:%S"
        self._root = Path(dirname(__file__))
        file_config = self._root / 'config.ini'
        file_log = self._root / 'log.txt'
        self.config = ConfigParser()
        self.config.read(file_config.absolute())
        self._bot_token = self.config['Telegram']['bot_token']
        self._chat_id = self.config['Telegram']['chat_id']
        self._work = True
        self.bot = telebot.TeleBot(self._bot_token)
        self.bot.config['api_key'] = self._bot_token
        self.tags_search = []
        logger.add(file_log.absolute(), rotation='10KB', compression='zip')

    def run(self) -> None:
        self._work = True

    def terminate(self) -> None:
        logger.debug('Stopped pooling...')
        self._work = False

    def send_msg(self, text: str) -> None:
        text = text.replace('<br />', '\n')
        logger.debug(f'Send: {text}')
        res = self.bot.send_message(self._chat_id, text)
        logger.debug(res)

    def add_tag(self, tag: str) -> None:
        self.tags_search.append(tag)

    def connect_to_mongo(self):
        self.mongo = MongoClient(self.config['MongoDB']['mongo_url'])
        self._mongo_db = self.mongo[self.config['MongoDB']['database']]
        self.collection = self._mongo_db[self.config['MongoDB']['collection']]

    def get_data_messages(self) -> list:
        try:
            result = get(self.config['AirAlarm']['resource_json_data_url'])
            if result.status_code == 200:
                return result.json()['messages']
            else:
                logger.error(f'Error result, status code: {result.status_code}')
                return []
        except HTTPError as err:
            # catastrophic error. bail.
            logger.error(err)
            return []

    def get_time(self) -> int:
        d = time.strftime(self.format_time)
        return int(time.mktime(time.strptime(d, self.format_time)))

    def get_data_dict(self, start: int, location: str, message: str, end: object = 0) -> dict:
        return {
            "start": start,
            "end": end,
            "location": location,
            "created_at": self.get_time(),
            "message": message
        }

    def filter_from_tag_message(self, messages: list) -> list:
        fil = []
        for message in messages:
            if message['message'] is None:
                continue
            for tag in self.tags_search:
                if re.search(tag, message['message']):
                    message['location'] = tag
                    fil.append(message)
        return fil

    def start(self, msg: str) -> None:
        logger.debug('Start air alert')
        self.send_msg(msg)

    def stop(self, msg: str) -> None:
        logger.debug('Stop air alert')
        self.send_msg(msg)

    def is_unique_alert(self, msg: dict) -> bool:
        res = self.collection.find_one({"start": msg['date']})
        if res is None:
            di = self.get_data_dict(int(msg['date']), msg['location'], msg['message'])
            if di is not None:
                self.collection.insert_one(di)
                return True
        else:
            return False

    def start_or_stop(self, msg) -> None:
        if re.search(self.config['AirAlarm']['air_tag_start'], msg['message']):
            self.start(msg['message'])
        elif re.search(self.config['AirAlarm']['air_tag_end'], msg['message']):
            self.stop(msg['message'])

    def execute_scan(self):
        msgs = self.get_data_messages()
        msgs = self.filter_from_tag_message(msgs)
        for msg in msgs:
            if self.is_unique_alert(msg):
                self.start_or_stop(msg)

    def pooling(self):
        self.connect_to_mongo()
        if len(self.tags_search) == 0:
            logger.error('No search tag, please add tag using  self.add_tag(str:tag)')
            return None
        try:
            while True:
                if not self._work:
                    logger.debug('Stop pooling.')
                    break
                self.execute_scan()
                time.sleep(int(self.config['General']['loop_timeout']))
        except KeyboardInterrupt:
            logger.info('Pooling stop!')


if __name__ == "__main__":
    a = AirMongo()
    a.add_tag('#Городищенська_територіальна_громада')
    a.add_tag('#Черкаська_область')
    a.pooling()
