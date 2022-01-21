import asyncio
import csv
import io
import logging
import threading
import uuid
from abc import abstractmethod, ABC

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer

import app.kafka.producers as producers
from app.models import BetDataList, BetData


class GenericConsumer(ABC):
    bootstrap_servers = 'broker:29092'

    @property
    @abstractmethod
    def group_id(self):
        ...

    @property
    @abstractmethod
    def auto_offset_reset(self):
        ...

    @property
    @abstractmethod
    def auto_commit(self):
        ...

    @property
    @abstractmethod
    def topic(self):
        ...

    @property
    @abstractmethod
    def schema(self):
        ...

    @abstractmethod
    def dict_to_model(self, map, ctx):
        ...

    def close(self):
        self._cancelled = True
        self._polling_thread.join()

    def consume_data(self):
        if not self._polling_thread.is_alive():
            self._polling_thread.start()

    @abstractmethod
    def _consume_data(self):
        ...

    def reset_state(self):
        self._cancelled = False

    def __init__(self, loop=None):
        json_deserializer = JSONDeserializer(self.schema,
                                             from_dict=self.dict_to_model)
        string_deserializer = StringDeserializer('utf_8')

        consumer_conf = {'bootstrap.servers': self.bootstrap_servers,
                         'key.deserializer': string_deserializer,
                         'value.deserializer': json_deserializer,
                         'group.id': self.group_id,
                         'auto.offset.reset': self.auto_offset_reset,
                         'enable.auto.commit': self.auto_commit,
                         'allow.auto.create.topics': True}
        self._loop = loop or asyncio.get_event_loop()
        self._consumer = DeserializingConsumer(consumer_conf)
        self._cancelled = False
        self._consumer.subscribe([self.topic])
        self._polling_thread = threading.Thread(target=self._consume_data)


class CsvGenConsumer(GenericConsumer):

    @property
    def group_id(self):
        return 'my_group'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'csv_gen'

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "CSV Generation Request",
  "description": "CSV Generation Kafka Request",
  "type": "object",
  "properties": {
    "data": {
      "description": "Bet Data",
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "date": {
            "type": "string"
          },
          "match": {
            "type": "string"
          },
          "one": {
            "type": "string"
          },
          "ics": {
            "type": "string"
          },
          "two": {
            "type": "string"
          },
          "gol": {
            "type": "string"
          },
          "over": {
            "type": "string"
          },
          "under": {
            "type": "string"
          }
        }
      }
    }
  }
}"""

    def dict_to_model(self, map, ctx):
        if map is None:
            return None

        return BetDataList.parse_obj(map)

    @staticmethod
    def parsing_to_csv(bet_data: BetDataList, f_obj):
        # using uuid.uuid4() to get random names for our csv files

        csv_writer = csv.DictWriter(f_obj, fieldnames=BetData.__fields__.keys())
        csv_writer.writeheader()
        csv_writer.writerows(data.dict() for data in bet_data.data)

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                # headers: [0] channel_id, [1] web_site, [2] category
                bet_data: BetDataList = msg.value()
                if bet_data is not None:

                    with io.StringIO(newline='') as str_buf:
                        self.parsing_to_csv(bet_data, str_buf)

                        async def sequential_finish():
                            await producers.bet_data_finish_producer.produce(msg.key(), 'success', headers=msg.headers())
                            await producers.csv_gen_producer.produce(msg.key(), str_buf.read(), headers=msg.headers())

                        asyncio.run_coroutine_threadsafe(sequential_finish(), loop=self._loop).result()
                    self._consumer.commit(msg)
                else:
                    logging.warning(f'Null value for the message: {msg.key()}')
                    self._consumer.commit(msg)
            except Exception as exc:
                logging.error(exc)
                try:
                    self._consumer.commit(msg)
                except:
                    pass

                # break

        self._consumer.close()


csv_gen_consumer: CsvGenConsumer


def init_consumers(client=None):
    global csv_gen_consumer
    csv_gen_consumer = CsvGenConsumer(asyncio.get_running_loop())
    csv_gen_consumer.consume_data()


def close_consumers():
    csv_gen_consumer.close()
