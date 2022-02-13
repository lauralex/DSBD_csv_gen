import asyncio
import threading
from abc import ABC, abstractmethod

from confluent_kafka import SerializingProducer, Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

from app.models import BetDataList
import app.settings as config
from app.utils.advanced_scheduler import async_repeat_deco


class GenericProducer(ABC):
    bootstrap_servers = config.broker_settings.broker
    schema_registry_conf = {'url': config.broker_settings.schema_registry}

    # bootstrap_servers = 'localhost:9092'
    # schema_registry_conf = {'url': 'http://localhost:8081'}

    @abstractmethod
    def model_to_dict(self, obj, ctx):
        ...

    @property
    @abstractmethod
    def schema(self):
        ...

    @abstractmethod
    def produce(self, id, value, headers) -> asyncio.Future:
        ...

    def _produce_data(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def produce_data(self):
        if not self._polling_thread.is_alive():
            self._polling_thread.start()

    def close(self):
        self._cancelled = True
        self._polling_thread.join()

    def reset_state(self):
        self._cancelled = False

    def __init__(self, loop=None, normal_prod=False):
        if not normal_prod:
            schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

            json_serializer = JSONSerializer(self.schema, schema_registry_client, to_dict=self.model_to_dict)

            producer_conf = {'bootstrap.servers': self.bootstrap_servers,
                             'key.serializer': StringSerializer('utf_8'),
                             'value.serializer': json_serializer
                             }
        else:
            producer_conf = {'bootstrap.servers': self.bootstrap_servers}
        self._loop = loop or asyncio.get_event_loop()
        if not normal_prod:
            self._producer = SerializingProducer(producer_conf)
        else:
            self._producer = Producer(producer_conf)
        self._polling_thread = threading.Thread(target=self._produce_data)
        self._cancelled = False


class CsvGenProducer(GenericProducer):
    topic = 'csv-gen-reply'

    def model_to_dict(self, obj: BetDataList, ctx):
        return None

    @property
    def schema(self):
        return None

    def produce(self, id, value, headers=None) -> asyncio.Future:
        result_fut = self._loop.create_future()

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
                self._loop.call_soon_threadsafe(result_fut.set_exception, KafkaException(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
                self._loop.call_soon_threadsafe(result_fut.set_result, msg)

        self._producer.produce(topic=self.topic, key=id, value=value, on_delivery=delivery_report, headers=headers)
        return result_fut


csv_gen_producer: CsvGenProducer


def init_producers():
    @async_repeat_deco(3, 3, always_reschedule=True)
    async def init_csv_gen_producer(_):
        global csv_gen_producer
        csv_gen_producer = CsvGenProducer(asyncio.get_running_loop(), normal_prod=True)
        csv_gen_producer.produce_data()

    @async_repeat_deco(3, 3, always_reschedule=True)
    async def init_betdata_finish_producer(_):
        global bet_data_finish_producer
        bet_data_finish_producer = BetDataFinishProducer(asyncio.get_running_loop(), normal_prod=True)
        bet_data_finish_producer.produce_data()

    asyncio.run_coroutine_threadsafe(init_csv_gen_producer('csv_gen_producer'), loop=asyncio.get_running_loop())


def close_producers():
    csv_gen_producer.close()
