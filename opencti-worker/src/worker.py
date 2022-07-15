import base64
import ctypes
import functools
import json
import logging
import random
import sys
import time
from pathlib import Path
from threading import Thread
from typing import Any, Dict, List

import pika
import yaml
from pika.adapters.blocking_connection import BlockingChannel
from pycti import OpenCTIApiClient
from pycti.connector.opencti_connector_helper import (
    create_ssl_context,
    get_config_variable,
)
from requests.exceptions import RequestException, Timeout

PROCESSING_COUNT: int = 4
MAX_PROCESSING_COUNT: int = 60

logger = logging.getLogger(__name__)


class Consumer(Thread):
    """Worker consumer thread."""

    def __init__(
        self,
        connector: Dict,
        opencti_url: str,
        opencti_token: str,
        log_level: str,
        ssl_verify: bool = False,
        json_logging: bool = False,
    ) -> None:
        """
        Initialize a new thread.
        :param connector: Connector configuration.
        :param opencti_url: OpenCTI url.
        :param opencti_token: OpenCTI token.
        :param log_level: Logging level.
        :param ssl_verify: Verify the OpenCTI SSL connection.
        :param json_logging: Enable JSON logging.
        """
        super().__init__()

        self.api = OpenCTIApiClient(
            url=opencti_url,
            token=opencti_token,
            log_level=log_level,
            ssl_verify=ssl_verify,
            json_logging=json_logging,
        )
        self.queue_name = connector["config"]["push"]

        pika_ssl_context = (
            pika.SSLOptions(create_ssl_context())
            if connector["config"]["connection"]["use_ssl"]
            else None
        )
        pika_credentials = pika.PlainCredentials(
            username=connector["config"]["connection"]["user"],
            password=connector["config"]["connection"]["pass"],
        )
        pika_parameters = pika.ConnectionParameters(
            host=connector["config"]["connection"]["host"],
            port=connector["config"]["connection"]["port"],
            virtual_host=connector["config"]["connection"]["vhost"],
            credentials=pika_credentials,
            ssl_options=pika_ssl_context,
        )

        self.pika_connection = pika.BlockingConnection(pika_parameters)
        self.channel = self.pika_connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.processing_count: int = 0
        self.current_bundle_id: [str, None] = None
        self.current_bundle_seq: int = 0

    def run(self) -> None:
        """
        Run the thread.
        :return: None.
        """
        try:
            # Consume the queue
            logger.info("Started thread for queue %s", self.queue_name)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._process_message,
            )
            self.channel.start_consuming()
        finally:
            self.channel.stop_consuming()
            logger.info(f"Terminated thread for queue %s", self.queue_name)

    def terminate(self) -> None:
        """
        Asynchronously raise an exception in this thread, thereby terminating it.
        :return: None.
        """
        result = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            self.ident,
            ctypes.py_object(SystemExit),
        )
        if result > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(self.ident, 0)
            logger.info("Unable to kill the thread: %d", result)

    def nack_message(self, channel: BlockingChannel, delivery_tag: int) -> None:
        """
        NACK a message.
        :param channel: Pika channel.
        :param delivery_tag: Delivery tag.
        :return: None.
        """
        if channel.is_open:
            logger.info(
                "Message (delivery_tag=%d) rejected",
                delivery_tag,
            )
            channel.basic_nack(delivery_tag)
        else:
            logger.info(
                "Message (delivery_tag=%d) NOT rejected (channel closed)",
                delivery_tag,
            )

    def ack_message(self, channel: BlockingChannel, delivery_tag: int) -> None:
        """
        ACK a message.
        :param channel: Pika channel.
        :param delivery_tag: Delivery tag.
        :return: None.
        """
        if channel.is_open:
            logger.info(
                "Message (delivery_tag=%d) acknowledged",
                delivery_tag,
            )
            channel.basic_ack(delivery_tag)
        else:
            logger.info(
                f"Message (delivery_tag=%d) NOT acknowledged (channel closed)",
                delivery_tag,
            )

    def stop_consume(self, channel: BlockingChannel) -> None:
        """
        Update the channel to stop consuming.
        :param channel: Pika channel.
        :return: None.
        """
        if channel.is_open:
            channel.stop_consuming()

    def _process_message(
        self,
        channel: BlockingChannel,
        method: pika.spec.Basic.Deliver,
        _properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        """
        Process a message from the channel.
        :param channel: Pika channel.
        :param method: Method spec.
        :param _properties: Basic properties spec.
        :param body: Message body.
        :return: None.
        """
        data = json.loads(body)
        logger.info(
            "Processing a new message (delivery_tag=%d), launching a thread",
            method.delivery_tag,
        )

        thread = Thread(
            target=self._data_handler,
            args=[self.pika_connection, channel, method.delivery_tag, data],
        )
        thread.start()

        while thread.is_alive():  # Loop while the thread is processing
            self.pika_connection.sleep(0.05)

        logger.info("Message processed, thread terminated")

    def _data_handler(
        self,
        connection: Any,
        channel: BlockingChannel,
        delivery_tag: int,
        data: Dict[str, Any],
    ) -> bool:
        """
        Handle the data from a message.
        :param connection: Connector config.
        :param channel: Pika channel.
        :param delivery_tag: Delivery tag.
        :param data: Message data.
        :return: True for success, False otherwise.
        """
        work_id = data.get("work_id")

        # Set the API headers
        applicant_id = data["applicant_id"]
        self.api.set_applicant_id_header(applicant_id)

        # Execute the import
        self.processing_count += 1
        content = "Unparseable"

        try:
            content = base64.b64decode(data["content"]).decode("utf-8")
            types = data.get("entities_types") or None
            update = data.get("update", False)
            retry_number = self.processing_count
            if self.processing_count == PROCESSING_COUNT:
                retry_number = None
            self.api.stix2.import_bundle_from_json(content, update, types, retry_number)

            # Ack the message
            callback = functools.partial(self.ack_message, channel, delivery_tag)
            connection.add_callback_threadsafe(callback)

            if work_id is not None:
                self.api.work.report_expectation(work_id, None)

            self.processing_count = 0
            return True

        except Timeout as te:
            logger.warning("A connection timeout occurred: %s", te)

            # Platform is under heavy load: wait for unlock & retry almost indefinitely.
            sleep_jitter = round(random.uniform(10, 30), 2)
            time.sleep(sleep_jitter)

            return self._data_handler(connection, channel, delivery_tag, data)

        except RequestException as re:
            logger.exception("A connection error occurred: %s", re)
            time.sleep(60)

            logger.info(f"Message (delivery_tag=%d) NOT acknowledged", delivery_tag)
            callback = functools.partial(self.nack_message, channel, delivery_tag)
            connection.add_callback_threadsafe(callback)

            self.processing_count = 0
            return False

        except Exception as ex:  # pylint: disable=broad-except
            error = str(ex)
            under_max_count = self.processing_count < MAX_PROCESSING_COUNT
            if "LockError" in error and under_max_count:
                sleep_jitter = round(random.uniform(10, 30), 2)
                time.sleep(sleep_jitter)

                logger.warning("Platform under heavy load, trying again: %s", ex)
                return self._data_handler(connection, channel, delivery_tag, data)

            elif "MissingReferenceError" in error and under_max_count:
                # In case of missing reference, wait & retry
                sleep_jitter = round(random.uniform(1, 3), 2)
                time.sleep(sleep_jitter)

                logger.info(
                    "Message (delivery_tag=%d) reprocess (retry nb: %d)",
                    delivery_tag,
                    self.processing_count,
                )
                return self._data_handler(connection, channel, delivery_tag, data)

            elif "Bad Gateway" in error:
                logger.error("A connection error occurred: %s", ex)
                time.sleep(60)

                logger.info(
                    "Message (delivery_tag=%d) NOT acknowledged",
                    delivery_tag,
                )
                callback = functools.partial(self.nack_message, channel, delivery_tag)
                connection.add_callback_threadsafe(callback)

                self.processing_count = 0
                return False

            else:
                # Platform does not know what to do and raises an error:
                # fail and acknowledge the message.
                logger.error("Unexpected error while handling data: %s", ex)
                self.processing_count = 0

                callback = functools.partial(self.ack_message, channel, delivery_tag)
                connection.add_callback_threadsafe(callback)

                if work_id is not None:
                    source = content if len(content) < 50000 else "Bundle too large"
                    self.api.work.report_expectation(
                        work_id, {"error": error, "source": source}
                    )

                return False


class Worker:
    def __init__(self):
        # Get configuration
        config_file_path = Path(__file__).absolute().parent.joinpath("config.yml")
        if config_file_path.is_file():
            with config_file_path.open("r") as fd:
                config = yaml.load(fd, Loader=yaml.SafeLoader)
        else:
            config = {}

        # Load API config
        self.opencti_url = get_config_variable(
            "OPENCTI_URL", ["opencti", "url"], config
        )
        self.opencti_token = get_config_variable(
            "OPENCTI_TOKEN", ["opencti", "token"], config
        )
        self.opencti_ssl_verify = get_config_variable(
            "OPENCTI_SSL_VERIFY", ["opencti", "ssl_verify"], config, default=False
        )
        self.opencti_json_logging = get_config_variable(
            "OPENCTI_JSON_LOGGING", ["opencti", "json_logging"], config
        )

        # Load worker config
        self.log_level = get_config_variable(
            "WORKER_LOG_LEVEL", ["worker", "log_level"], config
        )
        self.log_format = get_config_variable(
            "WORKER_LOG_FORMAT",
            ["worker", "log_format"],
            config,
            default="%(levelname)s:%(name)s:%(message)s",
        )
        self.log_datetime_format = get_config_variable(
            "WORKER_LOG_DATETIME_FORMAT",
            ["worker", "log_datetime_format"],
            config,
            default="%Y-%m-%d %H:%M:%S,%03d",
        )

        # Configure logger
        numeric_level = getattr(logging, self.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {self.log_level}")

        logging.basicConfig(
            level=numeric_level,
            format=self.log_format,
            datefmt=self.log_datetime_format,
        )

        # Check if openCTI is available
        self.api = OpenCTIApiClient(
            url=self.opencti_url,
            token=self.opencti_token,
            log_level=self.log_level,
            ssl_verify=self.opencti_ssl_verify,
            json_logging=self.opencti_json_logging,
        )

        # Initialize variables
        self.connectors: List[Any] = []
        self.queues: List[Any] = []
        self.consumer_threads: Dict[str, Any] = {}
        self.logger_threads: Dict[str, Any] = {}

    # Start the main loop
    def start(self) -> None:
        sleep_delay = 60
        while True:
            if sleep_delay == 0:
                sleep_delay = 1
            else:
                sleep_delay <<= 1
            sleep_delay = min(60, sleep_delay)
            try:
                # Fetch queue configuration from API
                self.connectors = self.api.connector.list()
                self.queues = [conn["config"]["push"] for conn in self.connectors]

                # Check if all queues are consumed
                for connector in self.connectors:
                    queue = connector["config"]["push"]
                    if queue in self.consumer_threads:
                        if not self.consumer_threads[queue].is_alive():
                            logger.info(
                                "Thread for queue %s not alive, creating a new one",
                                queue,
                            )
                            self.consumer_threads[queue] = Consumer(
                                connector,
                                self.opencti_url,
                                self.opencti_token,
                                self.log_level,
                                self.opencti_ssl_verify,
                                self.opencti_json_logging,
                            )
                            sleep_delay = 0
                            self.consumer_threads[queue].start()
                    else:
                        self.consumer_threads[queue] = Consumer(
                            connector,
                            self.opencti_url,
                            self.opencti_token,
                            self.log_level,
                            self.opencti_ssl_verify,
                            self.opencti_json_logging,
                        )
                        sleep_delay = 0
                        self.consumer_threads[queue].start()

                # Check if some threads must be stopped
                for thread in list(self.consumer_threads):
                    if thread not in self.queues:
                        logger.info(
                            "Queue no longer exists, killing thread %s",
                            thread,
                        )

                        try:
                            self.consumer_threads[thread].terminate()
                            self.consumer_threads.pop(thread, None)
                            sleep_delay = 1
                        except Exception as ex:  # pylint: disable=broad-except
                            logger.info(
                                f"Unable to kill the thread for queue %s, "
                                f"an operation is running, keep trying: %s",
                                thread,
                                ex,
                            )
                time.sleep(sleep_delay)
            except KeyboardInterrupt:
                # Graceful stop
                for thread in self.consumer_threads:
                    if thread not in self.queues:
                        self.consumer_threads[thread].terminate()
                sys.exit(0)

            except Exception as ex:  # pylint: disable=broad-except
                logger.exception("Unhandled error in worker loop: %s", ex)
                time.sleep(60)


def main() -> None:
    worker = Worker()
    try:
        worker.start()
    except Exception as ex:  # pylint: disable=broad-except
        logging.exception(f"Unhandled error: %s", ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
