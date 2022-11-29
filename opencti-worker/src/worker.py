"""OpenCTI Worker"""

from __future__ import annotations

import base64
import datetime
import functools
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from threading import Thread
from types import FrameType
from typing import Any, Dict, List, Union

import pika
import yaml
from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic
from prometheus_client import start_http_server
from pycti import OpenCTIApiClient
from pycti.connector.opencti_connector_helper import (
    create_ssl_context,
    get_config_variable,
)
from requests.exceptions import RequestException, Timeout

log = logging.getLogger("worker")

PROCESSING_COUNT = 4

# Telemetry variables definition
meter = metrics.get_meter(__name__)
resource = Resource(attributes={SERVICE_NAME: "opencti-worker"})
bundles_global_counter = meter.create_counter(
    name="opencti_bundles_global_counter",
    description="number of bundles processed",
)
bundles_success_counter = meter.create_counter(
    name="opencti_bundles_success_counter",
    description="number of bundles successfully processed",
)
bundles_timeout_error_counter = meter.create_counter(
    name="opencti_bundles_timeout_error_counter",
    description="number of bundles in timeout error",
)
bundles_request_error_counter = meter.create_counter(
    name="opencti_bundles_request_error_counter",
    description="number of bundles in request error",
)
bundles_technical_error_counter = meter.create_counter(
    name="opencti_bundles_technical_error_counter",
    description="number of bundles in technical error",
)
bundles_lock_error_counter = meter.create_counter(
    name="opencti_bundles_lock_error_counter",
    description="number of bundles in lock error",
)
bundles_missing_reference_error_counter = meter.create_counter(
    name="opencti_bundles_missing_reference_error_counter",
    description="number of bundles in missing reference error",
)
bundles_bad_gateway_error_counter = meter.create_counter(
    name="opencti_bundles_bad_gateway_error_counter",
    description="number of bundles in bad gateway error",
)
bundles_processing_time_gauge = meter.create_histogram(
    name="opencti_bundles_processing_time_gauge",
    description="number of bundles in bad gateway error",
)


class Consumer(Thread):
    """RabbitMQ message consumer"""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        connector: Dict[str, Any],
        opencti_url: str,
        opencti_token: str,
        log_level: str,
        ssl_verify: Union[bool, str] = False,
        json_logging: bool = False,
    ) -> None:
        super().__init__()

        self._api = OpenCTIApiClient(
            url=opencti_url,
            token=opencti_token,
            log_level=log_level,
            ssl_verify=ssl_verify,
            json_logging=json_logging,
        )
        self._queue_name = connector["config"]["push"]

        pika_parameters = pika.ConnectionParameters(
            host=connector["config"]["connection"]["host"],
            port=connector["config"]["connection"]["port"],
            virtual_host=connector["config"]["connection"]["vhost"],
            credentials=pika.PlainCredentials(
                connector["config"]["connection"]["user"],
                connector["config"]["connection"]["pass"],
            ),
            ssl_options=(
                pika.SSLOptions(create_ssl_context())
                if connector["config"]["connection"]["use_ssl"]
                else None
            ),
        )

        self._connection = BlockingConnection(pika_parameters)
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._processing_count = 0

    def run(self) -> None:
        """Run the thread"""

        consumer_tag = None

        try:
            # Consume the queue
            log.info("Thread for queue %s started", self._queue_name)

            consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=self._process_message,
            )
            self._channel.start_consuming()
        finally:
            if consumer_tag is not None:
                self._channel.basic_cancel(consumer_tag)
            if self._channel.is_open:
                self._channel.close()
            if self._connection.is_open:
                self._connection.close()
            log.info("Thread for queue %s terminated", self._queue_name)

    def stop(self) -> None:
        """Gracefully shutdown the consumer"""

        def callback():
            """A threadsafe callback to stop consuming"""
            self._channel.stop_consuming()

        self._connection.add_callback_threadsafe(callback)

    def _process_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        _properties: BasicProperties,
        body: bytes,
    ) -> None:
        """Callable for consuming a message"""

        data = json.loads(body)
        log.info(
            "Processing a new message (delivery_tag=%d)",
            method.delivery_tag,
        )

        self._data_handler(channel, method.delivery_tag, data)

        log.debug("Message processed")

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-branches
    def _data_handler(
        self,
        channel: BlockingChannel,
        delivery_tag: int,
        data: Dict[str, Any],
    ) -> None:
        """Handle the data from a message"""

        start_processing = datetime.datetime.now()

        # Set the API headers
        applicant_id = data["applicant_id"]
        self._api.set_applicant_id_header(applicant_id)
        work_id = data.get("work_id", None)

        # Execute the import
        content = "Unparseable"
        self._processing_count += 1
        has_retries = self._processing_count < PROCESSING_COUNT
        processing_count = self._processing_count if has_retries else None

        try:
            types = data.get("entities_types") or None
            event_type = data.get("type", "bundle")

            if event_type == "bundle":
                content = base64.b64decode(data["content"]).decode("utf-8")
                update = data.get("update", False)
                self._api.stix2.import_bundle_from_json(
                    json_data=content,
                    update=update,
                    types=types,
                    retry_number=processing_count,
                )

                # Ack the message
                cb = functools.partial(self._ack_message, channel, delivery_tag)
                channel.connection.add_callback_threadsafe(cb)

                work_id = data.get("work_id")
                if work_id is not None:
                    self._api.work.report_expectation(work_id, None)

                bundles_success_counter.add(1)
                self._processing_count = 0

            elif event_type == "event":
                content = base64.b64decode(data["content"]).decode("utf-8")
                event_content = json.loads(content)
                event_type = event_content["type"]

                if event_type in ("create", "update"):
                    bundle = {
                        "type": "bundle",
                        "objects": [event_content["data"]],
                    }
                    self._api.stix2.import_bundle(
                        stix_bundle=bundle,
                        update=True,
                        types=types,
                        retry_number=processing_count,
                    )

                elif event_type == "delete":
                    delete_id = event_content["data"]["id"]
                    self._api.stix.delete(id=delete_id)

                elif event_type == "merge":
                    # Start with a merge
                    target_id = event_content["data"]["id"]
                    source_ids = [
                        source["id"] for source in event_content["context"]["sources"]
                    ]
                    self._api.stix_core_object.merge(
                        id=target_id,
                        object_ids=source_ids,
                    )

                    # Update the target entity after merge
                    bundle = {
                        "type": "bundle",
                        "objects": [event_content["data"]],
                    }
                    self._api.stix2.import_bundle(
                        stix_bundle=bundle,
                        update=True,
                        types=types,
                        retry_number=processing_count,
                    )

                # Ack the message
                cb = functools.partial(self._ack_message, channel, delivery_tag)
                channel.connection.add_callback_threadsafe(cb)
                self._processing_count = 0
                bundles_success_counter.add(1)

            else:
                log.warning("Unknown event type: { %s }", event_type)

        except Timeout as ex:
            # Platform is under heavy load: wait for unlock & retry almost indefinitely.
            log.warning("A connection timeout occurred: { %s }", ex)
            bundles_timeout_error_counter.add(1)
            sleep_jitter = round(random.uniform(10, 30), 2)
            time.sleep(sleep_jitter)
            self._data_handler(channel, delivery_tag, data)

        except RequestException as ex:
            bundles_request_error_counter.add(1, {"origin": "opencti-worker"})
            log.error("A connection error occurred: { %s }", ex)
            time.sleep(60)
            cb = functools.partial(self._nack_message, channel, delivery_tag)
            channel.connection.add_callback_threadsafe(cb)
            self._processing_count = 0

        except Exception as ex:  # pylint: disable=broad-except
            error = str(ex)
            if "LockError" in error and has_retries:
                bundles_lock_error_counter.add(1)
                # Platform is under heavy load:
                # wait for unlock & retry almost indefinitely.
                sleep_jitter = round(random.uniform(10, 30), 2)
                time.sleep(sleep_jitter)
                self._data_handler(channel, delivery_tag, data)

            elif "MissingReferenceError" in error and has_retries:
                bundles_missing_reference_error_counter.add(1)
                # In case of missing reference, wait & retry
                sleep_jitter = round(random.uniform(1, 3), 2)
                time.sleep(sleep_jitter)
                log.info(
                    "Message (delivery_tag=%d) reprocess (retry nb: %d)",
                    delivery_tag,
                    self._processing_count,
                )
                self._data_handler(channel, delivery_tag, data)

            elif "Bad Gateway" in error:
                bundles_bad_gateway_error_counter.add(1)
                log.error("A connection error occurred: { %s }", error)
                time.sleep(60)

                cb = functools.partial(self._nack_message, channel, delivery_tag)
                channel.connection.add_callback_threadsafe(cb)

                self._processing_count = 0

            else:
                # Platform does not know what to do and raises an error
                # fail and acknowledge the message.
                bundles_technical_error_counter.add(1)

                # if len(content) > 50000:
                #    source = "Bundle too large"
                # else:
                source = content

                log.exception(f"Unexpected error from: {source}")
                self._processing_count = 0

                cb = functools.partial(self._ack_message, channel, delivery_tag)
                channel.connection.add_callback_threadsafe(cb)

                if work_id is not None:
                    self._api.work.report_expectation(
                        work_id, {"error": error, "source": source}
                    )

        finally:
            bundles_global_counter.add(1)
            processing_delta = datetime.datetime.now() - start_processing
            bundles_processing_time_gauge.record(processing_delta.seconds)

    def _ack_message(self, channel: BlockingChannel, delivery_tag: int) -> None:
        """ACK a message"""

        if channel.is_open:
            log.debug(
                "Message (delivery_tag=%d) acknowledged",
                delivery_tag,
            )
            channel.basic_ack(delivery_tag)
        else:
            log.warning(
                "Message (delivery_tag=%d) NOT acknowledged (channel closed)",
                delivery_tag,
            )

    def _nack_message(self, channel: BlockingChannel, delivery_tag: int) -> None:
        """NACK a message"""

        if channel.is_open:
            log.debug(
                "Message (delivery_tag=%d) rejected",
                delivery_tag,
            )
            channel.basic_nack(delivery_tag)
        else:
            log.warning(
                "Message (delivery_tag=%d) NOT rejected (channel closed)",
                delivery_tag,
            )


class Worker:  # pylint: disable=too-few-public-methods
    """Consumer controller"""

    def __init__(self) -> None:
        # Get configuration
        config_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.yml"
        )
        if os.path.isfile(config_file_path):
            with open(config_file_path, "r", encoding="utf-8") as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
        else:
            config = {}

        # Load API config
        self._opencti_url = get_config_variable(
            "OPENCTI_URL", ["opencti", "url"], config
        )
        self._opencti_token = get_config_variable(
            "OPENCTI_TOKEN", ["opencti", "token"], config
        )
        self._opencti_ssl_verify = get_config_variable(
            "OPENCTI_SSL_VERIFY", ["opencti", "ssl_verify"], config, False, False
        )
        self._opencti_json_logging = get_config_variable(
            "OPENCTI_JSON_LOGGING", ["opencti", "json_logging"], config
        )
        # Load worker config
        self._log_level = get_config_variable(
            "WORKER_LOG_LEVEL", ["worker", "log_level"], config
        )

        # Telemetry
        telemetry_enabled = get_config_variable(
            "WORKER_TELEMETRY_ENABLED",
            ["worker", "telemetry_enabled"],
            config,
            False,
            False,
        )
        telemetry_prometheus_port = get_config_variable(
            "WORKER_PROMETHEUS_TELEMETRY_PORT",
            ["worker", "telemetry_prometheus_port"],
            config,
            False,
            14270,
        )
        telemetry_prometheus_host = get_config_variable(
            "WORKER_PROMETHEUS_TELEMETRY_HOST",
            ["worker", "telemetry_prometheus_host"],
            config,
            False,
            "0.0.0.0",
        )

        # Telemetry
        if telemetry_enabled:
            start_http_server(
                port=telemetry_prometheus_port,
                addr=telemetry_prometheus_host,
            )
            provider = MeterProvider(
                resource=resource,
                metric_readers=[PrometheusMetricReader()],
            )
            metrics.set_meter_provider(provider)

        # Check if openCTI is available
        self._api = OpenCTIApiClient(
            url=self._opencti_url,
            token=self._opencti_token,
            log_level=self._log_level,
            ssl_verify=self._opencti_ssl_verify,
            json_logging=self._opencti_json_logging,
        )

        # Configure logger
        numeric_level = getattr(logging, self._log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {self._log_level}")
        logging.basicConfig(level=numeric_level)

        self._consumers = {}  # type: Dict[str, Consumer]
        self._shutdown = threading.Event()

        signal.signal(signal.SIGINT, self._handle_signal)  # ctrl-c
        signal.signal(signal.SIGTERM, self._handle_signal)  # docker stop

    def start(self) -> None:
        """Start the main loop"""

        while not self._shutdown.is_set():
            try:
                # Fetch queue configuration from API
                connectors = self._api.connector.list()
                conn_ids: List[str] = [conn["config"]["push"] for conn in connectors]

                # Check if all queues are consumed
                for connector in connectors:
                    conn_id = connector["config"]["push"]
                    if conn_id in self._consumers:
                        if not self._consumers[conn_id].is_alive():
                            log.info(
                                "Thread for queue %s not alive, creating a new one...",
                                conn_id,
                            )
                            self._consumers[conn_id] = Consumer(
                                connector,
                                self._opencti_url,
                                self._opencti_token,
                                self._log_level,
                                self._opencti_ssl_verify,
                                self._opencti_json_logging,
                            )
                            self._consumers[conn_id].start()
                    else:
                        self._consumers[conn_id] = Consumer(
                            connector,
                            self._opencti_url,
                            self._opencti_token,
                            self._log_level,
                            self._opencti_ssl_verify,
                            self._opencti_json_logging,
                        )
                        self._consumers[conn_id].start()

                # Check if some threads must be stopped
                for conn_id in list(self._consumers):
                    if conn_id not in conn_ids:
                        log.info("Queue %s no longer exists, stopping thread", conn_id)
                        thread = self._consumers.pop(conn_id)
                        thread.stop()
                        log.info("Waiting on thread for queue %s to finish", conn_id)
                        thread.join()
                        log.info("Thread for queue %s has terminated", conn_id)

            except Exception:  # pylint: disable=broad-except
                log.exception("Unexpected error")

            self._shutdown.wait(timeout=60)

    def _handle_signal(self, signum: int, _frame: FrameType) -> None:
        sig_name = signal.Signals(signum).name

        log.info("Stopping main loop")
        self._shutdown.set()

        log.info(f"Stopping all consumer threads ({sig_name})")
        for conn_id, thread in self._consumers.items():
            log.info("Stopping thread for queue %s", conn_id)
            thread.stop()
            log.info("Waiting on thread for queue %s to finish", conn_id)
            thread.join()
            log.info("Thread for queue %s has terminated", conn_id)


if __name__ == "__main__":
    worker = Worker()

    try:
        worker.start()
    except Exception:  # pylint: disable=broad-except
        log.exception("Unhandled exception")
        sys.exit(1)
