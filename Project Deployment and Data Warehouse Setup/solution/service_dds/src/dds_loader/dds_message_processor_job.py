from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect
from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        db: PgConnect,
        logger: Logger,
    ) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._db = db
        self._repository = DdsRepository(db)
        self._batch_size = 100
        self._load_src = "dds_message_processor"

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START DdsMessageProcessor")

        for _ in range(self._batch_size):
            try:
                msg = self._consumer.consume()
                if not msg:
                    continue

                payload = msg.get("payload")
                if not payload:
                    self._logger.warning("Received message without payload")
                    continue

                load_dt = datetime.utcnow()

                # 1. Hubs
                h_user_pk = self._repository.get_or_create_h_user(
                    payload["user"]["id"], load_dt, self._load_src
                )
                h_restaurant_pk = self._repository.get_or_create_h_restaurant(
                    payload["restaurant"]["id"], load_dt, self._load_src
                )
                h_order_pk = self._repository.get_or_create_h_order(
                    payload["id"], payload["date"], load_dt, self._load_src
                )

                product_mappings = []
                for product in payload["products"]:
                    h_product_pk = self._repository.get_or_create_h_product(
                        product["id"], load_dt, self._load_src
                    )
                    h_category_pk = self._repository.get_or_create_h_category(
                        product["category"], load_dt, self._load_src
                    )

                    # 2. Links
                    self._repository.insert_l_order_product(h_order_pk, h_product_pk, load_dt, self._load_src)
                    self._repository.insert_l_product_category(h_product_pk, h_category_pk, load_dt, self._load_src)
                    self._repository.insert_l_product_restaurant(h_product_pk, h_restaurant_pk, load_dt, self._load_src)

                    product_mappings.append({
                        "product_id": h_product_pk,
                        "product_name": product["name"],
                        "category_id": h_category_pk,
                        "category_name": product["category"],
                    })

                self._repository.insert_l_order_user(h_order_pk, h_user_pk, load_dt, self._load_src)

                # 3. Satellites
                self._repository.insert_s_order_cost(
                    h_order_pk, payload["cost"], payload["payment"], load_dt, self._load_src
                )
                self._repository.insert_s_order_status(h_order_pk, payload["status"], load_dt, self._load_src)
                self._repository.insert_s_user_names(
                    h_user_pk, payload["user"]["name"], payload["user"]["id"], load_dt, self._load_src
                )
                self._repository.insert_s_restaurant_names(
                    h_restaurant_pk, payload["restaurant"]["name"], load_dt, self._load_src
                )

                for p in product_mappings:
                    self._repository.insert_s_product_names(p["product_id"], p["product_name"], load_dt, self._load_src)

                # 4. Produce message for CDM
                for p in product_mappings:
                    output_msg = {
                        "user_id": str(h_user_pk),
                        "product_id": str(p["product_id"]),
                        "category_id": str(p["category_id"]),
                        "product_name": p["product_name"],
                        "category_name": p["category_name"],
                    }
                    self._producer.produce(output_msg)

                self._logger.info(f"{datetime.utcnow()}: Processed order_id {payload['id']}")

            except Exception as e:
                self._logger.error(f"{datetime.utcnow()}: Error processing message: {e}", exc_info=True)

        self._logger.info(f"{datetime.utcnow()}: FINISH DdsMessageProcessor")