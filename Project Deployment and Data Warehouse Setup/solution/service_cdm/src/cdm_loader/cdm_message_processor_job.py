from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from lib.pg import PgConnect
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        db: PgConnect,
        logger: Logger,
    ) -> None:
        self._logger = logger
        self._consumer = consumer
        self._db = db
        self._repository = CdmRepository(db, logger)
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START CdmMessageProcessor")

        for _ in range(self._batch_size):
            try:
                msg = self._consumer.consume()
                if not msg:
                    continue

                user_id = msg["user_id"]
                products = msg.get("products") or []

                for product in products:
                    qty = product.get("quantity", 1)
                    self._repository.increment_user_product_counter(
                        user_id=user_id,
                        product_id=product["product_id"],
                        product_name=product["product_name"],
                        qty=qty,
                    )
                    self._repository.increment_user_category_counter(
                        user_id=user_id,
                        category_id=product["category_id"],
                        category_name=product["category_name"],
                        qty=qty,
                    )

                self._logger.info(f"{datetime.utcnow()}: Processed message for user_id={user_id}")
                self._logger.info(f"Received message: {msg}")

            except Exception as e:
                self._logger.error(f"{datetime.utcnow()}: Error processing message: {str(e)}", exc_info=True)

        self._logger.info(f"{datetime.utcnow()}: FINISH CdmMessageProcessor")

