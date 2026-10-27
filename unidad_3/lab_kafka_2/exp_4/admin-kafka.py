
import argparse

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import (
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError
)

BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = ["ventas", "sensor"]

NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1


def create_topic(admin):
    """Crea los tópicos ventas y sensor."""

    for topic_name in TOPICS:
        topic = NewTopic(
            name=topic_name,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )

        try:
            admin.create_topics(new_topics=[topic])
            print(
                f"Topico '{topic_name}' creado con "
                f"{NUM_PARTITIONS} particiones."
            )

        except TopicAlreadyExistsError:
            print(f"El topico '{topic_name}' ya existe.")




def delete_topic(admin):
    """Solicita la eliminación de los tópicos configurados."""

    for topic_name in TOPICS:
        try:
            resultado = admin.delete_topics(
                topics=[topic_name],
                timeout_ms=10000
            )

            print(
                f"Solicitud de eliminacion enviada para "
                f"'{topic_name}'. Respuesta: {resultado}"
            )

        except Exception as error:
            print(
                f"Error al eliminar '{topic_name}': {error}"
            )

def main():
    parser = argparse.ArgumentParser(
        description="Administracion de topicos Kafka"
    )

    parser.add_argument(
        "action",
        choices=["create_topic", "delete_topic"],
        help="Accion que se ejecutara"
    )

    args = parser.parse_args()

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="admin-kafka"
    )

    try:
        if args.action == "create_topic":
            create_topic(admin)
        elif args.action == "delete_topic":
            delete_topic(admin)
    finally:
        admin.close()


if __name__ == "__main__":
    main()
