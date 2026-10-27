
import argparse

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ventas"


def create_topic(admin):
    """Crea el tópico ventas con una partición."""
    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=2,
        replication_factor=1
    )

    try:
        admin.create_topics(new_topics=[topic])
        print(f"Topico '{TOPIC_NAME}' creado con una particion.")

    except TopicAlreadyExistsError:
        print(f"El topico '{TOPIC_NAME}' ya existe.")


def delete_topic(admin):
    """Elimina el tópico ventas."""
    try:
        admin.delete_topics(
            topics=[TOPIC_NAME],
            timeout_ms=10000
        )
        print(f"Solicitud de eliminacion enviada para '{TOPIC_NAME}'.")

    except UnknownTopicOrPartitionError:
        print(f"El topico '{TOPIC_NAME}' no existe.")


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
