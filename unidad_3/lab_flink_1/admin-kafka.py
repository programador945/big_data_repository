
import argparse

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import (
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError
)


# --------------------------------------------------
# 1. Configuración
# --------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:9094"

TOPICS = [
    "mediciones",
    "alertas"
]


# --------------------------------------------------
# 2. Crear tópicos
# --------------------------------------------------

def create_topics(admin):
    """Crea los tópicos mediciones y alertas."""

    topics = [
        NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1
        )
        for topic_name in TOPICS
    ]

    try:

        admin.create_topics(
            new_topics=topics,
            validate_only=False
        )

        for topic_name in TOPICS:
            print(
                f"Topico '{topic_name}' "
                "creado con una particion."
            )

    except TopicAlreadyExistsError:

        print(
            "Uno o varios topicos ya existen. "
            "Verificando individualmente..."
        )

        for topic_name in TOPICS:

            topic = NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )

            try:

                admin.create_topics(
                    new_topics=[topic]
                )

                print(
                    f"Topico '{topic_name}' creado."
                )

            except TopicAlreadyExistsError:

                print(
                    f"El topico '{topic_name}' "
                    "ya existe."
                )


# --------------------------------------------------
# 3. Eliminar tópicos
# --------------------------------------------------

def delete_topics(admin):
    """Elimina los tópicos mediciones y alertas."""

    for topic_name in TOPICS:

        try:

            admin.delete_topics(
                topics=[topic_name],
                timeout_ms=10000
            )

            print(
                f"Solicitud de eliminacion enviada "
                f"para '{topic_name}'."
            )

        except UnknownTopicOrPartitionError:

            print(
                f"El topico '{topic_name}' "
                "no existe."
            )


# --------------------------------------------------
# 4. Programa principal
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Administracion de topicos Kafka"
    )

    parser.add_argument(
        "action",
        choices=[
            "create_topic",
            "delete_topic"
        ],
        help="Accion que se ejecutara"
    )

    args = parser.parse_args()

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="admin-kafka"
    )

    try:

        if args.action == "create_topic":

            create_topics(admin)

        elif args.action == "delete_topic":

            delete_topics(admin)

    finally:

        admin.close()


# --------------------------------------------------
# 5. Ejecutar
# --------------------------------------------------

if __name__ == "__main__":
    main()
