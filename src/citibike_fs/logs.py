import logging


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    # Feast logs every registry refresh at INFO.
    logging.getLogger("feast").setLevel(logging.WARNING)
    # py4j logs every Python callback from the Spark JVM at INFO.
    logging.getLogger("py4j").setLevel(logging.WARNING)
