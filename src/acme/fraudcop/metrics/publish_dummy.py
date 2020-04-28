import csv
from configparser import ConfigParser
from google.cloud import pubsub_v1
from acme.fraudcop.metrics import metric_pb2


def _main():
    conf = ConfigParser()
    conf.read("app.ini")
    conf = conf["metrics_publish_dummy"]
    input_file = conf["input_file"]
    project_id = conf["project"]
    topic_name = conf["topic"]
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    with open(input_file) as fi:
        rows = csv.DictReader(fi, delimiter="\t", quotechar='"')
        for row in rows:
            m = metric_pb2.Metric()
            m.metric = row["metric"]
            m.metric_value = float(row["metric_value"])
            m.test_ids.extend(row["test_ids"].split(","))
            m.test_time = int(row["test_time"])
            m.mode = row["mode"]
            m.version = row["version"]
            m.model_name = row["model_name"]
            m.model_uri = row["model_uri"]
            publisher.publish(
                topic_path, data=m.SerializeToString()  # data must be a bytestring.
            )


if __name__ == "__main__":
    _main()
