import csv
import time
from configparser import ConfigParser
from google.cloud import pubsub_v1
from acme.fraudcop.transactions import Transaction


def _main():
    conf = ConfigParser()
    conf.read("app.ini")
    conf = conf["transactions_publish_dummy"]
    input_file = conf["input_file"]
    project_id = conf["project"]
    topic_name = conf["topic"]
    null_token = conf["null_token"]
    quota = int(conf["messages_quota"])
    rate = int(conf["messages_per_second"])
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    with open(input_file) as fi:
        rows = csv.DictReader(fi, delimiter=",", quotechar='"')
        for i, row in enumerate(rows):
            if i >= quota:
                break
            t = Transaction.from_dictionary_of_strings(row, null_token=null_token)
            publisher.publish(
                topic_path,
                data=t.to_protobuf().SerializeToString(),  # data must be a bytestring.
            )
            if (i + 1) % 10 == 0:
                print(f"Published {i + 1} messages.")
            time.sleep(float(1 / rate))
    print("Done.")


if __name__ == "__main__":
    _main()
