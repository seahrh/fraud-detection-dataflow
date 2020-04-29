import csv
import time
from configparser import ConfigParser
from google.cloud import pubsub_v1
from acme.fraudcop.transactions import transaction_pb2


def _main():
    conf = ConfigParser()
    conf.read("app.ini")
    conf = conf["transactions_publish_dummy"]
    input_file = conf["input_file"]
    project_id = conf["project"]
    topic_name = conf["topic"]
    quota = int(conf["messages_quota"])
    rate = int(conf["messages_per_second"])
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    with open(input_file) as fi:
        rows = csv.DictReader(fi, delimiter="\t", quotechar='"')
        for i, row in enumerate(rows):
            if i >= quota:
                break
            m = transaction_pb2.Transaction()
            m.trans_id = int(row["trans_id"])
            m.account_id = int(row["account_id"])
            m.date = int(row["date"])
            m.type = row["type"]
            m.operation = row["operation"]
            m.amount = row["amount"]
            m.balance = row["balance"]
            m.k_symbol = row["k_symbol"]
            m.bank = row["bank"]
            m.account = int(row["account"])
            m.district_id = int(row["district_id"])
            m.acct_freq = row["acct_freq"]
            m.a2 = row["a2"]
            m.a3 = row["a3"]
            m.a4 = int(row["a4"])
            m.a5 = int(row["a5"])
            m.a6 = int(row["a6"])
            m.a7 = int(row["a7"])
            m.a8 = int(row["a8"])
            m.a9 = int(row["a9"])
            m.a10 = row["a10"]
            m.a11 = int(row["a11"])
            m.a12 = row["a12"]
            m.a13 = row["a13"]
            m.a14 = int(row["a14"])
            m.a15 = int(row["a15"])
            m.a16 = int(row["a16"])
            m.disp_type = row["disp_type"]
            m.card_id = int(row["card_id"])
            m.card_type = row["card_type"]
            m.card_issued = row["card_issued"]
            m.loan_date = int(row["loan_date"])
            m.loan_amount = int(row["loan_amount"])
            m.loan_duration = int(row["loan_duration"])
            m.loan_payments = row["loan_payments"]
            m.loan_status = row["loan_status"]
            publisher.publish(
                topic_path, data=m.SerializeToString()  # data must be a bytestring.
            )
            if (i + 1) % 10 == 0:
                print(f"Published {i + 1} messages.")
            time.sleep(float(1 / rate))
    print("Done.")


if __name__ == "__main__":
    _main()
