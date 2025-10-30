# airflow DAG goes here
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time, json
import requests
import boto3
from botocore.exceptions import BotoCoreError, ClientError

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 28),  #in the past so it can run now
    "retries": 1,
}

#initialize dag
dag = DAG(
    dag_id="fetch_sqs",
    default_args=default_args,
    description='Fetch an SQS message, order by order_no, then assemble the secret phrase',
    schedule="@once",
    catchup=False,
)

def run_pipeline():
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/sae3gg"

    # post request; run to refill the message queue with 21 messages
    payload = requests.post(url).json()
    print(payload)
    queue_url = payload["sqs_url"]

    def get_counts():
        """
        Fetch a message from SQS and display it. Easy peasy.
        """
    
        try:
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )
        except (BotoCoreError, ClientError) as e:
            print(f"Warning: get_queue_attributes failed ({type(e).__name__}): {e}")
            time.sleep(5)
            return 0, 0, 0
        attrs = response.get("Attributes", {}) or {}
        available = int(attrs.get("ApproximateNumberOfMessages", 0))
        inflight  = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        delayed   = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
        #printing live if needed
        print(f"available={available}, inflight={inflight}, delayed={delayed}")
        return available, inflight, delayed


    
    sqs = boto3.client("sqs", region_name="us-east-1")


    while True:
        available, inflight, delayed = get_counts()
        if available > 0:
            break
        #if nothing's available but there are delayed messages, wait a bit and re-check
        if delayed > 0 or inflight > 0:
            time.sleep(5)
            continue
        break
        
    #receive, parse, delete
    pairs = {}  # order_no -> word
    drained_confirms = 0

    while True:
        try: 
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,         
                VisibilityTimeout=60,
                MessageAttributeNames=["All"],  #required to get MessageAttributes
                AttributeNames=["All"],
            )
            if not isinstance(resp, dict):
                print("Warning: receive_message returned non-dict; retrying")
                time.sleep(5); continue
            messages = resp.get("Messages", []) or []            

        except (BotoCoreError, ClientError) as e:
            #warning only and continue
            print(f"Warning: receive_message failed ({type(e).__name__}): {e}")
            time.sleep(5)
            continue

        to_delete = []

        for m in messages:
            attrs = m.get("MessageAttributes", {}) or {}
            order_no = (attrs.get("order_no") or {}).get("StringValue")
            word     = (attrs.get("word") or {}).get("StringValue")
            try:
                k = int(order_no) if order_no is not None else None
            except ValueError:
                k = None
            if k is not None and word is not None:
                pairs[k] = word
                #"workflow persistently stores parsed message content"
                try:
                    with open("pairs.json", "w") as f:
                        json.dump({str(kk): vv for kk, vv in pairs.items()}, f)
                except OSError as e:
                    print(f"Warning: failed to write pairs.json: {e}")

            to_delete.append({"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]})

        if to_delete:
            try:
                resp_del = sqs.delete_message_batch(QueueUrl=queue_url, Entries=to_delete)
                failed = resp_del.get("Failed", []) or []
                if failed:
                    retry_entries = [ent for ent in to_delete if any(f.get("Id") == ent["Id"] for f in failed)]
                    if retry_entries:
                        resp_del2 = sqs.delete_message_batch(QueueUrl=queue_url, Entries=retry_entries)
                        failed2 = resp_del2.get("Failed", []) or []
                        if failed2:
                            print(f"WARNING: still failed to delete some messages: {failed2}")
            except (BotoCoreError, ClientError) as e:
                print(f"Delete batch error: {e}")

        #stop conditions (no prints)
        available, inflight, delayed = get_counts()
        all_zero = (available == 0 and inflight == 0 and delayed == 0)
        if all_zero:
            drained_confirms += 1
        else:
            drained_confirms = 0
        if drained_confirms >= 2:
            break
        if not messages and (delayed > 0 or inflight > 0):
            time.sleep(5)

    #assemble phrase and print
    results = [(k, pairs[k]) for k in sorted(pairs)]
    phrase = " ".join(word for _, word in results)
    print(f"Collected {len(results)} messages.")
    print("Phrase:", phrase)


    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    try:
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody=f"Solution phrase (airflow): {phrase}",
            MessageAttributes={
                "uvaid":   {"DataType": "String", "StringValue": "sae3gg"},
                "phrase":  {"DataType": "String", "StringValue": phrase},
                "platform":{"DataType": "String", "StringValue": "airflow"},
            },
        )
        status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        msg_id = response.get("MessageId")
        if status_code == 200:
            print(f"Successfully submitted phrase! (HTTP {status_code}, MessageId: {msg_id})")
        else:
            print(f"Submission response: {status_code}, raw response: {response}")
    except (BotoCoreError, ClientError) as e:
        print(f"Warning: failed to submit solution: {e}")

task_run_pipeline = PythonOperator(
    task_id="run_pipeline",
    python_callable=run_pipeline,
    dag=dag,
)