# prefect flow goes here
import requests
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
import time
import json, os

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/sae3gg"

# post request; run to refill the message queue with 21 messages
payload = requests.post(url, timeout=20).json()
print(payload)
queue_url = payload["sqs_url"]
#queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/sae3gg"

def get_counts(sqs, queue_url):
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
    attrs = response.get("Attributes", {})
    available = int(attrs.get("ApproximateNumberOfMessages", 0))
    inflight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))

    #printing live if needed
    print(f"available={available}, inflight={inflight}, delayed={delayed}")
    return available, inflight, delayed


try:
    sqs = boto3.client("sqs", region_name="us-east-1")


    while True:
        available, inflight, delayed = get_counts(sqs, queue_url)
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
                    #retry once for failed Ids
                    retry_entries = [ent for ent in to_delete if any(f.get("Id") == ent["Id"] for f in failed)]
                    if retry_entries:
                        
                        resp_del2 = sqs.delete_message_batch(QueueUrl=queue_url, Entries=retry_entries)
                        failed2 = resp_del2.get("Failed", []) or []
                        if failed2:
                            print(f"WARNING: still failed to delete some messages: {failed2}")
            except (BotoCoreError, ClientError) as e:
                print(f"Delete batch error: {e}") 

        #stop conditions (no prints)
        available, inflight, delayed = get_counts(sqs, queue_url)
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
except Exception as e:
    print("An error occurred:", str(e))


submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

message = "Solution phrase with prefect flow"
def send_solution(uvaid, phrase, platform):
    try:
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody=message,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        print(f"Response: {response}")
    except Exception as e:
        print(f"Error sending solution: {e}")

#sending solution
send_solution("sae3gg", phrase, "prefect")