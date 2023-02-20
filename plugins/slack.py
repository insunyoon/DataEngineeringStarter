from airflow.models import Variable

import logging
import requests

def on_success_callback(context):
    """
    https://airflow.apache.org/_modules/airflow/operators/slack_operator.html
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    text = str(context["task_instance"].xcom_pull(key="return_value", task_ids="load"))
    send_message_to_a_slack_channel(text, ":alert:")


# def send_message_to_a_slack_channel(message, emoji, channel, access_token):
def send_message_to_a_slack_channel(message, emoji):
    # url = "https://slack.com/api/chat.postMessage"
    url = "https://hooks.slack.com/services/"+Variable.get("slack_url")
    headers = {
        'content-type': 'application/json',
    }
    data = { "text": message } # id랑 emoji를 뺐는데도 None이 여전히 두 번 
    r = requests.post(url, json=data, headers=headers)
    return r
