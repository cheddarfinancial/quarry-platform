# Standard Library
import json
import random
import re
import requests
import os

# Third Party
from jinja2 import Template

# Local
import mixingboard


MANDRILL_KEY = mixingboard.getConf("mandrill_key")

GREETINGS = [
    "Hey there",
    "Hello",
    "Hi"
]

TEMPLATE_FILENAME = os.path.join(os.path.dirname(os.path.realpath(__file__)), "templates", "basic_email.html")
EMAIL_TEMPLATE = Template(open(TEMPLATE_FILENAME).read())

def sendEmail(toAddress, toName, subject, body, leadImage=None, title=None, 
              actionLink=None, actionLinkTitle=None, tags=[]):

    url = "https://mandrillapp.com/api/1.0/messages/send.json"
    kwargs = {
        "preview": re.sub('<[^<]+?>', '', body)[:100]+"...",
        "greeting": GREETINGS[random.randint(0, len(GREETINGS)-1)],
        "name": toName,
        "subject": subject,
        "title": title or subject,
        "leadImage": leadImage,
        "body": body,
        "actionLink": actionLink,
        "actionLinkTitle": actionLinkTitle
    }
    html = EMAIL_TEMPLATE.render(**kwargs)
    data = {
        "key": MANDRILL_KEY,
        "message": {
            "html": html,
            "subject": subject,
            "from_email": "hello@quarry.io",
            "from_name": "Quarry Team",
            "to": [
                {
                    "email": toAddress,
                    "name": toName,
                    "type": "to"
                }
            ],
            "headers": {
                "Reply-To": "hello@quarry.io"
            },
            "track_opens": True,
            "track_clicks": True,
            "auto_text": True,
            "inline_css": True,
            "url_strip_qs": True,
            "tags": tags
        }
    }

    res = requests.post(url, data=json.dumps(data))

    return res
