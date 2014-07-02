# Standard Library
import requests

# Third Party

# Local

AUTH_USER = "ACc4dbf808fb5b795feb7ed731070bd943"
AUTH_SECRET = "d99c522b9bea791c9b72f02ddee589c5"

def sendMessage(number, message):

    res = requests.post("https://api.twilio.com/2010-04-01/Accounts/ACc4dbf808fb5b795feb7ed731070bd943/Messages", data = {
        "From": "+16502048640",
        "To": number,
        "Body": message
    }, auth=(AUTH_USER, AUTH_SECRET))

    if res.status_code == 201:
        return True
    else:
        return False
