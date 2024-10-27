import argparse
import tomllib
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from blizzapi import Character, BlizzardAPIURLs, CharacterData

parser = argparse.ArgumentParser(description="Configuration for blizzard API requests")
parser.add_argument("client_file", type=str, help="File containing client ID and secret")

args = vars(parser.parse_args())
with open(args["client_file"], "rb") as client_file:
    client_data = tomllib.load(client_file)

CLIENT_ID = client_data["client"]["id"]
CLIENT_SECRET = client_data["client"]["secret"]

TOKEN_URL = "https://oauth.battle.net/token"

client = BackendApplicationClient(client_id=CLIENT_ID)
oauth = OAuth2Session(client=client, state="blah123")
token = oauth.fetch_token(token_url=TOKEN_URL, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

blizz_api = BlizzardAPIURLs()

testchar = Character("Aptosaurinae", "Draenor")
blizz_urls = BlizzardAPIURLs()
chardata = CharacterData(testchar, blizz_api_urls=blizz_urls, oauth=oauth)
print(chardata.get_specific_raid_data("The War Within", "Nerub-ar Palace", "Heroic"))
