# nop-raid-bot

Initial functionality is working towards something that can use a character name and realm to request current raid status and enchants from the WoW API. Long term hypothetical is a replacement for the raid helper system.

## Aim

First goal is to build something that can ingest a set of name-realm strings and output whether anyone is:
- saved to the given raid difficulty in this weekly reset
- missing enchants or gems

Long term hypothetical goal is to potentially build a fully featured raid helper equivalent but tailored to the No Pressure discord needs, if it feels like the additional functionality would be helpful. 
Advantages would be:
- Tailoring to raid leads needs without a third party being involved
- Automatically being able to check some of the above alongside other checks that raid helper doesn't support e.g. checking a charactername/realm is valid, and automatically pulling class from the API on signup

## Requirements / dev info

Python requirements are provided in `requirements.txt`. 

At present the functionality is limited to python-based queries of these elements. This will be developed into a `discord.py` bot that can be used to query this information. The `ipynb` file is a bit of a playground currently to allow testing of the core `.py` modules and explore the `json` responses from the API to check formats.

## Additional information

A `clientdetails.toml` file containing a set of Blizzard API keys is required to be able to run the system. This is a plain text file in the following format (populate the two fields with your own information provided from the Blizzard developer API):
``` yaml
[client]
id="clientid"
secret="clientsecret"
```
