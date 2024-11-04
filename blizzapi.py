"""Contains shortcuts for retrieving data from the blizzard API

It is expected that you create an oauth class and retrieve a token using `requests_oauthlib` first
to be able to request data
e.g.:
``` python
# set up oauth and get token
client = BackendApplicationClient(client_id=CLIENT_ID)
oauth = OAuth2Session(client=client, state="blah123")
token = oauth.fetch_token(token_url=TOKEN_URL, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

# retrieve character-specific data
testchar = Character("Aptosaurinae", "Draenor")
blizz_urls = BlizzardAPIURLs()
chardata = CharacterData(testchar, blizz_api_urls=blizz_urls, oauth=oauth)
aptosaurinae_nerubar_data = chardata.get_specific_raid_data("The War Within", "Nerub-ar Palace", "Heroic")
aptosaurinae_nerubar_data
>>> "Nerub-ar Palace [Heroic]:\n- Progress: 3 / 8\n- Sikran, Captain of the Sureki: <t:1729536136:D>\n- Rasha'nan: <t:1729538172:D>\n- Broodtwister Ovi'nax: <t:1729542901:D>"
```
"""

from __future__ import annotations
import time
import warnings
from dataclasses import dataclass
from requests_oauthlib import OAuth2Session
import polars as pl


REGION = "eu"
LANG = "en_GB"

COL_CHAR = "CharacterName"
COL_REALM = "RealmName"

class BlizzardAPIURLs:
    """Provides shortcuts to Blizz API URLs
    """
    def __init__(
            self,
            region: str = REGION,
            locale:str = LANG
    ):
        self.hostname = f"https://{region}.api.blizzard.com"
        self.profile_user = "/profile/user/wow"
        self.profile_char = "/profile/wow/character"
        self.profile_guild = "/profile/wow/guild"
        self.journal_instance = "/data/wow/journal-instance"
        self.locale = f"locale={locale}"
        self.namespace_profile = "namespace=profile-eu"
        self.namespace_static = "namespace=static-eu"
        self.urlend_profile = f"?{self.locale}&{self.namespace_profile}"
        self.urlend_static = f"?{self.locale}&{self.namespace_static}"

    # --- base request
    def _char(self, char: Character):
        return f"{self.hostname}{self.profile_char}/{char.realm.lower()}/{char.name.lower()}"

    def _journal(self):
        return f"{self.hostname}{self.journal_instance}"

    # --- equipment
    def get_equipment(self, char: Character):
        return f"{self._char(char)}/equipment{self.urlend_profile}"

    # --- Encounters
    def _encounters(self, char: Character, encounter_type: str):
        return f"{self._char(char)}/encounters/{encounter_type}{self.urlend_profile}"

    def get_raids(self, char: Character):
        return self._encounters(char, "raids")

    # --- Journal
    def get_encounter_journal_index(self):
        return f"{self._journal()}/index{self.urlend_static}"

    def get_encounter_list(self, id: int):
        return f"{self._journal()}/{id}{self.urlend_static}"

@dataclass
class Character:
    name: str
    realm: str

    def __str__(self):
        return f"{self.name.capitalize()}-{self.realm.capitalize()}"

class BatchData:
    """Gathers multiple characters data
    """
    def __init__(
            self,
            oauth: OAuth2Session,
            blizz_api_urls: BlizzardAPIURLs = None,
    ):
        if blizz_api_urls is None:
            blizz_api_urls = BlizzardAPIURLs()
        self.urls = blizz_api_urls
        self.oauth = oauth
        self.chars = []

    def add_chars(self, chars: list[Character]):
        for char in chars:
            self.add_char(char)

    def add_char(self, char: Character):
        if char not in self.chars:
            self.chars.append(char)

class CharacterData:
    """Manages data gathering for a character
    """
    def __init__(
            self,
            char: Character,
            oauth: OAuth2Session,
            blizz_api_urls: BlizzardAPIURLs = None,
    ):
        if blizz_api_urls is None:
            blizz_api_urls = BlizzardAPIURLs()
        self.char: Character = char
        self.urls = blizz_api_urls
        self.oauth = oauth
        self.df_raid = self._blank_df()
        self.df_enchants = self._blank_df()
        # TODO check that char exists and return warning if not?

    def __str__(
            self
    ):
        return f"{self.char}"

    # --- Retrieve jsons

    def _get_raid_json(
        self
    ) -> str:
        """Retrieves a relevant json containing raid data for the character
        """
        url = self.urls.get_raids(self.char)
        return self.oauth.get(url).json()

    def _get_equipment_json(
        self
    ) -> str:
        """Retrieves a relevant json containing equipment data for the character
        """
        url = self.urls.get_equipment(self.char)
        return self.oauth.get(url).json()

    # --- Utilities

    def _blank_df(self) -> pl.DataFrame:
        """Creates a "blank" df, including char name and realm
        """
        return pl.DataFrame({COL_CHAR: [self.char.name], COL_REALM: [self.char.realm]})

    def _format_unix_time(
            self,
            unix_time: int,
            discord_format: bool = True,
    ) -> str:
        """Converts unix times into either a discord time string or human readable

        Args:
            unix_time: Time in unix format (seconds since epoch start)
            discord_format: Whether to return the strings in discord time format
        """
        if discord_format:
            return f"<t:{unix_time}:D>"
        else:
            return time.ctime(unix_time)

    # --- Raid progress

    def _populate_raid(
            self,
            progress: dict[str: str],
    ) -> pl.DataFrame:
        """Converts a progress dict into a df with char name/realm included

        Args:
            progress: Dictionary of boss name to unix time int

        Returns:
            Polars dataframe with summary of raid progress
        """
        df = self._blank_df()
        return df.with_columns(**{key: pl.lit(value) for key, value in progress.items()})

    def _get_specific_raid_data(
            self,
            expansion_name: str,
            raid_name: str,
            difficulty: str,
    ) -> pl.DataFrame:
        """Gets raid progress for a given raid & difficulty

        Args:
            expansion_name: Name of the expansion
            raid_name: Name of the relevant raid
            difficulty: Difficulty level of the given raid

        Returns:
            df with a summary of progress
        """
        response = self._get_raid_json()
        expansions_list = [
            data["expansion"]["name"]
            for data
            in response["expansions"]
        ]
        if expansion_name in expansions_list:
            exp_idx = expansions_list.index(expansion_name)
            instances_list = [
                data["instance"]["name"]
                for data
                in response["expansions"][exp_idx]["instances"]
            ]
            if raid_name in instances_list:
                instance_idx = instances_list.index(raid_name)
                difficulties_list = [
                    data["difficulty"]["name"]
                    for data
                    in response["expansions"][exp_idx]["instances"][instance_idx]["modes"]
                ]
                if difficulty in difficulties_list:
                    difficulty_index = difficulties_list.index(difficulty)
                    char_raid_data = response["expansions"][exp_idx]["instances"][instance_idx]["modes"][difficulty_index]["progress"]
                    return self._populate_raid(
                        progress={
                            f"{difficulty}_{encounter["encounter"]["name"]}":
                            int(str(encounter["last_kill_timestamp"])[:-3])
                            for encounter in char_raid_data["encounters"]
                        }
                    )
                else:
                    warnings.warn(f"No data found for {raid_name} [{difficulty}]")
                    return self._blank_df()

    def _raid_progress_report(
            self,
            progress_df: pl.DataFrame,
            discord_format = True,
    ) -> str:
        """Reformats the progress for the raid into a nice output string

        Args:
            progress_df: Dataframe with a summary of progress
            discord_format: Whether to format times as strings or discord strings

        Returns:
            string of progression, nicely formatted
        """
        progress_str = None
        for col in progress_df.columns:
            if col not in [COL_CHAR, COL_REALM]:
                unix_time = progress_df.select(col)[0,0]
                time_str = self._format_unix_time(unix_time, discord_format)
                if progress_str is None:
                    progress_str = f"- {col}: {time_str}"
                else:
                    progress_str = f"{progress_str}\n- {col}: {time_str}"
        return progress_str

    def get_specific_raid_data(
            self,
            expansion_name: str,
            raid_name: str,
            difficulty: str,
    ):
        """Gets a summary of raid progress as a nice output string

        Args:
            expansion_name: Name of the expansion
            raid_name: Name of the relevant raid
            difficulty: Difficulty level of the given raid

        Returns:
            string of progression, nicely formatted
        """
        df = self._get_specific_raid_data(
            expansion_name=expansion_name,
            raid_name=raid_name,
            difficulty=difficulty
        )
        if df is not None:
            return f"{self.char}:\n{self._raid_progress_report(df)}"
        else:
            return f"{self.char}: No progress found"

    # --- Equipment

    def get_current_enchants(
            self,
            verbose = False
    ):
        response = self._get_equipment_json()
        enchant_slots = [
            "Back",
            "Chest",
            "Wrist",
            "Legs",
            "Feet",
            "Ring 1",
            "Ring 2",
            "Main Hand",
            "Off Hand"
        ]
        equipped = [item["slot"]["name"] for item in response["equipped_items"]]
        item_count = 0
        items_enchanted = 0
        enchants = {}
        for item_slot in enchant_slots:
            if item_slot in equipped:
                enchant_found = False
                item_count += 1
                slot_idx = equipped.index(item_slot)
                item_data = response["equipped_items"][slot_idx]
                if "enchantments" in item_data:
                    for enchant_type in item_data["enchantments"]:
                        if enchant_type["enchantment_slot"]["type"] == "PERMANENT":
                            enchant_idx = item_data["enchantments"].index(enchant_type)
                            enchant_found = True
                    if enchant_found:
                        enchant = item_data["enchantments"][enchant_idx]["display_string"]
                    items_enchanted += 1
                enchants[item_slot] = enchant
        enchants = _replace_quality_icons(enchants)
        # TODO make these go to dataframe, and have a separate string reporter
        # TODO probably want to shorten these strings for non-verbose mode
        return_string = f"Enchants:\n- {items_enchanted} / {item_count}"
        for enchant_slot in enchant_slots:
            if enchant_slot not in enchants:
                return_string = f"{return_string}\n- {enchant_slot} missing enchant"
        if verbose:
            for item_slot, enchant_name in enchants.items():
                return_string = f"{return_string}\n- {item_slot}: {enchant_name}"
        return return_string

    def get_current_gems(
            self
    ):
        gem_tertiary = [
            "Head",
            "Wrist",
            "Waist",
        ]
        gem_setting = [
            "Neck",
            "Ring 1",
            "Ring 2",
        ]
        # TODO implement gem info as with enchants above

def _replace_quality_icons(
        enchants_dict: dict[str, str]
):
    """Replaces the quality icons as they show up in the json response with
    the discord quality icon emotes in the No Pressure server
    """
    replacements = {
        "|A:Professions-ChatIcon-Quality-Tier3:20:20|a": ":quality3:",
        "|A:Professions-ChatIcon-Quality-Tier2:20:20|a": ":quality2:",
        "|A:Professions-ChatIcon-Quality-Tier1:20:20|a": ":quality1:",
    }
    new_dict = enchants_dict.copy()
    for slot, item in enchants_dict.items():
        for string, icon in replacements.items():
            if string in item:
                new_dict[slot] = item.replace(string, icon)
    return new_dict
