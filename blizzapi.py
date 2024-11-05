"""Contains shortcuts for retrieving data from the blizzard API

It is expected that you create an oauth class and retrieve a token using `requests_oauthlib` first
to be able to request data
e.g.:
``` python
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import blizzapi

# you'll have to input your own CLIENT_ID and CLIENT_SECRET from the API
TOKEN_URL = "https://oauth.battle.net/token"

# set up oauth and get token
client = BackendApplicationClient(client_id=CLIENT_ID)
oauth = OAuth2Session(client=client, state="blah123")
token = oauth.fetch_token(token_url=TOKEN_URL, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

expansion = "The War Within"
raid = "Nerub-ar Palace"
difficulty = "Heroic"

# retrieve character-specific data
testchar = blizzapi.Character("Aptosaurinae", "Draenor")
blizz_urls = blizzapi.BlizzardAPIURLs()
chardata = blizzapi.CharacterData(testchar, blizz_api_urls=blizz_urls, oauth=oauth)
chardata.get_specific_raid_data(expansion, raid, difficulty)
>>> "Aptosaurinae-Draenor:\n- Heroic Ulgrax the Devourer: <t:1730661349:D>\n- Heroic The Bloodbound Horror: <t:1730662050:D>\n- Heroic Sikran, Captain of the Sureki: <t:1730662840:D>\n- Heroic Rasha'nan: <t:1730663569:D>\n- Heroic Broodtwister Ovi'nax: <t:1730664631:D>\n- Heroic Nexus-Princess Ky'veza: <t:1730667103:D>"
chardata.get_specific_raid_lockout_status(expansion, raid, difficulty)
>>> "Aptosaurinae-Draenor:\n- Heroic Ulgrax the Devourer: True\n- Heroic The Bloodbound Horror: True\n- Heroic Sikran, Captain of the Sureki: True\n- Heroic Rasha'nan: True\n- Heroic Broodtwister Ovi'nax: True\n- Heroic Nexus-Princess Ky'veza: True"
chardata.get_current_enchants()
>>> 'Aptosaurinae-Draenor has no missing enchants'
chardata.get_current_gems()
>>> 'Aptosaurinae-Draenor has no missing gems'

# run a set of characters through the api queries
batch_data = blizzapi.BatchData(oauth, blizz_urls)
batch_data.add_chars([
    Character("Aptosaurinae", "Draenor"),
    Character("Aptosoar, "Draenor"),
    Character("Aptodin", "Draenor")
])
batch_data.get_raid_df(expansion, raid, difficulty)
>>> polars dataframe of raid information
batch_data.get_raid_df(expansion, raid, difficulty, report_type="lockout")
>>> polars dataframe of raid lockout information
batch_data.get_equipment_df()
>>> polars dataframe of equipment
```
"""

from __future__ import annotations
import time
from datetime import date
from datetime import timedelta
from calendar import WEDNESDAY
import warnings
from dataclasses import dataclass
from requests_oauthlib import OAuth2Session
import polars as pl

REGION = "eu"
LANG = "en_GB"

COL_CHAR = "CharacterName"
COL_REALM = "RealmName"

ENCHANT_SLOTS = [
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
GEM_TERTIARY = {
    "Head": 1,
    "Wrist": 1,
    "Waist": 1,
}
GEM_SETTING = {
    "Neck": 2,
    "Ring 1": 2,
    "Ring 2": 2,
}
MISSING_ENCHANT_STR = "Missing Enchant"

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
        self.chars: list[CharacterData] = []

    def add_chars(self, chars: list[Character]):
        for char in chars:
            self.add_char(char)

    def add_char(self, char: Character):
        if char not in self.chars:
            self.chars.append(CharacterData(char, self.oauth, self.urls))

    def get_equipment_df(
            self
    ):
        """Gets enchant and gem data for the current character set
        """
        dataframes = []
        for char in self.chars:
            enchants = char._get_current_enchants_df()
            gems = char._get_current_gems_df()
            equipment = enchants.join(gems, on=[COL_CHAR, COL_REALM])
            dataframes.append(equipment)
        df: pl.DataFrame = pl.concat(dataframes, how="diagonal_relaxed")
        df = df.fill_null("No Item")
        return df

    def get_raid_df(
            self,
            expansion_name: str,
            raid_name: str,
            difficulty: str,
            report_type: str = "progress"
    ):
        """Gets raid progress for a given raid & difficulty for the current character set

        Args:
            expansion_name: Name of the expansion
            raid_name: Name of the relevant raid
            difficulty: Difficulty level of the given raid

        Returns:
            df with a summary of progress
        """
        progress_str = "progress"
        lockout_str = "lockout"
        report_types = [progress_str, lockout_str]
        if report_type not in report_types:
            raise ValueError(f"get_raid_report given invalid report_type: {report_type}")
        if report_type == progress_str:
            col_dtype = pl.Int32
        elif report_type == lockout_str:
            col_dtype = pl.Boolean

        raid_encounters = RaidInfo(
            expansion_name=expansion_name,
            raid_name=raid_name,
            oauth=self.oauth,
        ).get_raid_encounters()
        basic_dict = {
            COL_CHAR: pl.String,
            COL_REALM: pl.String,
        }

        encounters_dict = {
            f"{difficulty} {encounter}": col_dtype
            for encounter
            in raid_encounters
        }
        blank_df = pl.DataFrame(schema=basic_dict | encounters_dict)
        dataframes = [blank_df]
        for char in self.chars:
            if report_type == progress_str:
                dataframes.append(
                    char._get_specific_raid_data_df(
                        expansion_name=expansion_name,
                        raid_name=raid_name,
                        difficulty=difficulty
                    )
                )
            elif report_type == lockout_str:
                dataframes.append(
                    char._get_specific_raid_lockout_status_df(
                        expansion_name=expansion_name,
                        raid_name=raid_name,
                        difficulty=difficulty
                    )
                )
        df: pl.DataFrame = pl.concat(dataframes, how="diagonal_relaxed")
        df = df.fill_null(False)
        return df


class RaidInfo:
    """Gets information about a raid
    """
    def __init__(
            self,
            expansion_name: str,
            raid_name: str,
            oauth: OAuth2Session,
            blizz_api_urls: BlizzardAPIURLs = None,
    ):
        if blizz_api_urls is None:
            blizz_api_urls = BlizzardAPIURLs()
        self.urls = blizz_api_urls
        self.oauth = oauth
        self.expansion = expansion_name
        self.raid = raid_name

    def get_raid_encounters(
            self
    ) -> list[str]:
        """Get a list of raid encounters for the raid based on the class info
        """
        encounters_url = self.urls.get_encounter_journal_index()
        encounters_json = self.oauth.get(encounters_url).json()
        encounters_dict = {item["name"]: item["id"] for item in encounters_json["instances"]}
        raid_id = encounters_dict[self.raid]
        raid_url = self.urls.get_encounter_list(raid_id)
        raid_json = self.oauth.get(raid_url).json()
        encounters = [item["name"] for item in raid_json["encounters"]]
        return encounters

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
        self.refresh_time = 300 # don't re-query data if it's been less than 5 minutes
        self.raid_json = None
        self.raid_json_time = None
        self.equipment_json = None
        self.equipment_json_time = None
        self.df_enchants = self._blank_df()
        # TODO check that char exists and return warning if not?
        # maybe that check should be at the character level instead?

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
        if (
            self.raid_json is None
            or self.raid_json_time > time.mktime(time.gmtime()) + self.refresh_time
        ):
            url = self.urls.get_raids(self.char)
            self.raid_json = self.oauth.get(url).json()
            self.raid_json_time = time.mktime(time.gmtime())
        return self.raid_json

    def _get_equipment_json(
        self
    ) -> str:
        """Retrieves a relevant json containing equipment data for the character
        """
        if (
            self.equipment_json is None
            or self.equipment_json_time > time.mktime(time.gmtime()) + self.refresh_time
        ):
            url = self.urls.get_equipment(self.char)
            self.equipment_json = self.oauth.get(url).json()
            self.equipment_json_time = time.mktime(time.gmtime())
        return self.equipment_json

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

    def _populate_from_dict(
            self,
            data_dict: dict[str, str]
    ):
        """Converts a dict into a df with char name/realm included

        Args:
            enchants: Dictionary of column to value

        Returns:
            Polars dataframe with summary of dict
        """
        df = self._blank_df()
        return df.with_columns(**{key: pl.lit(value) for key, value in data_dict.items()})

    # --- Raid progress

    def _get_specific_raid_data_df(
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
        if "expansions" not in response:
            warnings.warn(f"No data found for {raid_name} [{difficulty}]")
            return self._blank_df()
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
                    return self._populate_from_dict(
                        data_dict={
                            f"{difficulty} {encounter["encounter"]["name"]}":
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
            discord_format: bool = True,
    ) -> str:
        """Gets a summary of raid progress as a nice output string

        Args:
            expansion_name: Name of the expansion
            raid_name: Name of the relevant raid
            difficulty: Difficulty level of the given raid
            discord_format: Whether to format times as strings or discord strings

        Returns:
            string of progression, nicely formatted
        """
        df = self._get_specific_raid_data_df(
            expansion_name=expansion_name,
            raid_name=raid_name,
            difficulty=difficulty
        )
        if df is not None:
            return f"{self.char}:\n{self._raid_progress_report(df, discord_format=discord_format)}"
        else:
            return f"{self.char}: No progress found"

    def _get_specific_raid_lockout_status_df(
            self,
            expansion_name: str,
            raid_name: str,
            difficulty: str,
    ) -> pl.DataFrame:
        """Gets a dataframe of raid lockout status

        Args:
            expansion_name: Name of the expansion
            raid_name: Name of the relevant raid
            difficulty: Difficulty level of the given raid

        Returns:
            dataframe of lockout status
        """
        df = self._get_specific_raid_data_df(
            expansion_name=expansion_name,
            raid_name=raid_name,
            difficulty=difficulty
        )
        if len(df.columns) == 2:
            return df
        df = df.melt(id_vars=[COL_CHAR, COL_REALM])
        df = df.with_columns(
            pl.col("value")
            .map_elements(is_locked_out, return_dtype=pl.Boolean)
            .alias("is_locked")
        )
        df = df.drop("value")
        df = df.pivot(
            values="is_locked",
            index=[COL_CHAR, COL_REALM],
            columns="variable"
        )
        return df

    def get_specific_raid_lockout_status(
            self,
            expansion_name: str,
            raid_name: str,
            difficulty: str,
    ):
        """Gets a summary of raid lockout status as a nice output string

        Args:
            expansion_name: Name of the expansion
            raid_name: Name of the relevant raid
            difficulty: Difficulty level of the given raid

        Returns:
            string of raid lockout status, nicely formatted
        """
        df = self._get_specific_raid_lockout_status_df(
            expansion_name=expansion_name,
            raid_name=raid_name,
            difficulty=difficulty
        )
        relevant_cols = list(df.columns)
        relevant_cols.remove(COL_CHAR)
        relevant_cols.remove(COL_REALM)
        return_string = f"{self.char.name}-{self.char.realm}:"
        for col in relevant_cols:
            return_string = f"{return_string}\n- {col}: {df.select(col)[0,0]}"
        return return_string

    # --- Equipment

    def _get_current_enchants_df(
            self,
    ) -> pl.DataFrame:
        """Gets a dataframe of current enchants for the character
        """
        response = self._get_equipment_json()
        enchant_slots = ENCHANT_SLOTS
        equipped = [item["slot"]["name"] for item in response["equipped_items"]]
        enchants = {}
        for item_slot in enchant_slots:
            if item_slot in equipped:
                enchant_found = False
                slot_idx = equipped.index(item_slot)
                item_data = response["equipped_items"][slot_idx]
                if "enchantments" in item_data:
                    for enchant_type in item_data["enchantments"]:
                        if enchant_type["enchantment_slot"]["type"] == "PERMANENT":
                            enchant_idx = item_data["enchantments"].index(enchant_type)
                            enchant_found = True
                    if enchant_found:
                        enchant = item_data["enchantments"][enchant_idx]["display_string"]
                else:
                    enchant = MISSING_ENCHANT_STR
                enchants[item_slot] = enchant
        enchants = _replace_quality_icons(enchants)
        return self._populate_from_dict(data_dict=enchants)

    def get_current_enchants(
            self,
            verbose = False
    ):
        """Gets a nicely formatted string representation of current enchants

        Args:
            verbose: whether to report on missing slots
        """
        enchants_df = self._get_current_enchants_df()

        return_string = f"{self.char.name}-{self.char.realm} Enchants:"
        equipment_cols = list(enchants_df.columns)
        equipment_cols.remove(COL_CHAR)
        equipment_cols.remove(COL_REALM)
        for item_slot in enchants_df.select(equipment_cols).columns:
            if enchants_df.select(item_slot)[0,0] == MISSING_ENCHANT_STR:
                return_string = f"{return_string}\n- {item_slot}: {MISSING_ENCHANT_STR}"
            elif verbose:
                return_string = f"{return_string}\n- {item_slot}: {enchants_df.select(item_slot)[0,0]}"

        if return_string == f"{self.char.name}-{self.char.realm} Enchants:":
            return_string = f"{self.char.name}-{self.char.realm} has no missing enchants"
        return return_string

    def _get_current_gems_df(
            self
    ):
        """Gets a dataframe of current gems for the character
        """
        response = self._get_equipment_json()
        gem_slots = GEM_TERTIARY | GEM_SETTING
        equipped = [item["slot"]["name"] for item in response["equipped_items"]]
        gems = {}
        for item_slot, sockets_expected in gem_slots.items():
            if item_slot in equipped:
                slot_idx = equipped.index(item_slot)
                item_data = response["equipped_items"][slot_idx]
                for socket_num in range(sockets_expected):
                    if sockets_expected > 1:
                        socket_num_str = f" {socket_num + 1}"
                    else:
                        socket_num_str = ""
                    if "sockets" not in item_data:
                        gems[f"{item_slot} gem{socket_num_str}"] = "Missing socket"
                    elif socket_num + 1 > len(item_data["sockets"]):
                        gems[f"{item_slot} gem{socket_num_str}"] = "Missing socket"
                    elif "item" not in item_data["sockets"][socket_num]:
                        gems[f"{item_slot} gem{socket_num_str}"] = "Missing gem"
                    else:
                        gems[f"{item_slot} gem{socket_num_str}"] = (
                            item_data["sockets"][socket_num]["item"]["name"])
        gems = _replace_quality_icons(gems)
        return self._populate_from_dict(data_dict=gems)

    def get_current_gems(
            self,
            verbose = False
    ):
        """Gets a nicely formatted string representation of current gems

        Args:
            verbose: whether to report on missing slots
        """
        gems_df = self._get_current_gems_df()

        return_string = f"{self.char.name}-{self.char.realm} Gems:"
        equipment_cols = list(gems_df.columns)
        equipment_cols.remove(COL_CHAR)
        equipment_cols.remove(COL_REALM)
        for item_slot in gems_df.select(equipment_cols).columns:
            if (
                gems_df.select(item_slot)[0,0] == "Missing socket"
                or gems_df.select(item_slot)[0,0] == "Missing gem"
            ):
                return_string = f"{return_string}\n- {item_slot}: {gems_df.select(item_slot)[0,0]}"
            elif verbose:
                return_string = f"{return_string}\n- {item_slot}: {gems_df.select(item_slot)[0,0]}"

        if return_string == f"{self.char.name}-{self.char.realm} Gems:":
            return_string = f"{self.char.name}-{self.char.realm} has no missing gems"
        return return_string

def is_locked_out(
        unix_time: int
) -> bool:
    """Checks a time against the last reset time to see if someone is locked out

    Args:
        unix_time: Time in unix seconds to check

    Returns:
        bool: True if time is after last reset (locked out), false if not
    """
    # offset GMT by an hour to get CET which is server time in EU
    server_offset = 3600
    # reset happens at 4am
    reset_hour_offset = 3600 * 4
    today = date.today()
    offset = (today.weekday() - WEDNESDAY) % 7
    last_reset = time.mktime((today - timedelta(days=offset)).timetuple())
    last_reset = last_reset + reset_hour_offset + server_offset
    if unix_time > last_reset:
        return True
    return False

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
