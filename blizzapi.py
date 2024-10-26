
from dataclasses import dataclass
from requests_oauthlib import OAuth2Session

REGION = "eu"
LANG = "en_GB"
NAMESPACE = "profile-eu"

@dataclass
class Character:
    name: str
    realm: str

class BlizzardAPIURLs:
    """Provides shortcuts to Blizz API URLs
    """
    def __init__(
            self,
            region: str = REGION,
            locale:str = LANG,
            namespace: str = NAMESPACE,
    ):
        self.hostname = f"https://{region}.api.blizzard.com"
        self.profile_user = "/profile/user/wow"
        self.profile_char = "/profile/wow/character"
        self.profile_guild = "/profile/wow/guild"
        self.locale = f"locale={locale}"
        self.namespace= f"namespace={namespace}"
        self.urlend = f"?{self.locale}&{self.namespace}"

    # --- base request
    def _char(self, char: Character):
        return f"{self.hostname}{self.profile_char}/{char.realm.lower()}/{char.name.lower()}"

    # --- equipment
    def get_equipment(self, char: Character):
        return f"{self._char(char)}/equipment{self.urlend}"

    # --- Encounters
    def _encounters(self, char: Character, encounter_type: str):
        return f"{self._char(char)}/encounters/{encounter_type}{self.urlend}"

    def get_raids(self, char: Character):
        return self._encounters(char, "raids")

class CharacterData:
    """Gathers data about a character
    """
    def __init__(
            self,
            char: Character,
            blizz_api_urls: BlizzardAPIURLs,
            oauth: OAuth2Session,
    ):
        self.char = char
        self.urls = blizz_api_urls
        self.oauth = oauth

    def get_current_raid_data(
            self,
            expansion_name: str,
            raid_name: str,
            difficulty: str
    ):
        url = self.urls.get_raids(self.char)
        response = self.oauth.get(url).json()
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
                    progress_summary = f"- Progress: {char_raid_data["completed_count"]} / {char_raid_data["total_count"]}"
                    boss_summary = {encounter["encounter"]["name"]: f"<t:{str(encounter["last_kill_timestamp"])[:-3]}:D>" for encounter in char_raid_data["encounters"]}
                    return_string = f"{raid_name} [{difficulty}]:\n{progress_summary}"
                    for boss_name, boss_last_killed in boss_summary.items():
                        return_string = f"{return_string}\n- {boss_name}: {boss_last_killed}"
                    return return_string
        return f"No data found for {raid_name} [{difficulty}]"

    def get_current_enchants(
            self,
            verbose = False
    ):
        url = self.urls.get_equipment(self.char)
        response = self.oauth.get(url).json()
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
        enchants = replace_quality_icons(enchants)
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

def replace_quality_icons(
        enchants_dict: dict[str, str]
):
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
