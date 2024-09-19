# parser.py
import requests
from bs4 import BeautifulSoup
import re
from sqlalchemy.orm import sessionmaker
from database import Player, Alliance, Planet, Resource, Building, Research
from datetime import datetime, timezone
import logging
import time

def parse_espionage_report(content):
    soup = BeautifulSoup(content, 'html.parser')
    report_data = {
        "planet_info": {},
        "player_details": {},
        "resources": {},
        "buildings": {},
        "researches": {}
    }

    # Extract planet information
    planet_info = soup.find('td', class_='second', colspan='4', width='90%')
    if planet_info:
        planet_text = planet_info.get_text(strip=True).replace("Planet information ", "")
        # Use regex to extract name and coordinates
        match = re.match(r'"(.*?)\s*-\s*(\d+x\d+x\d+)"', planet_text)
        if match:
            name, coordinates = match.groups()
            report_data["planet_info"]["name"] = name.strip() if name else "Unnamed"
            report_data["planet_info"]["coordinates"] = coordinates
        else:
            # If the regex doesn't match, store the whole text as coordinates
            report_data["planet_info"]["name"] = "Unnamed"
            report_data["planet_info"]["coordinates"] = planet_text.strip('"')
    else:
        report_data["planet_info"]["name"] = "Unknown"
        report_data["planet_info"]["coordinates"] = "Unknown"

    # Extract player details and other information
    details = soup.find_all('td', class_=['second', 'first'])
    for detail in details:
        label = detail.get_text(strip=True)
        if label.endswith(':'):
            value = detail.find_next('td').get_text(strip=True)
            key = label[:-1].lower()
            if key in ['owner', 'race', 'alliance']:
                report_data["player_details"][key] = value
            elif key in ['temperature', 'planet type', 'attack', 'defense', 'invasion protection']:
                if key in ['attack', 'defense']:
                    # Extract only the numeric part before any non-digit characters
                    numeric_value = re.match(r'(\d+)', value)
                    if numeric_value:
                        report_data["planet_info"][key] = int(numeric_value.group(1))
                    else:
                        report_data["planet_info"][key] = 0  # Default to 0 if not found
                else:
                    report_data["planet_info"][key] = value
            elif key in ['pig-iron', 'crystals', 'frubin', 'orizin', 'frurozin', 'gold']:
                if '(' in value:
                    total, raidable = value.split('(')
                    report_data["resources"][key] = {
                        "total": total.strip().replace(',', '.'),  # Ensure float conversion later
                        "raidable": raidable.replace(')', '').strip().replace(',', '.')  # Ensure float conversion later
                    }
                else:
                    report_data["resources"][key] = {
                        "total": value.strip().replace(',', '.'),  # Ensure float conversion later
                        "raidable": "0"
                    }

    # Find the tables containing buildings and researches
    tables = soup.find_all('table', attrs={'border': '0', 'cellspacing': '1', 'cellpadding': '0', 'width': '100%', 'colspan': '2'})

    if len(tables) >= 2:
        # Extract buildings
        buildings_table = tables[0]
        for row in buildings_table.find_all('td', class_=['first', 'second']):
            building_info = row.get_text(strip=True)
            match = re.match(r"(.+)\s(\d+)$", building_info)
            if match:
                building_name, building_level = match.groups()
                report_data["buildings"][building_name.strip()] = int(building_level)

        # Extract researches
        researches_table = tables[1]
        for row in researches_table.find_all('td', class_=['first', 'second']):
            research_info = row.get_text(strip=True)
            match = re.match(r"(.+)\s(\d+)$", research_info)
            if match:
                research_name, research_level = match.groups()
                report_data["researches"][research_name.strip()] = int(research_level)

    return report_data

def process_reports(urls, session):
    for url in urls:
        try:
            response = requests.get(url)
            if response.status_code != 200:
                logging.warning(f"Failed to retrieve {url}: Status code {response.status_code}")
                continue

            report = parse_espionage_report(response.content)
            player_name = report["player_details"].get("owner", "Unknown")
            race = report["player_details"].get("race", "Unknown")
            alliance_name = report["player_details"].get("alliance", "None")

            # Get or create Alliance
            if alliance_name != "None":
                alliance = session.query(Alliance).filter_by(name=alliance_name).first()
                if not alliance:
                    alliance = Alliance(name=alliance_name)
                    session.add(alliance)
                    session.commit()
            else:
                alliance = None  # Correctly handle players without alliances

            # Get or create Player
            player = session.query(Player).filter_by(name=player_name).first()
            if not player:
                player = Player(
                    name=player_name,
                    race=race,
                    alliance=alliance,
                    last_update=datetime.now(timezone.utc)  # Updated here
                )
                session.add(player)
                session.commit()
            else:
                # Update existing player
                player.race = race
                player.alliance = alliance
                player.last_update = datetime.now(timezone.utc)  # Updated here
                session.commit()

            # Get or create Planet
            coordinates = report["planet_info"].get("coordinates", "Unknown")
            planet = session.query(Planet).filter_by(coordinates=coordinates).first()
            if not planet:
                planet = Planet(
                    name=report["planet_info"].get("name", "Unknown"),
                    coordinates=coordinates,
                    temperature=report["planet_info"].get("temperature", "Unknown"),
                    planet_type=report["planet_info"].get("planet type", "Unknown"),
                    attack=report["planet_info"].get("attack", 0),  # Already an integer
                    defense=report["planet_info"].get("defense", 0),  # Already an integer
                    invasion_protection=report["planet_info"].get("invasion protection", "Unknown"),
                    player=player
                )
                session.add(planet)
                session.commit()
            else:
                # Update existing planet
                planet.name = report["planet_info"].get("name", "Unknown")
                planet.temperature = report["planet_info"].get("temperature", "Unknown")
                planet.planet_type = report["planet_info"].get("planet type", "Unknown")
                planet.attack = report["planet_info"].get("attack", 0)
                planet.defense = report["planet_info"].get("defense", 0)
                planet.invasion_protection = report["planet_info"].get("invasion protection", "Unknown")
                session.commit()

            # Update Resources
            for res_type, res_values in report["resources"].items():
                resource = session.query(Resource).filter_by(type=res_type, planet=planet).first()
                if not resource:
                    try:
                        total = float(res_values["total"])
                    except ValueError:
                        total = 0.0
                    try:
                        raidable = float(res_values["raidable"])
                    except ValueError:
                        raidable = 0.0
                    resource = Resource(
                        type=res_type,
                        total=total,
                        raidable=raidable,
                        planet=planet
                    )
                    session.add(resource)
                else:
                    try:
                        resource.total = float(res_values["total"])
                    except ValueError:
                        resource.total = 0.0
                    try:
                        resource.raidable = float(res_values["raidable"])
                    except ValueError:
                        resource.raidable = 0.0
                session.commit()

            # Update Buildings
            for building_name, building_level in report["buildings"].items():
                building = session.query(Building).filter_by(name=building_name, planet=planet).first()
                if not building:
                    building = Building(
                        name=building_name,
                        level=building_level,
                        planet=planet
                    )
                    session.add(building)
                else:
                    building.level = building_level
                session.commit()

            # Update Researches (shared per player)
            for research_name, research_level in report["researches"].items():
                research = session.query(Research).filter_by(name=research_name, player=player).first()
                if not research:
                    research = Research(
                        name=research_name,
                        level=research_level,
                        player=player
                    )
                    session.add(research)
                else:
                    if research_level > research.level:
                        research.level = research_level
                session.commit()

            logging.info(f"Processed report from {player_name} at {coordinates}")

            time.sleep(1)  # Sleep to prevent overwhelming the server

        except Exception as e:
            logging.error(f"Error processing {url}: {e}")
