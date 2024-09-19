# queries.py
from sqlalchemy.orm import sessionmaker
from database import init_db, Player, Planet, Resource, Research, Alliance, Building
from sqlalchemy import desc, func
import logging
from tabulate import tabulate

def get_unique_resources(session):
    return session.query(Resource.type).distinct().all()

def get_unique_buildings(session):
    return session.query(Building.name).distinct().all()

def get_unique_researches(session):
    return session.query(Research.name).distinct().all()

def display_options(options, option_type):
    print(f"\nAvailable {option_type}:")
    for idx, (option,) in enumerate(options, start=1):
        print(f"{idx}. {option}")
    print("0. Cancel")

def get_user_selection(options, option_type):
    while True:
        display_options(options, option_type)
        selection = input(f"Select a {option_type[:-1]} by number or name (0 to cancel): ").strip()
        
        if selection == '0':
            return None
        
        # Try to interpret as a number
        if selection.isdigit():
            index = int(selection) - 1
            if 0 <= index < len(options):
                return options[index][0]
            else:
                print("Invalid number selection. Please try again.")
        else:
            # Attempt case-insensitive partial match
            matches = [option[0] for option in options if selection.lower() in option[0].lower()]
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                print("Multiple matches found:")
                for match in matches:
                    print(f"- {match}")
                print("Please be more specific.")
            else:
                print("No matches found. Please try again.")

def get_players_with_most_raidable_resources(session, resource_type, limit=10):
    results = session.query(
        Player.name,
        func.sum(Resource.raidable).label('total_raidable')
    ).join(Player.planets).join(Planet.resources).filter(
        Resource.type == resource_type
    ).group_by(Player.id).order_by(desc('total_raidable')).limit(limit).all()
    return results

def get_players_with_highest_research(session, research_name, limit=10):
    results = session.query(
        Player.name,
        Research.level
    ).join(Player.researches).filter(
        Research.name == research_name
    ).order_by(desc(Research.level)).limit(limit).all()
    return results

def get_players_with_highest_building_level(session, building_name, limit=10):
    results = session.query(
        Player.name,
        func.max(Building.level).label('max_level')
    ).join(Player.planets).join(Planet.buildings).filter(
        Building.name == building_name
    ).group_by(Player.id).order_by(desc('max_level')).limit(limit).all()
    return results

def search_players(session, player_name=None, alliance_name=None):
    query = session.query(Player)
    if player_name:
        query = query.filter(Player.name.ilike(f"%{player_name}%"))
    if alliance_name:
        query = query.join(Player.alliance).filter(Alliance.name.ilike(f"%{alliance_name}%"))
    return query.all()

def get_newest_update_for_player(session, player_name):
    player = session.query(Player).filter(Player.name == player_name).first()
    if player:
        return player.last_update
    return None

def list_queries():
    print("\nAvailable Queries:")
    print("1. Top Players with Most Raidable Resources")
    print("2. Top Players with Highest Research Level")
    print("3. Top Players with Highest Building Level")
    print("4. Search Players by Name or Alliance")
    print("5. Get Newest Update for a Player")
    print("6. Return to Main Menu")

def execute_query(session):
    while True:
        list_queries()
        choice = input("\nSelect a query option (1-6): ").strip()
        if choice == '1':
            # Top Players with Most Raidable Resources
            resources = get_unique_resources(session)
            if not resources:
                print("No resources found in the database.")
                continue
            resource = get_user_selection(resources, "Resources")
            if resource:
                limit = input("Enter the number of top players to display (default 10): ").strip()
                try:
                    limit = int(limit) if limit else 10
                except ValueError:
                    limit = 10
                results = get_players_with_most_raidable_resources(session, resource, limit)
                if results:
                    table = tabulate(results, headers=["Player Name", "Total Raidable"], tablefmt="pretty")
                    print(f"\nTop {limit} Players with Most Raidable {resource.capitalize()}:")
                    print(table)
                else:
                    print("No data found for the selected resource.")
        elif choice == '2':
            # Top Players with Highest Research Level
            researches = get_unique_researches(session)
            if not researches:
                print("No researches found in the database.")
                continue
            research = get_user_selection(researches, "Researches")
            if research:
                limit = input("Enter the number of top players to display (default 10): ").strip()
                try:
                    limit = int(limit) if limit else 10
                except ValueError:
                    limit = 10
                results = get_players_with_highest_research(session, research, limit)
                if results:
                    table = tabulate(results, headers=["Player Name", "Research Level"], tablefmt="pretty")
                    print(f"\nTop {limit} Players with Highest {research} Research:")
                    print(table)
                else:
                    print("No data found for the selected research.")
        elif choice == '3':
            # Top Players with Highest Building Level
            buildings = get_unique_buildings(session)
            if not buildings:
                print("No buildings found in the database.")
                continue
            building = get_user_selection(buildings, "Buildings")
            if building:
                limit = input("Enter the number of top players to display (default 10): ").strip()
                try:
                    limit = int(limit) if limit else 10
                except ValueError:
                    limit = 10
                results = get_players_with_highest_building_level(session, building, limit)
                if results:
                    table = tabulate(results, headers=["Player Name", "Max Building Level"], tablefmt="pretty")
                    print(f"\nTop {limit} Players with Highest Level of {building}:")
                    print(table)
                else:
                    print("No data found for the selected building.")
        elif choice == '4':
            # Search Players by Name or Alliance
            player_name = input("Enter part or full player name to search (leave blank to skip): ").strip()
            alliance_name = input("Enter part or full alliance name to search (leave blank to skip): ").strip()
            if not player_name and not alliance_name:
                print("At least one search criterion must be provided.")
                continue
            players = search_players(session, player_name if player_name else None, alliance_name if alliance_name else None)
            if players:
                table = tabulate(
                    [[p.name, p.race, p.alliance.name if p.alliance else 'None', p.last_update] for p in players],
                    headers=["Player Name", "Race", "Alliance", "Last Update"],
                    tablefmt="pretty"
                )
                print("\nSearch Results:")
                print(table)
            else:
                print("No players found matching the criteria.")
        elif choice == '5':
            # Get Newest Update for a Player
            player_name = input("Enter the player name to get the newest update: ").strip()
            if not player_name:
                print("Player name cannot be empty.")
                continue
            update_time = get_newest_update_for_player(session, player_name)
            if update_time:
                print(f"\nNewest Update for Player '{player_name}': {update_time}")
            else:
                print(f"No data found for player '{player_name}'.")
        elif choice == '6':
            # Return to Main Menu
            break
        else:
            print("Invalid choice. Please select a valid option.")

def main_queries():
    # Initialize the database
    engine = init_db()
    Session = sessionmaker(bind=engine)
    session = Session()

    execute_query(session)

    session.close()
