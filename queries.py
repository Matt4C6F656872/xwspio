# queries.py
import json
from sqlalchemy.orm import sessionmaker
from database import init_db, Player, Planet, Resource, Research, Alliance, Building
from sqlalchemy import desc, func
import logging
from tabulate import tabulate
import pandas as pd
import sys

SETTINGS_FILE = 'settings.json'

def load_settings():
    """
    Loads the settings from the SETTINGS_FILE.
    If the file does not exist, returns default settings.
    """
    try:
        with open(SETTINGS_FILE, 'r') as f:
            settings = json.load(f)
    except FileNotFoundError:
        settings = {"user_player": None}
        save_settings(settings)
    return settings

def save_settings(settings):
    """
    Saves the settings to the SETTINGS_FILE.
    """
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(settings, f, indent=4)

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
        print("Type 'b' or 'back' to go back.")
        print("Type 'e' or 'exit' to exit the program.")
        selection = input(f"Select a {option_type[:-1]} by number or name (0 to cancel): ").strip().lower()

        if selection in ['b', 'back']:
            print("\nGoing back...")
            return None

        if selection in ['e', 'exit']:
            confirm_exit()
            return None  # This line won't be reached if exit is confirmed

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
            matches = [option[0] for option in options if selection in option[0].lower()]
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                print("Multiple matches found:")
                for match in matches:
                    print(f"- {match}")
                print("Please be more specific.")
            else:
                print("No matches found. Please try again.")

def export_results(results, columns):
    """
    Prompts the user to export results to a CSV file.

    Parameters:
    - results: List of tuples containing the query results.
    - columns: List of column names corresponding to the results.
    """
    while True:
        export = input("Do you want to export the results to CSV? (y/N): ").strip().lower()
        if export == 'y':
            df = pd.DataFrame(results, columns=columns)
            while True:
                filename = input("Enter the filename (without extension): ").strip()
                if filename:
                    break
                else:
                    print("Filename cannot be empty. Please try again.")
            try:
                df.to_csv(f"{filename}.csv", index=False)
                print(f"Results exported to {filename}.csv")
                break
            except Exception as e:
                print(f"Failed to export results: {e}")
                # Optionally, ask if the user wants to retry
        elif export == 'n' or export == '':
            # Default is 'No'
            break
        else:
            print("Invalid input. Please enter 'y' or 'n'.")

def get_total_raidable_resources(session):
    """
    Retrieves the total raidable resources across all resource types
    and lists individual resource raidable amounts.

    Returns:
        total_raidable (float): The sum of raidable resources.
        individual_resources (list of tuples): Each tuple contains (Resource Type, Raidable Amount).
    """
    total = session.query(func.sum(Resource.raidable)).scalar() or 0.0
    logging.info(f"Total Raidable Resources: {total}")

    individual = session.query(Resource.type, func.sum(Resource.raidable)) \
                        .group_by(Resource.type).all()
    for resource_type, raidable_amount in individual:
        logging.info(f"Resource: {resource_type}, Raidable Amount: {raidable_amount}")

    return total, individual

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

def get_player_planets_and_resources(session, player_name):
    """
    Retrieves planets and their resources for a given player.

    Returns:
        planets_resources (list of tuples): Each tuple contains (Coordinates, Planet Type, Resource Type, Raidable Amount)
    """
    planets = session.query(Planet).filter(Planet.player.has(name=player_name)).all()
    if not planets:
        return []

    planets_resources = []
    for planet in planets:
        coord = f"{planet.x_coord}x{planet.y_coord}x{planet.z_coord}"
        planet_type = planet.planet_type
        resources = session.query(Resource.type, Resource.raidable).filter(Resource.planet_id == planet.id).all()
        for res_type, raidable in resources:
            planets_resources.append((coord, planet_type, res_type, raidable))
    return planets_resources

def confirm_exit():
    """
    Confirms with the user before exiting the program.
    """
    while True:
        confirm = input("Are you sure you want to exit the program? (y/N): ").strip().lower()
        if confirm == 'y':
            print("Exiting the program. Goodbye!")
            sys.exit(0)
        elif confirm == 'n' or confirm == '':
            print("Exit canceled.")
            break
        else:
            print("Invalid input. Please enter 'y' or 'n'.")

def view_player_details(session):
    """
    Allows the user to select a player and view their planets and resources.
    Displays Coordinates, Planet Type, individual resources, sum total per planet,
    and an overall sum total of all resources.
    """
    players = session.query(Player.name).distinct().all()
    if not players:
        print("No players found in the database.")
        return

    player = get_user_selection(players, "Players")
    if not player:
        return  # User chose to go back or cancel

    planets_resources = get_player_planets_and_resources(session, player)
    if not planets_resources:
        print(f"No planets found for player '{player}'.")
        return

    # Organize data per planet
    planet_dict = {}
    for coord, planet_type, res_type, raidable in planets_resources:
        if coord not in planet_dict:
            planet_dict[coord] = {
                "Planet Type": planet_type,
                "Resources": {}
            }
        planet_dict[coord]["Resources"][res_type] = raidable

    # Get all unique resource types for headers
    resource_types = session.query(Resource.type).distinct().all()
    resource_types = sorted([res[0] for res in resource_types])

    # Prepare table data
    headers = ["Coordinates", "Planet Type"] + resource_types + ["Total"]
    table = []
    grand_total = 0.0
    for coord, data in planet_dict.items():
        planet_type = data["Planet Type"]
        row = [coord, planet_type]
        total = 0.0
        for res in resource_types:
            amount = data["Resources"].get(res, 0.0)
            row.append(amount)
            total += amount
        row.append(total)
        grand_total += total
        table.append(row)

    # Append grand total row
    grand_total_row = ["Grand Total", "", *[""] * len(resource_types), grand_total]
    table.append(grand_total_row)

    # Display data
    print(f"\nPlayer: {player}")
    print("\nPlanets and Their Resources:")
    print(tabulate(table, headers=headers, tablefmt="pretty"))

    # Export option
    while True:
        export = input("Do you want to export the results to CSV? (y/N): ").strip().lower()
        if export == 'y':
            # Prepare data for CSV
            df = pd.DataFrame(table, columns=headers)
            while True:
                filename = input("Enter the filename (without extension): ").strip()
                if filename:
                    break
                else:
                    print("Filename cannot be empty. Please try again.")
            try:
                df.to_csv(f"{filename}.csv", index=False)
                print(f"Results exported to {filename}.csv")
                break
            except Exception as e:
                print(f"Failed to export results: {e}")
        elif export == 'n' or export == '':
            # Default is 'No'
            break
        else:
            print("Invalid input. Please enter 'y' or 'n'.")

def set_user_player(session):
    """
    Allows the user to set their player name for comparison purposes.
    """
    players = session.query(Player.name).distinct().all()
    if not players:
        print("No players found in the database.")
        return

    player = get_user_selection(players, "Players to set as User")
    if not player:
        return  # User chose to go back or cancel

    settings = load_settings()
    settings["user_player"] = player
    save_settings(settings)
    print(f"User player set to '{player}'.")

def compare_tech(session):
    """
    Compares research levels between the user player and a target player.
    Displays a table comparing each research's level.
    """
    settings = load_settings()
    user_player_name = settings.get("user_player")
    if not user_player_name:
        print("User player is not set. Please set it in the Settings menu.")
        return

    # Get user player's researches
    user_player = session.query(Player).filter(Player.name == user_player_name).first()
    if not user_player:
        print(f"User player '{user_player_name}' not found in the database.")
        return

    user_researches = {research.name: research.level for research in user_player.researches}

    # Select target player
    players = session.query(Player.name).filter(Player.name != user_player_name).distinct().all()
    if not players:
        print("No other players found in the database.")
        return

    target_player = get_user_selection(players, "Target Players to Compare Tech")
    if not target_player:
        return  # User chose to go back or cancel

    target_player_obj = session.query(Player).filter(Player.name == target_player).first()
    target_researches = {research.name: research.level for research in target_player_obj.researches}

    # Get all unique research names
    all_researches = set(user_researches.keys()).union(set(target_researches.keys()))

    # Prepare comparison table
    comparison = []
    for research in sorted(all_researches):
        user_level = user_researches.get(research, 0)
        target_level = target_researches.get(research, 0)
        comparison.append((research, user_level, target_level))

    # Display comparison
    print(f"\nResearch Comparison between '{user_player_name}' and '{target_player}':")
    table = tabulate(comparison, headers=["Research", f"{user_player_name} Level", f"{target_player} Level"], tablefmt="pretty")
    print(table)

    # Export option
    export_results(comparison, ["Research", f"{user_player_name} Level", f"{target_player} Level"])

def tech_steal_targets(session):
    """
    Picks a target player to steal tech from based on specific criteria.
    """
    settings = load_settings()
    user_player_name = settings.get("user_player")
    if not user_player_name:
        print("User player is not set. Please set it in the Settings menu.")
        return

    # Get user player's researches
    user_player = session.query(Player).filter(Player.name == user_player_name).first()
    if not user_player:
        print(f"User player '{user_player_name}' not found in the database.")
        return

    user_researches = {research.name: research.level for research in user_player.researches}

    # Select research to steal
    researches = session.query(Research.name).distinct().all()
    if not researches:
        print("No researches found in the database.")
        return

    selected_research = get_user_selection(researches, "Research to Steal")
    if not selected_research:
        return  # User chose to go back or cancel

    selected_research_level = user_researches.get(selected_research, 0) + 1  # Target level to reach

    # Find players who have the selected research at a higher level than the user
    potential_targets = session.query(Player).join(Player.researches).filter(
        Research.name == selected_research,
        Research.level >= selected_research_level
    ).all()

    if not potential_targets:
        print(f"No players found with '{selected_research}' level >= {selected_research_level}.")
        return

    # Further filter players who have equal or lower levels in all other researches
    suitable_targets = []
    for target in potential_targets:
        target_researches = {research.name: research.level for research in target.researches}
        higher_in_selected = target_researches.get(selected_research, 0) >= selected_research_level
        lower_or_equal_in_others = all(
            target_researches.get(research, 0) <= user_researches.get(research, 0)
            for research in user_researches
            if research != selected_research
        )
        if higher_in_selected and lower_or_equal_in_others:
            suitable_targets.append(target.name)

    if not suitable_targets:
        print(f"No suitable targets found for stealing '{selected_research}'.")
        return

    # Display suitable targets
    print(f"\nSuitable Targets for Stealing '{selected_research}':")
    table = tabulate([(name,) for name in suitable_targets], headers=["Player Name"], tablefmt="pretty")
    print(table)

    # Export option
    export_results([(name,) for name in suitable_targets], ["Player Name"])

def tech_steal_goals(session):
    """
    Picks target players that are the least "far away" in terms of research levels.
    """
    settings = load_settings()
    user_player_name = settings.get("user_player")
    if not user_player_name:
        print("User player is not set. Please set it in the Settings menu.")
        return

    # Get user player's researches
    user_player = session.query(Player).filter(Player.name == user_player_name).first()
    if not user_player:
        print(f"User player '{user_player_name}' not found in the database.")
        return

    user_researches = {research.name: research.level for research in user_player.researches}

    # Calculate "distance" between user and each player
    players = session.query(Player).filter(Player.name != user_player_name).all()
    if not players:
        print("No other players found in the database.")
        return

    distances = []
    for player in players:
        target_researches = {research.name: research.level for research in player.researches}
        # Calculate the sum of absolute differences in research levels
        all_researches = set(user_researches.keys()).union(set(target_researches.keys()))
        distance = sum(abs(user_researches.get(research, 0) - target_researches.get(research, 0)) for research in all_researches)
        distances.append((player.name, distance))

    # Sort players by distance (ascending)
    sorted_players = sorted(distances, key=lambda x: x[1])

    # Display top N closest players
    top_n = 5  # You can adjust this number as needed
    closest_players = sorted_players[:top_n]
    print(f"\nTop {top_n} Closest Players to '{user_player_name}' in Research Levels:")
    table = tabulate(closest_players, headers=["Player Name", "Distance"], tablefmt="pretty")
    print(table)

    # Export option
    export_results(closest_players, ["Player Name", "Distance"])

def settings_menu(session):
    """
    Displays the Settings menu.
    """
    while True:
        print("\n--- Settings Menu ---")
        print("1. Set Player Name as User")
        print("b. Back to Previous Menu")
        print("e. Exit Program")
        choice = input("Select an option (1, b, e): ").strip().lower()

        if choice == '1':
            set_user_player(session)
        elif choice in ['b', 'back']:
            print("\nReturning to the Query Menu...")
            break
        elif choice in ['e', 'exit']:
            confirm_exit()
        else:
            print("Invalid choice. Please select a valid option.")

def compare_tech(session):
    """
    Compares research levels between the user player and a target player.
    Displays a table comparing each research's level.
    """
    settings = load_settings()
    user_player_name = settings.get("user_player")
    if not user_player_name:
        print("User player is not set. Please set it in the Settings menu.")
        return

    # Get user player's researches
    user_player = session.query(Player).filter(Player.name == user_player_name).first()
    if not user_player:
        print(f"User player '{user_player_name}' not found in the database.")
        return

    user_researches = {research.name: research.level for research in user_player.researches}

    # Select target player
    players = session.query(Player.name).filter(Player.name != user_player_name).distinct().all()
    if not players:
        print("No other players found in the database.")
        return

    target_player = get_user_selection(players, "Target Players to Compare Tech")
    if not target_player:
        return  # User chose to go back or cancel

    target_player_obj = session.query(Player).filter(Player.name == target_player).first()
    target_researches = {research.name: research.level for research in target_player_obj.researches}

    # Get all unique research names
    all_researches = set(user_researches.keys()).union(set(target_researches.keys()))

    # Prepare comparison table
    comparison = []
    for research in sorted(all_researches):
        user_level = user_researches.get(research, 0)
        target_level = target_researches.get(research, 0)
        comparison.append((research, user_level, target_level))

    # Display comparison
    print(f"\nResearch Comparison between '{user_player_name}' and '{target_player}':")
    table = tabulate(comparison, headers=["Research", f"{user_player_name} Level", f"{target_player} Level"], tablefmt="pretty")
    print(table)

    # Export option
    export_results(comparison, ["Research", f"{user_player_name} Level", f"{target_player} Level"])

def tech_steal_targets(session):
    """
    Picks a target player to steal tech from based on specific criteria.
    """
    settings = load_settings()
    user_player_name = settings.get("user_player")
    if not user_player_name:
        print("User player is not set. Please set it in the Settings menu.")
        return

    # Get user player's researches
    user_player = session.query(Player).filter(Player.name == user_player_name).first()
    if not user_player:
        print(f"User player '{user_player_name}' not found in the database.")
        return

    user_researches = {research.name: research.level for research in user_player.researches}

    # Select research to steal
    researches = session.query(Research.name).distinct().all()
    if not researches:
        print("No researches found in the database.")
        return

    selected_research = get_user_selection(researches, "Research to Steal")
    if not selected_research:
        return  # User chose to go back or cancel

    selected_research_level = user_researches.get(selected_research, 0) + 1  # Target level to reach

    # Find players who have the selected research at a higher level than the user
    potential_targets = session.query(Player).join(Player.researches).filter(
        Research.name == selected_research,
        Research.level >= selected_research_level
    ).all()

    if not potential_targets:
        print(f"No players found with '{selected_research}' level >= {selected_research_level}.")
        return

    # Further filter players who have equal or lower levels in all other researches
    suitable_targets = []
    for target in potential_targets:
        target_researches = {research.name: research.level for research in target.researches}
        higher_in_selected = target_researches.get(selected_research, 0) >= selected_research_level
        lower_or_equal_in_others = all(
            target_researches.get(research, 0) <= user_researches.get(research, 0)
            for research in user_researches
            if research != selected_research
        )
        if higher_in_selected and lower_or_equal_in_others:
            suitable_targets.append(target.name)

    if not suitable_targets:
        print(f"No suitable targets found for stealing '{selected_research}'.")
        return

    # Display suitable targets
    print(f"\nSuitable Targets for Stealing '{selected_research}':")
    table = tabulate([(name,) for name in suitable_targets], headers=["Player Name"], tablefmt="pretty")
    print(table)

    # Export option
    export_results([(name,) for name in suitable_targets], ["Player Name"])

def tech_steal_goals(session):
    """
    Picks target players that are the least "far away" in terms of research levels.
    """
    settings = load_settings()
    user_player_name = settings.get("user_player")
    if not user_player_name:
        print("User player is not set. Please set it in the Settings menu.")
        return

    # Get user player's researches
    user_player = session.query(Player).filter(Player.name == user_player_name).first()
    if not user_player:
        print(f"User player '{user_player_name}' not found in the database.")
        return

    user_researches = {research.name: research.level for research in user_player.researches}

    # Calculate "distance" between user and each player
    players = session.query(Player).filter(Player.name != user_player_name).all()
    if not players:
        print("No other players found in the database.")
        return

    distances = []
    for player in players:
        target_researches = {research.name: research.level for research in player.researches}
        # Calculate the sum of absolute differences in research levels
        all_researches = set(user_researches.keys()).union(set(target_researches.keys()))
        distance = sum(abs(user_researches.get(research, 0) - target_researches.get(research, 0)) for research in all_researches)
        distances.append((player.name, distance))

    # Sort players by distance (ascending)
    sorted_players = sorted(distances, key=lambda x: x[1])

    # Display top N closest players
    top_n = 5  # You can adjust this number as needed
    closest_players = sorted_players[:top_n]
    print(f"\nTop {top_n} Closest Players to '{user_player_name}' in Research Levels:")
    table = tabulate(closest_players, headers=["Player Name", "Distance"], tablefmt="pretty")
    print(table)

    # Export option
    export_results(closest_players, ["Player Name", "Distance"])

def settings_menu(session):
    """
    Displays the Settings menu.
    """
    while True:
        print("\n--- Settings Menu ---")
        print("1. Set Player Name as User")
        print("b. Back to Previous Menu")
        print("e. Exit Program")
        choice = input("Select an option (1, b, e): ").strip().lower()

        if choice == '1':
            set_user_player(session)
        elif choice in ['b', 'back']:
            print("\nReturning to the Query Menu...")
            break
        elif choice in ['e', 'exit']:
            confirm_exit()
        else:
            print("Invalid choice. Please select a valid option.")

def list_queries():
    print("\nAvailable Queries:")
    print("1. Player Info")
    print("2. Search")
    print("3. GDZ Tools")
    print("4. Settings")
    print("0. Exit Program")

def execute_query(session):
    while True:
        list_queries()
        print("Type 'b' or 'back' to go back.")
        print("Type 'e' or 'exit' to exit the program.")
        choice = input("\nSelect a query option (1-4): ").strip().lower()

        if choice in ['e', 'exit']:
            confirm_exit()
            continue  # After exiting, loop will stop

        if choice in ['b', 'back']:
            print("\nGoing back to the main menu...")
            break

        if choice == '0':
            confirm_exit()
            continue

        if choice == '1':
            # Player Info Submenu
            while True:
                print("\n--- Player Info ---")
                print("1. View Player Details")
                print("2. Top Players with Most Raidable Resources")
                print("3. Top Players with Highest Research Level")
                print("4. Top Players with Highest Building Level")
                print("5. Get Newest Update for a Player")
                print("b. Back to Previous Menu")
                print("e. Exit Program")
                sub_choice = input("Select an option (1-5, b, e): ").strip().lower()

                if sub_choice in ['e', 'exit']:
                    confirm_exit()
                    continue  # After exiting, loop will stop

                if sub_choice in ['b', 'back']:
                    print("\nReturning to the Query Menu...")
                    break

                if sub_choice == '1':
                    # View Player Details
                    view_player_details(session)

                elif sub_choice == '2':
                    # Top Players with Most Raidable Resources
                    resources = get_unique_resources(session)
                    if not resources:
                        print("No resources found in the database.")
                        continue
                    resource = get_user_selection(resources, "Resources")
                    if resource:
                        limit_input = input("Enter the number of top players to display (default 10): ").strip()
                        try:
                            limit = int(limit_input) if limit_input else 10
                        except ValueError:
                            print("Invalid input. Defaulting to 10.")
                            limit = 10
                        results = get_players_with_most_raidable_resources(session, resource, limit)
                        if results:
                            table = tabulate(results, headers=["Player Name", "Total Raidable"], tablefmt="pretty")
                            print(f"\nTop {limit} Players with Most Raidable {resource.capitalize()}:")
                            print(table)
                            export_results(results, ["Player Name", "Total Raidable"])
                        else:
                            print("No data found for the selected resource.")

                elif sub_choice == '3':
                    # Top Players with Highest Research Level
                    researches = get_unique_researches(session)
                    if not researches:
                        print("No researches found in the database.")
                        continue
                    research = get_user_selection(researches, "Researches")
                    if research:
                        limit_input = input("Enter the number of top players to display (default 10): ").strip()
                        try:
                            limit = int(limit_input) if limit_input else 10
                        except ValueError:
                            print("Invalid input. Defaulting to 10.")
                            limit = 10
                        results = get_players_with_highest_research(session, research, limit)
                        if results:
                            table = tabulate(results, headers=["Player Name", "Research Level"], tablefmt="pretty")
                            print(f"\nTop {limit} Players with Highest {research} Research:")
                            print(table)
                            export_results(results, ["Player Name", "Research Level"])
                        else:
                            print("No data found for the selected research.")

                elif sub_choice == '4':
                    # Top Players with Highest Building Level
                    buildings = get_unique_buildings(session)
                    if not buildings:
                        print("No buildings found in the database.")
                        continue
                    building = get_user_selection(buildings, "Buildings")
                    if building:
                        limit_input = input("Enter the number of top players to display (default 10): ").strip()
                        try:
                            limit = int(limit_input) if limit_input else 10
                        except ValueError:
                            print("Invalid input. Defaulting to 10.")
                            limit = 10
                        results = get_players_with_highest_building_level(session, building, limit)
                        if results:
                            table = tabulate(results, headers=["Player Name", "Max Building Level"], tablefmt="pretty")
                            print(f"\nTop {limit} Players with Highest Level of {building}:")
                            print(table)
                            export_results(results, ["Player Name", "Max Building Level"])
                        else:
                            print("No data found for the selected building.")

                elif sub_choice == '5':
                    # Get Newest Update for a Player
                    player_name = input("Enter the player name to get the newest update: ").strip()
                    if not player_name:
                        print("Player name cannot be empty.")
                        continue
                    update_time = get_newest_update_for_player(session, player_name)
                    if update_time:
                        print(f"\nNewest Update for Player '{player_name}': {update_time}")
                        while True:
                            export = input("Do you want to export the result to CSV? (y/N): ").strip().lower()
                            if export == 'y':
                                df = pd.DataFrame([[player_name, update_time]], columns=["Player Name", "Newest Update"])
                                while True:
                                    filename = input("Enter the filename (without extension): ").strip()
                                    if filename:
                                        break
                                    else:
                                        print("Filename cannot be empty. Please try again.")
                                try:
                                    df.to_csv(f"{filename}.csv", index=False)
                                    print(f"Results exported to {filename}.csv")
                                    break
                                except Exception as e:
                                    print(f"Failed to export results: {e}")
                            elif export == 'n' or export == '':
                                # Default is 'No'
                                break
                            else:
                                print("Invalid input. Please enter 'y' or 'n'.")
                    else:
                        print(f"No data found for player '{player_name}'.")
                else:
                    print("Invalid choice. Please select a valid option.")

        if choice == '2':
            # Search Submenu
            while True:
                print("\n--- Search Menu ---")
                print("1. Players by Name")
                print("2. Players by Alliance")
                print("b. Back to Previous Menu")
                print("e. Exit Program")
                sub_choice = input("Select an option (1-2, b, e): ").strip().lower()

                if sub_choice in ['e', 'exit']:
                    confirm_exit()
                    continue  # After exiting, loop will stop

                if sub_choice in ['b', 'back']:
                    print("\nReturning to the Query Menu...")
                    break

                if sub_choice == '1':
                    # Players by Name
                    player_name = input("Enter part or full player name to search: ").strip()
                    if not player_name:
                        print("Player name cannot be empty.")
                        continue
                    players = search_players(session, player_name=player_name)
                    if players:
                        results = [[p.name, p.race, p.alliance.name if p.alliance else 'None', p.last_update] for p in players]
                        table = tabulate(
                            results,
                            headers=["Player Name", "Race", "Alliance", "Last Update"],
                            tablefmt="pretty"
                        )
                        print("\nSearch Results:")
                        print(table)
                        export_results(results, ["Player Name", "Race", "Alliance", "Last Update"])
                    else:
                        print("No players found matching the criteria.")

                elif sub_choice == '2':
                    # Players by Alliance
                    alliance_name = input("Enter part or full alliance name to search: ").strip()
                    if not alliance_name:
                        print("Alliance name cannot be empty.")
                        continue
                    players = search_players(session, alliance_name=alliance_name)
                    if players:
                        results = [[p.name, p.race, p.alliance.name if p.alliance else 'None', p.last_update] for p in players]
                        table = tabulate(
                            results,
                            headers=["Player Name", "Race", "Alliance", "Last Update"],
                            tablefmt="pretty"
                        )
                        print("\nSearch Results:")
                        print(table)
                        export_results(results, ["Player Name", "Race", "Alliance", "Last Update"])
                    else:
                        print("No players found matching the criteria.")

                else:
                    print("Invalid choice. Please select a valid option.")

        elif choice == '3':
            # GDZ Tools Submenu
            while True:
                print("\n--- GDZ Tools ---")
                print("1. Compare Tech")
                print("2. Tech Steal Targets")
                print("b. Back to Previous Menu")
                print("e. Exit Program")
                sub_choice = input("Select an option (1-2, b, e): ").strip().lower()

                if sub_choice in ['e', 'exit']:
                    confirm_exit()
                    continue  # After exiting, loop will stop

                if sub_choice in ['b', 'back']:
                    print("\nReturning to the Query Menu...")
                    break

                if sub_choice == '1':
                    # Compare Tech
                    compare_tech(session)

                elif sub_choice == '2':
                    # Tech Steal Targets
                    tech_steal_targets(session)

                else:
                    print("Invalid choice. Please select a valid option.")

        elif choice == '4':
            # Settings Submenu
            settings_menu(session)

        else:
            print("Invalid choice. Please select a valid option.")

def show_player_resources(session, player_name):
    """
    Displays a player's planets, their resources, sum total per planet, and a grand total of all resources.
    Includes Planet Type as the second column after Coordinates.
    """
    planets_resources = get_player_planets_and_resources(session, player_name)
    if not planets_resources:
        print(f"No planets found for player '{player_name}'.")
        return

    # Organize data per planet
    planet_dict = {}
    for coord, planet_type, res_type, raidable in planets_resources:
        if coord not in planet_dict:
            planet_dict[coord] = {
                "Planet Type": planet_type,
                "Resources": {}
            }
        planet_dict[coord]["Resources"][res_type] = raidable

    # Get all unique resource types for headers
    resource_types = session.query(Resource.type).distinct().all()
    resource_types = sorted([res[0] for res in resource_types])

    # Prepare table data
    headers = ["Coordinates", "Planet Type"] + resource_types + ["Total"]
    table = []
    grand_total = 0.0
    for coord, data in planet_dict.items():
        planet_type = data["Planet Type"]
        row = [coord, planet_type]
        total = 0.0
        for res in resource_types:
            amount = data["Resources"].get(res, 0.0)
            row.append(amount)
            total += amount
        row.append(total)
        grand_total += total
        table.append(row)

    # Append grand total row
    grand_total_row = ["Grand Total", "", *[""] * len(resource_types), grand_total]
    table.append(grand_total_row)

    # Display data
    print(f"\nPlayer: {player_name}")
    print("\nPlanets and Their Resources:")
    print(tabulate(table, headers=headers, tablefmt="pretty"))

    # Export option
    while True:
        export = input("Do you want to export the results to CSV? (y/N): ").strip().lower()
        if export == 'y':
            # Prepare data for CSV
            df = pd.DataFrame(table, columns=headers)
            while True:
                filename = input("Enter the filename (without extension): ").strip()
                if filename:
                    break
                else:
                    print("Filename cannot be empty. Please try again.")
            try:
                df.to_csv(f"{filename}.csv", index=False)
                print(f"Results exported to {filename}.csv")
                break
            except Exception as e:
                print(f"Failed to export results: {e}")
        elif export == 'n' or export == '':
            # Default is 'No'
            break
        else:
            print("Invalid input. Please enter 'y' or 'n'.")

def list_queries():
    print("\nAvailable Queries:")
    print("1. Player Info")
    print("2. Search")
    print("3. GDZ Tools")
    print("4. Settings")
    print("0. Exit Program")

def execute_query(session):
    while True:
        list_queries()
        print("Type 'b' or 'back' to go back.")
        print("Type 'e' or 'exit' to exit the program.")
        choice = input("\nSelect a query option (1-4): ").strip().lower()

        if choice in ['e', 'exit']:
            confirm_exit()
            continue  # After exiting, loop will stop

        if choice in ['b', 'back']:
            print("\nGoing back to the main menu...")
            break

        if choice == '0':
            confirm_exit()
            continue

        if choice == '1':
            # Player Info Submenu
            while True:
                print("\n--- Player Info ---")
                print("1. View Player Details")
                print("2. Top Players with Most Raidable Resources")
                print("3. Top Players with Highest Research Level")
                print("4. Top Players with Highest Building Level")
                print("5. Get Newest Update for a Player")
                print("b. Back to Previous Menu")
                print("e. Exit Program")
                sub_choice = input("Select an option (1-5, b, e): ").strip().lower()

                if sub_choice in ['e', 'exit']:
                    confirm_exit()
                    continue  # After exiting, loop will stop

                if sub_choice in ['b', 'back']:
                    print("\nReturning to the Query Menu...")
                    break

                if sub_choice == '1':
                    # View Player Details
                    view_player_details(session)

                elif sub_choice == '2':
                    # Top Players with Most Raidable Resources
                    resources = get_unique_resources(session)
                    if not resources:
                        print("No resources found in the database.")
                        continue
                    resource = get_user_selection(resources, "Resources")
                    if resource:
                        limit_input = input("Enter the number of top players to display (default 10): ").strip()
                        try:
                            limit = int(limit_input) if limit_input else 10
                        except ValueError:
                            print("Invalid input. Defaulting to 10.")
                            limit = 10
                        results = get_players_with_most_raidable_resources(session, resource, limit)
                        if results:
                            table = tabulate(results, headers=["Player Name", "Total Raidable"], tablefmt="pretty")
                            print(f"\nTop {limit} Players with Most Raidable {resource.capitalize()}:")
                            print(table)
                            export_results(results, ["Player Name", "Total Raidable"])
                        else:
                            print("No data found for the selected resource.")

                elif sub_choice == '3':
                    # Top Players with Highest Research Level
                    researches = get_unique_researches(session)
                    if not researches:
                        print("No researches found in the database.")
                        continue
                    research = get_user_selection(researches, "Researches")
                    if research:
                        limit_input = input("Enter the number of top players to display (default 10): ").strip()
                        try:
                            limit = int(limit_input) if limit_input else 10
                        except ValueError:
                            print("Invalid input. Defaulting to 10.")
                            limit = 10
                        results = get_players_with_highest_research(session, research, limit)
                        if results:
                            table = tabulate(results, headers=["Player Name", "Research Level"], tablefmt="pretty")
                            print(f"\nTop {limit} Players with Highest {research} Research:")
                            print(table)
                            export_results(results, ["Player Name", "Research Level"])
                        else:
                            print("No data found for the selected research.")

                elif sub_choice == '4':
                    # Top Players with Highest Building Level
                    buildings = get_unique_buildings(session)
                    if not buildings:
                        print("No buildings found in the database.")
                        continue
                    building = get_user_selection(buildings, "Buildings")
                    if building:
                        limit_input = input("Enter the number of top players to display (default 10): ").strip()
                        try:
                            limit = int(limit_input) if limit_input else 10
                        except ValueError:
                            print("Invalid input. Defaulting to 10.")
                            limit = 10
                        results = get_players_with_highest_building_level(session, building, limit)
                        if results:
                            table = tabulate(results, headers=["Player Name", "Max Building Level"], tablefmt="pretty")
                            print(f"\nTop {limit} Players with Highest Level of {building}:")
                            print(table)
                            export_results(results, ["Player Name", "Max Building Level"])
                        else:
                            print("No data found for the selected building.")

                elif sub_choice == '5':
                    # Get Newest Update for a Player
                    player_name = input("Enter the player name to get the newest update: ").strip()
                    if not player_name:
                        print("Player name cannot be empty.")
                        continue
                    update_time = get_newest_update_for_player(session, player_name)
                    if update_time:
                        print(f"\nNewest Update for Player '{player_name}': {update_time}")
                        while True:
                            export = input("Do you want to export the result to CSV? (y/N): ").strip().lower()
                            if export == 'y':
                                df = pd.DataFrame([[player_name, update_time]], columns=["Player Name", "Newest Update"])
                                while True:
                                    filename = input("Enter the filename (without extension): ").strip()
                                    if filename:
                                        break
                                    else:
                                        print("Filename cannot be empty. Please try again.")
                                try:
                                    df.to_csv(f"{filename}.csv", index=False)
                                    print(f"Results exported to {filename}.csv")
                                    break
                                except Exception as e:
                                    print(f"Failed to export results: {e}")
                            elif export == 'n' or export == '':
                                # Default is 'No'
                                break
                            else:
                                print("Invalid input. Please enter 'y' or 'n'.")
                    else:
                        print(f"No data found for player '{player_name}'.")
                else:
                    print("Invalid choice. Please select a valid option.")

        if choice == '2':
            # Search Submenu
            while True:
                print("\n--- Search Menu ---")
                print("1. Players by Name")
                print("2. Players by Alliance")
                print("b. Back to Previous Menu")
                print("e. Exit Program")
                sub_choice = input("Select an option (1-2, b, e): ").strip().lower()

                if sub_choice in ['e', 'exit']:
                    confirm_exit()
                    continue  # After exiting, loop will stop

                if sub_choice in ['b', 'back']:
                    print("\nReturning to the Query Menu...")
                    break

                if sub_choice == '1':
                    # Players by Name
                    player_name = input("Enter part or full player name to search: ").strip()
                    if not player_name:
                        print("Player name cannot be empty.")
                        continue
                    players = search_players(session, player_name=player_name)
                    if players:
                        results = [[p.name, p.race, p.alliance.name if p.alliance else 'None', p.last_update] for p in players]
                        table = tabulate(
                            results,
                            headers=["Player Name", "Race", "Alliance", "Last Update"],
                            tablefmt="pretty"
                        )
                        print("\nSearch Results:")
                        print(table)
                        export_results(results, ["Player Name", "Race", "Alliance", "Last Update"])
                    else:
                        print("No players found matching the criteria.")

                elif sub_choice == '2':
                    # Players by Alliance
                    alliance_name = input("Enter part or full alliance name to search: ").strip()
                    if not alliance_name:
                        print("Alliance name cannot be empty.")
                        continue
                    players = search_players(session, alliance_name=alliance_name)
                    if players:
                        results = [[p.name, p.race, p.alliance.name if p.alliance else 'None', p.last_update] for p in players]
                        table = tabulate(
                            results,
                            headers=["Player Name", "Race", "Alliance", "Last Update"],
                            tablefmt="pretty"
                        )
                        print("\nSearch Results:")
                        print(table)
                        export_results(results, ["Player Name", "Race", "Alliance", "Last Update"])
                    else:
                        print("No players found matching the criteria.")

                else:
                    print("Invalid choice. Please select a valid option.")

        elif choice == '3':
            # GDZ Tools Submenu
            while True:
                print("\n--- GDZ Tools ---")
                print("1. Compare Tech")
                print("2. Tech Steal Targets")
                print("b. Back to Previous Menu")
                print("e. Exit Program")
                sub_choice = input("Select an option (1-2, b, e): ").strip().lower()

                if sub_choice in ['e', 'exit']:
                    confirm_exit()
                    continue  # After exiting, loop will stop

                if sub_choice in ['b', 'back']:
                    print("\nReturning to the Query Menu...")
                    break

                if sub_choice == '1':
                    # Compare Tech
                    compare_tech(session)

                elif sub_choice == '2':
                    # Tech Steal Targets
                    tech_steal_targets(session)

                else:
                    print("Invalid choice. Please select a valid option.")

        elif choice == '4':
            # Settings Submenu
            settings_menu(session)

        else:
            print("Invalid choice. Please select a valid option.")
