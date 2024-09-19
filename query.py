# query.py
from sqlalchemy.orm import sessionmaker
from database import init_db, Player, Planet, Resource, Research, Alliance, Building  # Ensure Building is imported
from sqlalchemy import desc, func
import logging

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

def main_queries():
    engine = init_db()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Example Queries
    print("Top 10 Players with Most Raidable Pig-Iron:")
    results = get_players_with_most_raidable_resources(session, 'pig-iron', 10)
    for name, total in results:
        print(f"{name}: {total}")

    print("\nTop 10 Players with Highest Espionage Research:")
    results = get_players_with_highest_research(session, 'Espionage', 10)
    for name, level in results:
        print(f"{name}: Level {level}")

    print("\nTop 10 Players with Highest Level of Headquarter:")
    results = get_players_with_highest_building_level(session, 'Headquarter', 10)
    for name, max_level in results:
        print(f"{name}: Level {max_level}")

    print("\nSearch Players by Name containing 'Eren':")
    players = search_players(session, player_name='Eren')
    for player in players:
        alliance = player.alliance.name if player.alliance else 'None'
        print(f"{player.name} - Alliance: {alliance}")

    session.close()

if __name__ == "__main__":
    main_queries()
