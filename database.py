# database.py
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()

class Alliance(Base):
    __tablename__ = 'alliances'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    players = relationship('Player', back_populates='alliance')

class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    race = Column(String)
    alliance_id = Column(Integer, ForeignKey('alliances.id'), nullable=True)  # Made nullable
    last_update = Column(DateTime, default=datetime.utcnow)

    alliance = relationship('Alliance', back_populates='players')
    planets = relationship('Planet', back_populates='player')
    researches = relationship('Research', back_populates='player')

class Planet(Base):
    __tablename__ = 'planets'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    coordinates = Column(String, unique=True)
    temperature = Column(String)
    planet_type = Column(String)
    attack = Column(Integer)
    defense = Column(Integer)
    invasion_protection = Column(String)
    player_id = Column(Integer, ForeignKey('players.id'))

    player = relationship('Player', back_populates='planets')
    resources = relationship('Resource', back_populates='planet')
    buildings = relationship('Building', back_populates='planet')

class Resource(Base):
    __tablename__ = 'resources'
    id = Column(Integer, primary_key=True)
    type = Column(String)
    total = Column(Float)
    raidable = Column(Float)
    planet_id = Column(Integer, ForeignKey('planets.id'))

    planet = relationship('Planet', back_populates='resources')

class Building(Base):
    __tablename__ = 'buildings'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    level = Column(Integer)
    planet_id = Column(Integer, ForeignKey('planets.id'))

    planet = relationship('Planet', back_populates='buildings')

class Research(Base):
    __tablename__ = 'researches'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    level = Column(Integer)
    player_id = Column(Integer, ForeignKey('players.id'))

    player = relationship('Player', back_populates='researches')

def init_db(db_name='espionage_reports.db'):
    engine = create_engine(f'sqlite:///{db_name}', echo=False)
    Base.metadata.create_all(engine)
    return engine
