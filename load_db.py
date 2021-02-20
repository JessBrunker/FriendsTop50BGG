import numpy as np
import pandas as pd
import os
import sqlite3

from boardgamegeek import BGGClient


bgg = BGGClient()
game_data = {}
db_loc = 'data/bgg_data.sqlite'
user_list_dir = 'data/user_lists'

# limit publishers to this number - maybe also limit families?
PUBLISHER_COUNT = 5


def build_bgg_dataframe_from_csv_list():
    # get the names of the csv files we'll be using
    files = os.listdir(user_list_dir)
    try:
        # remove the existing database so we can rebuild it
        os.remove(db_loc)
    except OSError as e:
        print('Could not delete db file')
        print(e)
        
    # clear the local caching
    game_data.clear()
    
    for file in files:
        print(f'Working on {file}...')
        filename = f'{user_list_dir}/{file}'
        
        # pull the persons' name from the file
        person = file.split('.')[0]
        
        df = pd.read_csv(filename)
        # assume that the games are ranked in descending order
        df['Rank'] = df.index + 1
        
        load_bgg_data(df, person)
        
    print(f'Finished - built local database at {db_loc}')


def load_bgg_data(df, person):
    # each row object is a NamedTuple instance with named properties matching column names
    for row in df.itertuples():
        # we also have a Title property, but we don't need it
        rank = row.Rank
        bgg_id = row.BggId

        # need to query bgg
        if bgg_id not in game_data:
            game = bgg.game(None, game_id=bgg_id)
            
            # add data to the local cache
            bgg_data = get_game_data(game)
            game_data[bgg_id] = bgg_data
            
            # add this data to the database
            add_to_database(bgg_data)
        
        add_user_ranking(person, bgg_id, rank)


def get_game_data(game):
    # find the player count with the most votes
    player_count_votes = np.array([sg.best for sg in game.player_suggestions])
    best_player_count = game.player_suggestions[player_count_votes.argmax()].player_count
    
    # each level loosely corresponds to a table in the database
    game_info = {
        'game': {
            'id': game.id,
            'name': game.name,
            'year': game.year,
            'playing_time': game.playing_time,
            'rating': game.rating_average,
            'weight': game.rating_average_weight,
            'bgg_rank': game.boardgame_rank,
            'best_count': best_player_count,
            'min_count': game.min_players,
            'max_count': game.max_players,
            'expansions': len(game.expansions),
            'users_rated': game.users_rated,
            'img_url': game.image
        },
        'mechanics': game.mechanics,
        'categories': game.categories,
        'designers': game.designers,
        'publishers': game.publishers[:PUBLISHER_COUNT],
        'families': game.families
    }
    
    return game_info


def add_to_database(bgg_data):
    conn = sqlite3.connect(db_loc)
    
    try:
        df = pd.DataFrame.from_records([bgg_data['game']])
        df.to_sql('games', conn, if_exists='append', index=False)

        bgg_id = bgg_data['game']['id']

        name = 'mechanics'
        df = pd.DataFrame([[bgg_id, mech] for mech in bgg_data[name]], columns=['game_id', 'mechanic'])
        df.to_sql(name, conn, if_exists='append', index=False)

        name = 'categories'
        df = pd.DataFrame([[bgg_id, cat] for cat in bgg_data[name]], columns=['game_id', 'category'])
        df.to_sql(name, conn, if_exists='append', index=False)

        name = 'designers'
        df = pd.DataFrame([[bgg_id, des] for des in bgg_data[name]], columns=['game_id', 'designer'])
        df.to_sql(name, conn, if_exists='append', index=False)

        name = 'publishers'
        df = pd.DataFrame([[bgg_id, pub] for pub in bgg_data[name]], columns=['game_id', 'publisher'])
        df.to_sql(name, conn, if_exists='append', index=False)

        name = 'families'
        df = pd.DataFrame([[bgg_id, fam] for fam in bgg_data[name]], columns=['game_id', 'family'])
        df.to_sql(name, conn, if_exists='append', index=False)
    finally:
        conn.close()
    
def add_user_ranking(user, game, rank):
    conn = sqlite3.connect(db_loc)
    
    try:
        df = pd.DataFrame([[user, game, rank]], columns=['name', 'game_id', 'rank'])
        df.to_sql('user_ranks', conn, if_exists='append', index=False)
    finally:
        conn.close()

    
if __name__ == '__main__':
    build_bgg_dataframe_from_csv_list()