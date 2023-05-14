# Import libraries
import betfairlightweight
from betfairlightweight import APIClient
from betfairlightweight import filters
from betfairlightweight import StreamListener
import pandas as pd
import numpy as np
import os
import datetime
import json
from db_betfair import DBMethods



# TODO - create utils file

# OK
def betfair_login():
    delayed_app_key = 'KaiQJ11LhakrDsh5'
    live_app_key = 'wsuhoFnT0LAokHC9'
    app_key = delayed_app_key
    UserName = 'darrenblount'
    Password = '76N[<z}/6bsWw^c'

    trading = betfairlightweight.APIClient(UserName, Password, app_key=app_key, certs=r"C:\certs")

    trading.login()
    return trading

# OK
def dict_to_json_file(output_file_name, input_dict):
    # Input = python dict
    # Output = json file
    with open(output_file_name+'.json', 'w') as outfile:
        # Write the dictionary to the file as JSON data
        json.dump(input_dict, outfile)

# OK
def json_file_to_dict(input_file_name):
    # Open the JSON file in read mode
    with open(input_file_name+'.json', 'r') as infile:
        # Load the data from the file as a Python dictionary
        my_dict = json.load(infile)

    return my_dict


class EventTypes:

    def get_event_types_from_betfair(self, trading):
        # Grab all event type ids. This will return a list which we will iterate over to print out the id and the name of the sport
        event_types = trading.betting.list_event_types()
        event_type_dict = {}
        for i in event_types:
            event_type_dict[i.event_type.name] = i.event_type.id

        dict_to_json_file('betfair_event_types', event_type_dict)

    def add_event_types_to_db(self, trading, connect):
        # Grab all event type ids. This will return a list which we will iterate over to print out the id and the name of the sport
        event_types = trading.betting.list_event_types()

        for i in event_types:
            print(i.event_type.name, i.event_type.id)
            connect.insert_betfair_event_type(i.event_type.id, i.event_type.name)
            print('\tadded to db')

    def get_event_types_from_db(self, connect):

        r = connect.select_betfair_event_type()
        for i in r:
            _id = i[0]
            event_types = i[1]
            print('{} - {}'.format(_id, event_types))


class Competitions:

    def get_competition_from_betfair_by_event_type(self, trading, event_type_id):

        # Create a competition filter
        competition_filter = betfairlightweight.filters.market_filter(event_type_ids=[event_type_id])

        # Get a list of competitions
        competitions = trading.betting.list_competitions(filter=competition_filter)

        competitions_dict = {}
        for i in competitions:
            competitions_dict[i.competition.name] = i.competition.id

        print(competitions_dict)
        dict_to_json_file(f'competitions_in_event_type_{event_type_id}', competitions_dict)

    def add_competion_from_betfair_to_db_by_event_type(self, trading, connect, event_Type_id):

        # Create a competition filter
        competition_filter = betfairlightweight.filters.market_filter(event_type_ids=[event_Type_id])

        # Get a list of competitions
        r = trading.betting.list_competitions(filter=competition_filter)

        for i in r:
            print(i.competition.id, i.competition.name)
            connect.insert_betfair_competition(i.competition.id, event_Type_id, i.competition.name)
            print('\tadded to db')

    def get_competitions_from_db_by_event_type(self, connect, event_type):

        r = connect.select_betfair_competition(event_type)
        for i in r:
            _id = i[0]
            event_types = i[1]
            competition = i[2]
            print('{} - {} - {}'.format(_id, event_types, competition))


class Events:

    def get_events_from_betfair_by_event_type(self, trading, event_type_id):

        # Create a competition filter
        competition_filter = betfairlightweight.filters.market_filter(event_type_ids=[event_type_id])

        # Get a list of competitions
        r = trading.betting.list_events(filter=competition_filter)

        events_list = []
        for i in r:
            # print(i.event.id, i.event.name, i.event.venue, i.event.country_code, i.event.time_zone, i.market_count, i.elapsed_time)
            events_list += [{'event_id': i.event.id,
                            'event_name': i.event.name,
                            'event_venue': i.event.venue,
                            'event_country_code': i.event.country_code,
                            'event_timezone': i.event.time_zone,
                            'event_market_count': i.market_count,
                            'event_elapsed_time': i.elapsed_time}]

        return events_list

    def get_events_from_betfair_by_competition_id(self, trading, competition_id):

        # Create a competition filter
        competition_filter = betfairlightweight.filters.market_filter(competition_ids=[competition_id])

        # Get a list of competitions
        r = trading.betting.list_events(filter=competition_filter)

        # for i in r:
        #     print(i.event.id, i.event.name, i.event.venue, i.event.country_code, i.event.time_zone, i.event.open_date, i.market_count,
        #           i.elapsed_time)

        events_list = []
        for i in r:
            # print(i.event.id, i.event.name, i.event.venue, i.event.country_code, i.event.time_zone, i.market_count, i.elapsed_time)
            events_list += [{'event_id': i.event.id,
                             'event_name': i.event.name,
                             'event_venue': i.event.venue,
                             'event_country_code': i.event.country_code,
                             'event_timezone': i.event.time_zone,
                             'event_open_date': i.event.open_date,
                             'event_market_count': i.market_count,
                             'event_elapsed_time': i.elapsed_time}]

        return events_list


    def get_events_from_betfair_by_competition_id_add_to_db(self, trading, connect, event_type, competition_id):

        # Create a competition filter
        competition_filter = betfairlightweight.filters.market_filter(competition_ids=[competition_id])

        # Get a list of competitions
        r = trading.betting.list_events(filter=competition_filter)

        for i in r:
            print('\n', i.event.id, i.event.name, i.event.venue, i.event.country_code, i.event.time_zone, i.event.open_date, i.market_count, i.elapsed_time)
            connect.insert_betfair_event(i.event.id, event_type, competition_id, i.event.name, i.event.open_date)
            print('\tdb updated')

    def get_events_from_betfair_by_event_id_add_to_db(self, event_id):

        # Create a competition filter
        competition_filter = betfairlightweight.filters.market_filter(event_ids=[event_id])

        # Get a list of competitions
        r = trading.betting.list_events(filter=competition_filter)

        for i in r:
            print('\n', i.event.id, i.event.name, i.event.venue, i.event.country_code, i.event.time_zone, i.event.open_date, i.market_count, i.elapsed_time)
            # connect.insert_betfair_event(i.event.id, event_type, competition_id, i.event.name, i.event.open_date)
            # print('\tdb updated')

    def get_all_events_from_db(self):
        r = connect.select_all_events()
        for i in r:
            print(i)


class MarketTypes:

    def get_market_types_from_betfair_by_event_id(self, trading, event_id):

        # Create a competition filter
        competition_filter = betfairlightweight.filters.market_filter(event_ids=[event_id])

        # Get a list of competitions
        r = trading.betting.list_market_types(filter=competition_filter)

        for i in r:
            print(i, i.market_type, '-', i.market_count)




class MarketCatalogue:

    def get_market_catalogue_from_betfair_by_event_id(self, trading, event_id, market=None):

        # Create a competition filter
        if market == None:
            competition_filter = betfairlightweight.filters.market_filter(event_ids=[event_id])
        else:
            competition_filter=betfairlightweight.filters.market_filter(event_ids=[event_id],
                market_type_codes=[market],  # type of the market you want to get prices for
            )


        # Get a list of competitions
        r = trading.betting.list_market_catalogue(filter=competition_filter, max_results='100', sort='FIRST_TO_START',
        market_projection=[
            "MARKET_START_TIME",
            "RUNNER_DESCRIPTION",
        ])  # runner description required)


        market_catalogue_dict = {}

        for i in r:
            # print('id: {} - {}: matched: {:,.0f} - {} - {} - {} - {}'.format(i.market_id, i.market_name, float(i.total_matched), i.event_type, i.competition, i.description, i.market_start_time))
            # print(f'id: {i.market_id} - {i.market_name}: matched: {float(i.total_matched):,.0f} - {i.event_type} - {i.competition} - {i.description} - {i.market_start_time}')

            market_catalogue_dict[i.market_name] = {'market_id': i.market_id,
                                                    'matched': f'float(i.total_matched):,.0f',
                                                    'event_tpye': i.event_type,
                                                    'competition': i.competition,
                                                    'description': i.description,
                                                    'market_start_time': i.market_start_time,
                                                    'runners': []
                                                    }

            for runner in i.runners:
                market_catalogue_dict[i.market_name]['runners'] += [{'runner_id': runner.selection_id,
                                                                    'runner_name': runner.runner_name,
                                                                    'handicap': runner.handicap}]


        return market_catalogue_dict


    # TODO - get runners by market id


    def get_market_catalogue_match_odds_correct_score_underovers_by_event_id(self, trading, event_id):

        # Create a competition filter
        competition_filter = betfairlightweight.filters.market_filter(event_ids=[event_id])

        # Get a list of competitions
        r = trading.betting.list_market_catalogue(filter=competition_filter, max_results='100',
                                                  sort='FIRST_TO_START',
                                                  market_projection=[
                                                      "MARKET_START_TIME",
                                                      "RUNNER_DESCRIPTION",
                                                  ])  # runner description required)

        self.market_ids = []
        self.match_odd_runners = {}
        self.correct_score_runners = {}
        self.under_over_2p5_runners = {}

        for i in r:
            # print('id: {} - {}: matched: {:,.0f} - {} - {} - {} - {}'.format(i.market_id, i.market_name,
            #                                                                  float(i.total_matched), i.event_type,
            #                                                                  i.competition, i.description,
            #                                                                  i.market_start_time))

            if i.market_name == 'Match Odds':
                # print('\tMATCH ODDS')
                self.market_ids += [i.market_id]
                for x in i.runners:
                    # print('\t', x.selection_id, x.runner_name, x.handicap)
                    self.match_odd_runners[x.selection_id] = x.runner_name

            elif i.market_name == 'Correct Score':
                # print('\tCORRECT SCORE')
                self.market_ids += [i.market_id]
                for x in i.runners:
                    # print('\t', x.selection_id, x.runner_name, x.handicap)
                    self.correct_score_runners[x.selection_id] = x.runner_name

            elif i.market_name == 'Over/Under 2.5 Goals':
                # print('\tOVER UNDER 2.5')
                self.market_ids += [i.market_id]
                for x in i.runners:
                    # print('\t', x.selection_id, x.runner_name, x.handicap)
                    self.under_over_2p5_runners[x.selection_id] = x.runner_name

        print('\nmatch_odd_runners')
        for i in self.match_odd_runners:
            print('\t', i, self.match_odd_runners[i])

        print('\ncorrect_score_runners')
        for i in self.correct_score_runners:
            print('\t', i, self.correct_score_runners[i])

        print('\nunder_over_2p5_runners')
        for i in self.under_over_2p5_runners:
            print('\t', i, self.under_over_2p5_runners[i])


class MarketBook:

    def get_market_book_by_market_id(self, trading, market_id):
        # Create a price filter. Get all traded and offer data

        '''
            - EX_BEST_OFFERS = Only the best prices available for each runner, to requested price depth.
            - EX_ALL_OFFERS = EX_ALL_OFFERS trumps EX_BEST_OFFERS if both settings are  present
            - EX_TRADED = Amount traded on the exchange.
        '''

        price_filter = betfairlightweight.filters.price_projection(price_data=['EX_BEST_OFFERS'])

        # Request market books
        r = trading.betting.list_market_book(market_ids=[market_id], price_projection=price_filter)

        for i in r:
            for x in i.runners:
                print(x.selection_id, x.total_matched, x.orders, x.status, x.matches, x.last_price_traded, x.matches_by_strategy, x.sp, x.ex.available_to_back[0], x.ex.available_to_lay[0], x.ex.traded_volume)
            #     print('{} - {} - {} - {} - {} - {} - {} - {} - {}'.format(i.runners, i.status, i.complete, i.inplay, i.key_line_description, i.last_match_time, i.market_definition, i.total_available, i.total_matched))

        print('\n###########################\n')

        # Create a price filter. Get all traded and offer data
        price_filter = betfairlightweight.filters.price_projection(
            price_data=['EX_BEST_OFFERS']
        )

        # Request market books
        market_books = trading.betting.list_market_book(
            market_ids=[market_id],
            price_projection=price_filter
        )

        # Grab the first market book from the returned list as we only requested one market
        market_book = market_books[0]

        runners_df = self.process_runner_books(market_book.runners)
        print(runners_df)

    def process_runner_books(self, runner_books):
        '''
        This function processes the runner books and returns a DataFrame with the best back/lay prices + vol for each runner
        :param runner_books:
        :return:
        '''
        best_back_prices = [runner_book.ex.available_to_back[0].price
                            if runner_book.ex.available_to_back[0].price
                            else 1.01
                            for runner_book
                            in runner_books]
        best_back_sizes = [runner_book.ex.available_to_back[0].size
                           if runner_book.ex.available_to_back[0].size
                           else 1.01
                           for runner_book
                           in runner_books]

        best_lay_prices = [runner_book.ex.available_to_lay[0].price
                           if runner_book.ex.available_to_lay[0].price
                           else 1000.0
                           for runner_book
                           in runner_books]
        best_lay_sizes = [runner_book.ex.available_to_lay[0].size
                          if runner_book.ex.available_to_lay[0].size
                          else 1.01
                          for runner_book
                          in runner_books]

        selection_ids = [runner_book.selection_id for runner_book in runner_books]
        last_prices_traded = [runner_book.last_price_traded for runner_book in runner_books]
        total_matched = [runner_book.total_matched for runner_book in runner_books]
        statuses = [runner_book.status for runner_book in runner_books]
        scratching_datetimes = [runner_book.removal_date for runner_book in runner_books]
        adjustment_factors = [runner_book.adjustment_factor for runner_book in runner_books]

        df = pd.DataFrame({
            'Selection ID': selection_ids,
            'Best Back Price': best_back_prices,
            'Best Back Size': best_back_sizes,
            'Best Lay Price': best_lay_prices,
            'Best Lay Size': best_lay_sizes,
            'Last Price Traded': last_prices_traded,
            'Total Matched': total_matched,
            'Status': statuses,
            'Removal Date': scratching_datetimes,
            'Adjustment Factor': adjustment_factors
        })
        return df

    def get_market_book_by_event_ids(self, trading, event_ids, match_odd_runners, correct_score_runners, under_over_2p5_runners):
        # Create a price filter. Get all traded and offer data

        '''
            - EX_BEST_OFFERS = Only the best prices available for each runner, to requested price depth.
            - EX_ALL_OFFERS = EX_ALL_OFFERS trumps EX_BEST_OFFERS if both settings are  present
            - EX_TRADED = Amount traded on the exchange.
        '''

        price_filter = betfairlightweight.filters.price_projection(price_data=['EX_BEST_OFFERS'])

        # Request market books
        r = trading.betting.list_market_book(market_ids=event_ids, price_projection=price_filter)

        for i in r:
            for x in i.runners:

                if x.selection_id in match_odd_runners:
                    print('match_odd_runner:', x.selection_id, match_odd_runners[x.selection_id], x.total_matched, x.orders, x.status, x.matches, x.last_price_traded, x.matches_by_strategy, x.sp, x.ex.available_to_back[0], x.ex.available_to_lay[0], x.ex.traded_volume)
                elif x.selection_id in correct_score_runners:
                    print('correct_score_runner:', x.selection_id, correct_score_runners[x.selection_id], x.total_matched, x.orders, x.status, x.matches, x.last_price_traded, x.matches_by_strategy, x.sp, x.ex.available_to_back[0], x.ex.available_to_lay[0], x.ex.traded_volume)
                elif x.selection_id in under_over_2p5_runners:
                    print('under_over_2p5_runner:', x.selection_id, under_over_2p5_runners[x.selection_id], x.total_matched, x.orders, x.status, x.matches, x.last_price_traded, x.matches_by_strategy, x.sp, x.ex.available_to_back[0], x.ex.available_to_lay[0], x.ex.traded_volume)
            #     print('{} - {} - {} - {} - {} - {} - {} - {} - {}'.format(i.runners, i.status, i.complete, i.inplay, i.key_line_description, i.last_match_time, i.market_definition, i.total_available, i.total_matched))

        print('\n###########################\n')




# init
# connect = DBMethods()
trading = betfair_login()

'''
Event Types
'''
# app = EventTypes()
# app.get_event_types_from_betfair(trading) # Only need to run this once to create betfair_event_types.json
# app.add_event_types_to_db(trading, connect)
# app.get_event_types_from_db(connect)

betfair_event_types_dict = json_file_to_dict('betfair_event_types')
# print(d)

'''
Competition
'''
sport = 'Soccer' # event type id = 1
event_type_id = betfair_event_types_dict[sport]  # Football
# app = Competitions()
# app.get_competition_from_betfair_by_event_type(trading, event_type_id)  # Only need to run this once to create competitions_in_event_type_1.json (FOR FOOTBALL
# app.add_competion_from_betfair_to_db_by_event_type(trading, connect, event_type)
# app.get_competitions_from_db_by_event_type(connect, event_type)

betfair_competitions_dict = json_file_to_dict(f'competitions_in_event_type_{event_type_id}')
# print(betfair_competitions_dict)
# for competition in betfair_competitions_dict:
#     print(competition)

# competition_name = 'English Premier League'
competition_name = 'UEFA Europa League'
# competition_name = 'UEFA - Champions League'
competition_id = betfair_competitions_dict[competition_name]

'''
Events
'''
# competition_id = 10932509  # Premier League
# events_app = Events()
# events_list = events_app.get_events_from_betfair_by_event_type(trading, event_type_id) # NOT PREFERRED
# events_list = events_app.get_events_from_betfair_by_competition_id(trading, competition_id) # PREFERRED

# for event in events_list:
#     print (event)

event_id = '31895077'  # Barca V Man U

# events_app.get_events_from_betfair_by_competition_id_add_to_db(trading, connect, event_type, competition_id)
# events_app.get_all_events_from_db()

'''
Market Types - lists the market_type and market_count for an event id - NOT GREATLY USEFUL
'''
# event_id = 30013741  # Brighton v Man U
# app = MarketTypes()
# app.get_market_types_from_betfair_by_event_id(trading, event_id)


'''
Market Catalogue
'''
# event_id = 30013741  # Brighton v Man U
# market_catalogue_app = MarketCatalogue()
# market = "MATCH_ODDS"
# market_catalogue = market_catalogue_app.get_market_catalogue_from_betfair_by_event_id(trading, event_id, market)
# print(market_catalogue)
# for market in market_catalogue:
#     print(market, '-', market_catalogue[market]['market_id'])

# market_catalogue_app.get_market_catalogue_match_odds_correct_score_underovers_by_event_id(trading, event_id)

'''
Market Book
'''
market_id = 1.206250862  # match odds
# market_book_app = MarketBook()
# print(market_catalogue_app.market_ids)
# market_book_app.get_market_book_by_market_id(trading, market_id)
# market_book_app.get_market_book_by_event_ids(trading, market_catalogue_app.market_ids, market_catalogue_app.match_odd_runners, market_catalogue_app.correct_score_runners, market_catalogue_app.under_over_2p5_runners)


def process_runner_books(runner_books):
    '''
    This function processes the runner books and returns a DataFrame with the best back/lay prices + vol for each runner
    :param runner_books:
    :return:
    '''
    best_back_prices = [runner_book.ex.available_to_back[0].price
        if runner_book.ex.available_to_back.price
        else 1.01
        for runner_book
        in runner_books]
    best_back_sizes = [runner_book.ex.available_to_back[0].size
        if runner_book.ex.available_to_back.size
        else 1.01
        for runner_book
        in runner_books]

    best_lay_prices = [runner_book.ex.available_to_lay[0].price
        if runner_book.ex.available_to_lay.price
        else 1000.0
        for runner_book
        in runner_books]
    best_lay_sizes = [runner_book.ex.available_to_lay[0].size
        if runner_book.ex.available_to_lay.size
        else 1.01
        for runner_book
        in runner_books]

    selection_ids = [runner_book.selection_id for runner_book in runner_books]
    last_prices_traded = [runner_book.last_price_traded for runner_book in runner_books]
    total_matched = [runner_book.total_matched for runner_book in runner_books]
    statuses = [runner_book.status for runner_book in runner_books]
    scratching_datetimes = [runner_book.removal_date for runner_book in runner_books]
    adjustment_factors = [runner_book.adjustment_factor for runner_book in runner_books]

    df = pd.DataFrame({
        'Selection ID': selection_ids,
        'Best Back Price': best_back_prices,
        'Best Back Size': best_back_sizes,
        'Best Lay Price': best_lay_prices,
        'Best Lay Size': best_lay_sizes,
        'Last Price Traded': last_prices_traded,
        'Total Matched': total_matched,
        'Status': statuses,
        'Removal Date': scratching_datetimes,
        'Adjustment Factor': adjustment_factors
    })
    return df

def get_current_prices():
    # Create a price filter. Get all traded and offer data
    price_filter = betfairlightweight.filters.price_projection(
        price_data=['EX_BEST_OFFERS']
    )

    # Request market books
    market_books = trading.betting.list_market_book(
        market_ids=['1.206250862'],
        price_projection=price_filter
    )

    # extract the current prices from the market book
    for market_book in market_books:
        # process_runner_books(market_book)
        for runner in market_book.runners:
            print(f"selection_id: {runner.selection_id}, "
                  f"last_price_traded: {runner.last_price_traded}, "
                  f"total_matched: {runner.total_matched}, "
                  f"price available_to_back[0]: {runner.ex.available_to_back[0].price}, "
                  f"size available_to_back[0]: {runner.ex.available_to_back[0].size}, "
                  f"price available_to_back[1]: {runner.ex.available_to_back[1].price}, "
                  f"size available_to_back[1]: {runner.ex.available_to_back[1].size}, "
                  f"price available_to_back[2]: {runner.ex.available_to_back[2].price}, "
                  f"size available_to_back[2]: {runner.ex.available_to_back[2].size}, "
                  f"status: {runner.status}, "
                  f"sp: {runner.sp}, "
                  f"traded_volume: {runner.ex.traded_volume}, "
                  f"price available_to_lay[0]: {runner.ex.available_to_lay[0].price}, "
                  f"size available_to_lay[0]: {runner.ex.available_to_lay[0].size}, "
                  f"price available_to_lay[1]: {runner.ex.available_to_lay[1].price}, "
                  f"size available_to_lay[1]: {runner.ex.available_to_lay[1].size}, "
                  f"price available_to_lay[2]: {runner.ex.available_to_lay[2].price}, "
                  f"size available_to_lay[2]: {runner.ex.available_to_lay[2].size} ")
            # print(runner.ex.available_to_back.price)
            # print('\n')

    # runners_df = process_runner_books(market_book.runners)
    #
    # runners_df



# TODO - need to update it so it has more logical like the process_runner_books
get_current_prices()

# logout
trading.logout()
# connect.close_connection()
