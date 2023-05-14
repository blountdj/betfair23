
from pprint import pprint
import logging

import betfairlightweight

"""
Historic is the API endpoint that can be used to
download data betfair provide.
https://historicdata.betfair.com/#/apidocs
"""

# setup logging
logging.basicConfig(level=logging.INFO)  # change to DEBUG to see log all updates

def betfair_login():
    delayed_app_key = 'KaiQJ11LhakrDsh5'
    live_app_key = 'wsuhoFnT0LAokHC9'
    app_key = delayed_app_key
    UserName = 'darrenblount'
    Password = '76N[<z}/6bsWw^c'

    trading = betfairlightweight.APIClient(UserName, Password, app_key=app_key, certs=r"C:\certs")

    trading.login()
    return trading

trading = betfair_login()


# get my data
def list_all_my_historic_data_on_betfair(trading):
    my_data = trading.historic.get_my_data()
    for i in my_data:
        print(i)

# list_all_my_historic_data_on_betfair()


# get collection options (allows filtering)
# collection_options = trading.historic.get_collection_options(
#     "Soccer", "Basic Plan", 1, 1, 2023, 1, 1, 2023
# )
# pprint(collection_options)


# get advance basket data size
# basket_size = trading.historic.get_data_size(
#     "Soccer", "Basic Plan", 1, 1, 2023, 1, 1, 2023
# )
# print(basket_size)

# get file list
file_list = trading.historic.get_file_list(
    "Soccer",
    "Basic Plan",
    from_day=1,
    from_month=1,
    from_year=2023,
    to_day=31,
    to_month=1,
    to_year=2023,
    market_types_collection=["MATCH_ODDS"],
    countries_collection=["GB"],
    file_type_collection=["M"],
)
# pprint(file_list)

# download the files
for file in file_list:
    print(file)
    download = trading.historic.download_file(file_path=file)
    print(download)
