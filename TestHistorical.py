import betfairlightweight
from betfairlightweight import filters,APIClient
from betfairlightweight import   StreamListener
from betfairlightweight.streaming.stream import MarketStream
import logging
import json
import queue
from betfairlightweight.streaming.betfairstream import HistoricalGeneratorStream  
from betfairlightweight.filters import (
    streaming_market_filter,
    streaming_market_data_filter,
)

class HistoricalListener(StreamListener):
    def _add_stream(self, unique_id, stream_type):
        if stream_type == 'marketSubscription':
            return HistoricalStream(self)

class HistoricalStream(MarketStream):
    # create custom listener and stream

    def __init__(self, listener):
        super(HistoricalStream, self).__init__(listener)
        with open('output.txt', 'w') as output:
            output.write('Time,MarketId,Status,Inplay,SelectionId,LastPriceTraded\n')

    def on_process(self, market_books):
        with open('output.txt', 'a') as output:
            for market_book in market_books:
                for runner in market_book.runners:

                    # how to get runner details from the market definition
                    market_def = market_book.market_definition
                    runners_dict = {(runner.selection_id, runner.handicap): runner for runner in market_def.runners}
                    runner_def = runners_dict.get(
                        (runner.selection_id, runner.handicap)
                    )

                    output.write('%s,%s,%s,%s,%s,%s\n' % (
                        market_book.publish_time, market_book.market_id, market_book.status, market_book.inplay,
                        runner.selection_id, runner.last_price_traded or ''
                    ))



def stream_historical_file():
    # setup logging
    logging.basicConfig(level=logging.INFO)
    # create listener
    listener = HistoricalListener(output_queue=queue.Queue() ,  max_latency=1e100 )
    
    # create trading instance (no need to put in correct details)
    trading = betfairlightweight.APIClient('username', 'password')
    # create historical stream, update directory to file location
    stream = trading.streaming.create_historical_generator_stream(
        file='1.207324924.bz2',
        listener=listener
    )

    # start stream
    stream.start( async_=True)
    while True:       
        if stream._running:
            
            market_book = stream.listener.output_queue.get()
            if market_book :
                    a=1

    return 


stream_historical_file()