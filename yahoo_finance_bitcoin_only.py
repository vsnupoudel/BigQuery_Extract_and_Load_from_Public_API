#!/usr/bin/env python3.92 - 3.95

# modified by = "replytobishnu@gmail.com"

# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def load_table_dataframe(table_id= None):

    from google.cloud import bigquery
    import yfinance as yf
    from datetime import datetime, timedelta

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    if not table_id:
        table_id = "proud-sweep-342309.yahoofinance.bitcoin"
    # Get data for 4 hours, Run this script every 4 hours
    startdate = datetime.today() - timedelta(hours=4)

    
    dataframe = yf.download("BTC-USD", 
        start = startdate, end=datetime.today(), interval = "1m",
        # group_by = 'ticker',
        # adjust all OHLC automatically
        # (optional, default is False)
        auto_adjust = True,
        # download pre/post regular market hours data
        # (optional, default is False)
        prepost = True,
        # use threads for mass downloading? (True/False/Integer)
        # (optional, default is True)
        threads = True)
    # Move the DateTime column from index
    dataframe.reset_index(inplace=True)
    

    job = client.load_table_from_dataframe(
        dataframe, table_id
        #, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

    #return table

if __name__=="__main__":
    import sys
    table_id = sys.argv[1]
    if table_id:
        print(table_id)
        load_table_dataframe(table_id)
    else:
        load_table_dataframe()