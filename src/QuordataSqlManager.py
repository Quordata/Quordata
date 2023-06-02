import mysql.connector
import os
import numpy as np


class QuordataSqlManager:

    def __init__(self):

        # Replace the placeholder values with your actual database credentials
        host = '192.232.218.154'
        #username = 'saddlnts_bcollin'
        #password = 'r^isHk6xFL4G2pEoZb9k'
        username = 'saddlnts'
        password = 'o*#nD*eLA2QoJ!N8VajE'
        database = 'saddlnts_quordata'

        # Connect to the MySQL database
        self.connection = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
            database=database
        )

    def update_companies_table(self, company_data):
        cursor = self.connection.cursor()

        # Retrieve the existing tickers from the companies table
        query = "SELECT ticker FROM companies"
        cursor.execute(query)
        existing_tickers = set([ticker[0] for ticker in cursor.fetchall()])

        # Filter the rows in the input dictionary based on existing tickers
        filtered_data = {ticker: data for ticker, data in company_data.items() if ticker not in existing_tickers}

        # Iterate over the filtered data and insert it into the companies table
        for ticker, data in filtered_data.items():

            # Get the shortest name and longest name, as YF lies to you
            shortname, longname = sorted([data.get('longName', ''), data.get('shortName', '')], key=len)

            name = longname
            industry = data.get('industry', '').replace(',', '')
            sector = data.get('sector', '').replace(',', '')

            if industry:
                industry += ',' + sector
            else:
                industry += sector

            alias_names = shortname.replace(',', '')

            # If longName and shortName are equal, attempt to shorten shortName by removing the words after the
            # first space
            if name == alias_names:
                alias_names_split = alias_names.split()
                if len(alias_names_split) > 1:
                    alias_names = alias_names_split[0]

            exchange = data.get('exchange', '')

            # Prepare the SQL query
            query = "INSERT INTO companies (name, ticker, industry, alias_names, exchange) VALUES " \
                    "(%s, %s, %s, %s, %s)"
            values = (name, ticker, industry, alias_names, exchange)

            # Execute the query
            cursor.execute(query, values)

        # Commit the changes and close the cursor
        self.connection.commit()
        cursor.close()

    def add_earnings_call(self, quarter, year, transcript, transcript_summary, transcript_sentiment,
                          company_name=None, company_ticker=None):
        # Check if at least one of the company name or ticker is provided
        if not company_name and not company_ticker:
            raise ValueError("At least one of company_name or company_ticker must be provided.")

        # Retrieve the company_id based on the provided company name or ticker
        company_id = None

        if company_name:
            query = "SELECT company_id FROM companies WHERE name = %s"
            cursor = self.connection.cursor()
            cursor.execute(query, (company_name,))
            result = cursor.fetchone()
            cursor.close()

            if result:
                company_id = result[0]

        # Only get if it is not retrieved from the name. Name should take precedence, as some companies may share a
        # ticker
        if company_ticker and not company_id:
            query = "SELECT company_id FROM companies WHERE ticker = %s"
            cursor = self.connection.cursor()
            cursor.execute(query, (company_ticker,))
            result = cursor.fetchone()
            cursor.close()

            if result:
                company_id = result[0]

        if not company_id:
            raise ValueError("Invalid company_name or company_ticker provided.")

        # Insert the earnings call entry into the table
        query = "INSERT INTO earnings_calls (company_id, quarter, year, transcript, summary, sentiment) " \
                "VALUES (%s, %s, %s, %s, %s, %s)"
        values = (company_id, quarter, year, transcript, transcript_summary, transcript_sentiment)

        cursor = self.connection.cursor()
        cursor.execute(query, values)
        self.connection.commit()
        cursor.close()

    def update_stock_data(self, dataframe, file_path='/home4/saddlnts/public_html/temp_data.csv'):
        # Reset the index to convert the multi-index columns to separate columns
        dataframe = dataframe.reset_index()

        # Replace NaN values with 0
        dataframe = dataframe.fillna(0)

        # Save the dataframe to a temporary CSV file
        #dataframe.to_csv(file_path, index=False)

        # Get the columns for Adj Close and Volume
        adj_close_columns = dataframe['Adj Close'].columns
        volume_columns = dataframe['Volume'].columns

        # Get the tickers from the columns
        tickers = adj_close_columns.tolist() + volume_columns.tolist()

        # Create a cursor to execute SQL queries
        cursor = self.connection.cursor()

        for ticker in tickers:
            # Get the company_id from the companies table
            query = "SELECT company_id FROM companies WHERE ticker = %s"
            cursor.execute(query, (ticker,))
            result = cursor.fetchone()

            if result is None:
                continue

            company_id = result[0]

            # Determine whether it's Adj Close or Volume
            if ticker in adj_close_columns:
                column_name = 'Adj Close'
            else:
                column_name = 'Volume'

            # Set up the LOAD DATA LOCAL INFILE query
            query = """
            LOAD DATA LOCAL INFILE '{}'
            INTO TABLE stock_data
            FIELDS TERMINATED BY ','
            IGNORE 1 LINES
            (@date, @value)
            SET company_id = '{}', timestamp = STR_TO_DATE(@date, '%Y-%m-%d'), `price` = NULLIF(@value, '')
            """.format(file_path, company_id)

            if column_name == 'Volume':
                query = query.replace('price', 'volume')

            # Execute the LOAD DATA LOCAL INFILE query
            cursor.execute(query)

        # Commit the changes and close the cursor
        self.connection.commit()
        cursor.close()

        # Remove the temporary CSV file
        os.remove(file_path)
