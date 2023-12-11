import mysql.connector
from mysql.connector import Error
import time


class QuordataSqlManager:

    def __init__(self):

        # Replace the placeholder values with your actual database credentials
        host = '192.232.218.154'
        username = 'saddlnts_bcollin'
        password = 'r^isHk6xFL4G2pEoZb9k'
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
            print(f'Invalid company_name {company_name} or company_ticker {company_ticker} provided.')
            return False

        # Insert the earnings call entry into the table
        query = "INSERT INTO earnings_calls (company_id, quarter, year, transcript, summary, sentiment) " \
                "VALUES (%s, %s, %s, %s, %s, %s)"
        values = (company_id, quarter, year, transcript, transcript_summary, transcript_sentiment)

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, values)
            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as error:
            if error.errno == mysql.connector.errorcode.ER_DUP_ENTRY:
                # Handle the duplicate entry error
                return False
            else:
                # Handle other database-related errors
                raise error

        return True

    def update_stock_data(self, dataframe, batch_size=100):

        # Replace NaN with 0
        dataframe.fillna(0, inplace=True)

        tickers = dataframe.columns.levels[1].tolist()
        cursor = self.connection.cursor()

        rows_to_insert = []  # List to store the rows to be inserted

        for date, row in dataframe.iterrows():
            timestamp = date.strftime('%Y-%m-%d')

            print(timestamp)

            for ticker in tickers:
                query = "SELECT company_id FROM companies WHERE ticker = %s"
                cursor.execute(query, (ticker,))
                result = cursor.fetchone()

                if result is None:
                    continue

                company_id = result[0]
                price = row[('Adj Close', ticker)]
                volume = row[('Volume', ticker)]

                rows_to_insert.append((company_id, timestamp, float(price), int(volume)))

            # Insert the rows in batches
            if len(rows_to_insert) >= batch_size:
                query = "INSERT INTO stock_data (company_id, timestamp, price, volume) VALUES (%s, %s, %s, %s)"
                cursor.executemany(query, rows_to_insert)
                rows_to_insert = []  # Reset the list

                start_time = time.time()

        # Insert any remaining rows
        if len(rows_to_insert) > 0:
            query = "INSERT INTO stock_data (company_id, timestamp, price, volume) VALUES (%s, %s, %s, %s)"
            cursor.executemany(query, rows_to_insert)

        # Commit the changes and close the cursor
        self.connection.commit()
        cursor.close()

    def get_companies(self):

        # Create a dictionary to store the results
        result_dict = {}

        cursor = self.connection.cursor()

        # Execute SELECT query
        query = "SELECT ticker, name, alias_names FROM companies"
        cursor.execute(query)

        # Process the retrieved data
        for ticker, name, alias_names in cursor:
            # Split the comma-separated string of alias_names into a list
            alias_list = alias_names.split(',')

            # Create the list to be associated with the ticker
            values_list = [name] + alias_list

            # Add the ticker and associated list to the result dictionary
            result_dict[ticker] = values_list

        # Close the cursor and database connection
        cursor.close()

        return result_dict
