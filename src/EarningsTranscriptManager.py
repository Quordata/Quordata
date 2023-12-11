import os
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import re
from collections import defaultdict
from QuordataSqlManager import QuordataSqlManager
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.luhn import LuhnSummarizer
import nltk


class EarningsTranscriptManager:

    def __init__(self):
        self.bad_tickers = set()

    @staticmethod
    def scrape_earnings_transcript(earnings_ticker):
        headers = {
            "Cache-Control": "max-age=0",
            "Cookie": "OptanonAlertBoxClosed=2023-05-30T06:04:24.193Z; sessionid=bvbeyb8gq479aiox523rvgpe3ezistrt; __cf_bm=I6XqR0hxPyNF9DPsVwxsVjChevRe8xrHHZMBUTooq7U-1688872537-0-AX+Gt1hlhmQPIOdb1rj+f3eQ5SMnf2N3obogfRbeUaIeGWHHHGT9yseKLB3Z4hXRTg==; Visit=visit=cdc6fc8e-f6df-419a-a558-d454fc3f27e1&first_article_in_session=0&first_marketing_page=0; Visitor=uid=&username=&account=&registered=false&ecapped=false&dskPrf=false&version=7&visits=4&visitor=077fa7e5-ea59-4d1c-89aa-ee5f83810100; ct=1; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Jul+08+2023+20%3A39%3A52+GMT-0700+(Pacific+Daylight+Time)&version=202303.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=ccd53e14-642d-4626-ae58-45b0525de4e6&interactionCount=2&landingPath=NotLandingPage&groups=C0003%3A1%2CC0001%3A1%2CC0004%3A1%2CC0002%3A1%2CSPD_BG%3A1&AwaitingReconsent=false&geolocation=US%3BCA",
            "Sec-Ch-Ua": "\"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }

        # Construct the URL with the dynamic ticker
        url = f"https://www.fool.com/quote/nasdaq/{earnings_ticker.lower()}/#quote-earnings-transcripts"

        # Send an HTTP GET request to the URL
        response = requests.get(url, headers=headers)

        if not response.ok:
            url = f"https://www.fool.com/quote/nyse/{earnings_ticker.lower()}/#quote-earnings-transcripts"
            response = requests.get(url, headers=headers)

        if not response.ok:
            url = f"https://www.fool.com/quote/nysemkt/{earnings_ticker.lower()}/#quote-earnings-transcripts"
            response = requests.get(url, headers=headers)

        if response.status_code == 404:
            return {}

        # Parse the HTML content
        earnings_soup = BeautifulSoup(response.content, "html.parser")

        # Find the div with ID "earnings-transcript-container"
        transcript_container = earnings_soup.find("div", id="earnings-transcript-container")

        if transcript_container is None:
            return {}

        # Find the first URL for the transcript
        transcript_url = transcript_container.find("a")["href"]

        # Send an HTTP GET request to the transcript URL
        transcript_response = requests.get('https://www.fool.com' + transcript_url)
        transcript_response.raise_for_status()  # Check for any errors in the request

        # Parse the transcript HTML content
        transcript_soup = BeautifulSoup(transcript_response.content, "html.parser")

        # Find the article <div>
        article_div = transcript_soup.find("div", class_="tailwind-article-body")

        # Extract the quarter and year from the second <p> tag
        second_p_tag = article_div.find_all("p")[1]
        quarter_year_data = second_p_tag.get_text(strip=True)

        # Parse the quarter and year using regular expressions
        pattern = r"Q(\d+)[^\d]+(\d{4})"
        match = re.search(pattern, quarter_year_data)

        if not match and 'FY' in quarter_year_data:
            pattern = r"FY[^\d]+(\d{4})"
            match = re.search(pattern, quarter_year_data)

            quarter = '4'
            year = match.group(1)
        else:
            quarter = match.group(1)
            year = match.group(2)

        # Find all the <h2> tags
        h2_tags = article_div.find_all("h2")

        # Find the start and end index of the desired transcript section
        start_index = 0
        end_index = len(h2_tags) - 1

        # Skip the sections before the second <h2> tag
        if len(h2_tags) > 1:
            start_index = 1

        # Find the transcript section using the desired start and end index
        transcript_section = article_div.find_all("h2")[start_index:end_index]

        # Initialize the transcript data
        transcript = defaultdict(list)
        current_speaker = None
        current_text = ''

        # Process each element within the transcript section
        for element in transcript_section:
            section_name = element.text.replace(':', '').replace(' &', '')
            next_element = element.next_sibling
            while next_element:

                # Reached next header, break
                if next_element.name == "h2":
                    break

                # Skip over non-<p> elements
                if next_element.name != "p":
                    next_element = next_element.next_sibling
                    continue

                # Check if the <p> contains a speaker name
                if next_element.find("strong"):
                    # Check if there is any text to add to the transcript
                    if current_speaker and current_text:
                        transcript[section_name].append({"speaker": current_speaker, "text": current_text.strip()})

                    current_speaker = next_element.get_text(strip=True)
                    current_text = ""
                else:
                    # Get the text spoken by the current speaker
                    spoken_text = next_element.get_text(strip=True)

                    # Add the spoken text to the current text for the speaker
                    current_text += " " + spoken_text

                next_element = next_element.next_sibling

            # Add the last speaker and text
            transcript[section_name].append({"speaker": current_speaker, "text": current_text.strip()})

        # Create a dictionary with the quarter, year, and transcript
        result = {"quarter": quarter, "year": year, "transcript": transcript}

        # Return the result dictionary
        return result

    @staticmethod
    def write_transcript_to_file(transcript_ticker, transcript_dict):

        # Define the directory path and filename
        directory = f"../data/Earnings/Transcripts/{transcript_ticker}"
        os.makedirs(directory, exist_ok=True)
        filename = f"Q{transcript_dict['quarter']}_{transcript_dict['year']}_Transcript.json"
        filepath = os.path.join(directory, filename)

        # Write the transcript data to the file
        with open(filepath, "w") as file:
            json.dump(transcript_dict['transcript'], file)

    @staticmethod
    def get_summary_of_transcript(transcript):

        # Skip Questions & Answers (TODO support for later?)
        remarks_transcript = transcript['Prepared Remarks']
        remarks = ''

        for remark in remarks_transcript:

            # Don't summarize Operator remarks
            if remark['speaker'] == 'Operator':
                continue

            remarks += remark['text']

        # Summarize remarks
        parser = PlaintextParser.from_string(remarks, Tokenizer("english"))
        summarizer = LuhnSummarizer()

        '''summarizer.bonus_words = ["Revenue", "Profit", "Growth", "Expenses", "Trends", "Market", "Competition", "Guidance",
                                  "Performance", "Strategy"]
        summarizer.stigma_words = ["Increase", "Decrease", "Strong", "Weak", "Successful", "Challenges", "Opportunities",
                                   "Expansion", "Innovation", "Outlook"]
        summarizer.null_words = stopwords.words('english')'''

        # Adjust the number of sentences for the summary
        summarized_sentences = summarizer(parser.document, sentences_count=10)

        summary = []
        for sentence in summarized_sentences:
            summary.append(str(sentence))

        return ' '.join(summary)

    @staticmethod
    def get_sentiment_of_transcript(transcript):
        return 1

    @staticmethod
    def get_compiled_transcript(transcript):

        compiled = []
        for _, val in transcript.items():
            compiled += val

        return compiled

    def update_ticker_transcript(self, ticker):

        earnings_transcript = EarningsTranscriptManager.scrape_earnings_transcript(ticker)
        if not earnings_transcript:
            self.bad_tickers.add(ticker)
            return

        EarningsTranscriptManager.write_transcript_to_file(ticker, earnings_transcript)

        compiled_transcript = EarningsTranscriptManager.get_compiled_transcript(earnings_transcript['transcript'])
        transcript_summary = EarningsTranscriptManager.get_summary_of_transcript(earnings_transcript['transcript'])
        transcript_sentiment = EarningsTranscriptManager.get_sentiment_of_transcript(earnings_transcript['transcript'])

        qsm = QuordataSqlManager()
        result = qsm.add_earnings_call(earnings_transcript['quarter'], earnings_transcript['year'],
                                       json.dumps(compiled_transcript), transcript_summary,
                                       transcript_sentiment, company_ticker=ticker)
        if not result:
            self.bad_tickers.add(ticker)
        print(f'Bad tickers: {self.bad_tickers}')

    def update_tickers_transcript(self, tickers):
        for i, ticker in enumerate(tickers):
            print(f'Getting {ticker}, {i}/{len(tickers)}')
            self.update_ticker_transcript(ticker)
            time.sleep(1)

    def get_sp100_earnings(self):
        sp100df = pd.read_csv('../doc/sp-100.csv')
        sp100 = list(sp100df['Symbol'])

        self.update_tickers_transcript(sp100)

    def get_sp500_earnings(self):
        sp500df = pd.read_csv('../doc/sp-500.csv')
        sp500 = list(sp500df['Symbol'])

        self.update_tickers_transcript(sp500)


if __name__ == '__main__':

    nltk.download('stopwords')  # Download the stopwords corpus if not already downloaded

    etm = EarningsTranscriptManager()
    etm.update_ticker_transcript('JWN')
