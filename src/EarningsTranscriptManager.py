import os
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


def scrape_earnings_transcript(earnings_ticker):
    # Construct the URL with the dynamic ticker
    url = f"https://www.fool.com/quote/nasdaq/{earnings_ticker.lower()}/#quote-earnings-transcripts"

    # Send an HTTP GET request to the URL
    response = requests.get(url)
    response.raise_for_status()  # Check for any errors in the request

    # Parse the HTML content
    earnings_soup = BeautifulSoup(response.content, "html.parser")

    # Find the div with ID "earnings-transcript-container"
    transcript_container = earnings_soup.find("div", id="earnings-transcript-container")

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


def write_transcript_to_file(transcript_ticker, transcript_dict):

    # Define the directory path and filename
    directory = f"../data/Earnings/Transcripts/{transcript_ticker}"
    os.makedirs(directory, exist_ok=True)
    filename = f"Q{transcript_dict['quarter']}_{transcript_dict['year']}_Transcript.json"
    filepath = os.path.join(directory, filename)

    # Write the transcript data to the file
    with open(filepath, "w") as file:
        json.dump(transcript_dict['transcript'], file)


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


def get_sentiment_of_transcript(transcript):
    return 1


def get_compiled_transcript(transcript):

    compiled = []
    for _, val in transcript.items():
        compiled += val

    return compiled


if __name__ == '__main__':

    nltk.download('stopwords')  # Download the stopwords corpus if not already downloaded

    ticker = "AMZN"

    earnings_transcript = scrape_earnings_transcript(ticker)
    write_transcript_to_file(ticker, earnings_transcript)

    compiled_transcript = get_compiled_transcript(earnings_transcript['transcript'])
    transcript_summary = get_summary_of_transcript(earnings_transcript['transcript'])
    transcript_sentiment = get_sentiment_of_transcript(earnings_transcript['transcript'])

    qsm = QuordataSqlManager()
    qsm.add_earnings_call(earnings_transcript['quarter'], earnings_transcript['year'],
                          json.dumps(compiled_transcript), transcript_summary,
                          transcript_sentiment, company_ticker=ticker)
