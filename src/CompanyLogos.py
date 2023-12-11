from benzinga import financial_data


api_key = '98948443179a47d78e2e96fe64900a25'
fin = financial_data.Benzinga(api_key)

aapl_logo = fin.logos('AAPL')
print(aapl_logo)