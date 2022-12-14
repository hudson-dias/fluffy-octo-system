import pandas as pd
import re

# create a sample dataframe with name, address, and document number
df = pd.DataFrame({'name': ['John Smith', 'Jane Doe'],
                   'address': ['123 Main St.', '456 Park Ave.'],
                   'document number': ['123-45-6789', '987.65.4321']})

# print the original dataframe
print(df)

# remove special characters and accents from the name and address columns
df['name'] = df['name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
df['address'] = df['address'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

# remove dots and dashes from the document number column using regular expressions
df['document number'] = df['document number'].apply(lambda x: re.sub(r'[.-]', '', x))

# print the modified dataframe
print(df)
