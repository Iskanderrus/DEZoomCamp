import pandas as pd

df = pd.read_csv('action.csv', low_memory=True)
df['popularity'] = df.loc[:, 'popularity'].apply(lambda x: x.strip('.')).apply(lambda x: x.replace(',', ''))
df[['title', 'episode', 'age', 'duration', 'genre', 'votes', 'description']] = df.loc[:,
                                                                               ['title', 'episode', 'age', 'duration',
                                                                                'genre', 'votes', 'description']].apply(
    lambda x: x.str.replace('\n', '')).apply(lambda x: x.str.strip())

df['year'] = df['year'].str.extract(r'(\d{4}?)', expand=False)
df['episode_year'] = df['episode_year'].str.extract(r'(\d{4}?)', expand=False)
df['duration'] = df.loc[:, 'duration'].astype(str).apply(lambda x: x.split(' ')[0]).apply(
    lambda x: x.replace(',', '')).astype(float)
df['votes'] = df.loc[:, 'votes'].astype(str).apply(lambda x: x.replace(',', ''))
df['votes'] = pd.to_numeric(df['votes'], downcast='float', errors='coerce')
df['votes'].fillna(0, inplace=True)
df['votes'] = pd.to_numeric(df['votes'], downcast='integer')
df['popularity'] = pd.to_numeric(df['popularity'], downcast='integer')
df = df[df['year'].notna()]
df['year'] = pd.to_numeric(df['year'], downcast='integer')
df['episode_year'] = pd.to_numeric(df['episode_year'], downcast='float')
df['rating'].fillna('Not Rated', inplace=True)
df['episode'].fillna('No Episodes', inplace=True)
df['age'].fillna('Not Rated', inplace=True)
df['duration'] = df['duration'].fillna(0)
df['duration'] = pd.to_numeric(df['duration'], downcast='integer')
df['description'].fillna('No description', inplace=True)
