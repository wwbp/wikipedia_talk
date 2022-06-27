import argparse
import warnings

import pandas as pd
from nltk.tokenize import word_tokenize
from pandarallel import pandarallel
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.types import CHAR, INTEGER, VARCHAR
from tqdm import tqdm

warnings.filterwarnings('ignore')
pandarallel.initialize(nb_workers=12, progress_bar=True)


def parse_args():
	opts = argparse.ArgumentParser()
	opts.add_argument('db')
	opts.add_argument('trunc_tbl', help='Name of the table to be created with truncated English texts')
	opts.add_argument('tbls', nargs='+', help='The names of the tables in each language, which contain English translations.')
	args = opts.parse_args()
	return args


def db_connect(db):
	con = create_engine('mysql://127.0.0.1/{db}?read_default_file=~/.my.cnf&charset=utf8mb4'.format(db=db))
	return con


def tokenized_pages(tbl, con):
	df = pd.read_sql(tbl, con)
	message_col = 'message_en'
	if message_col not in df.columns:  # it's the English table, so hasn't been translated
		message_col = 'message'
	df['tokenized'] = df.parallel_apply(lambda r: list(word_tokenize(r[message_col])), axis=1)
	df['length'] = df['tokenized'].apply(len)
	return df


def unify_length(unified_id, full_tokenized):
	df = full_tokenized.query('unified_id == {}'.format(unified_id))
	lengths = df['length'].to_numpy()
	min_length = min(lengths)
	if min_length == 0:
		return pd.DataFrame()
	df['message'] = df['tokenized'].apply(lambda toks: ' '.join(toks[:min_length]))
	df = df.drop(['tokenized', 'length', 'message_en'], axis=1)
	return df


def main():
	args = parse_args()
	con = db_connect(args.db)

	# Tokenize all the English pages
	msgs = pd.concat([tokenized_pages(tbl, con) for tbl in tqdm(args.tbls, desc='Tokenizing')])

	pages = msgs['unified_id'].drop_duplicates().to_numpy()
	truncated_df = pd.concat([unify_length(page, msgs) for page in tqdm(pages, desc='Unifying page lengths')])

	truncated_df.to_sql(args.trunc_tbl, con, index=False, if_exists='replace', chunksize=500, dtype={
			'unified_id': INTEGER,
			'message_wiki_id': INTEGER,
			'message': LONGTEXT,
			'lang': CHAR(2),
			'message_id': VARCHAR(126)
		})


if __name__ == '__main__':
	main()
