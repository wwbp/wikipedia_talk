import argparse
import pandas as pd
import warnings

from nltk.tokenize import word_tokenize
from pandarallel import pandarallel
from sqlalchemy import create_engine
from tqdm import tqdm


warnings.filterwarnings('ignore')
pandarallel.initialize(nb_workers=12, progress_bar=True)


def parse_args():
	opts = argparse.ArgumentParser()
	opts.add_argument('db')
	opts.add_argument('trunc_tbl', description='Name of the table to be created with truncated English texts')
	opts.add_argument('tbls', nargs='+', description='The names of the tables in each language, which contain English translations.')
	args = opts.parse_args()
	return args


def db_connect(db):
	con = create_engine('mysql://127.0.0.1/{db}?read_default_file=~/.my.cnf&charset=utf8mb4'.format(db=db))
	return con


def tokenized_pages(tbl, con):
	df = pd.read_sql(tbl, con)
	df['tokenized'] = df.parallel_apply(lambda r: list(word_tokenize(r['message_en'])), axis=1)
	df['length'] = df['tokenized'].apply(len)
	return df


def unify_length(unified_id, full_tokenized):
	df = full_tokenized.query('unified_id == {}'.format(unified_id))
	lengths = df['length'].to_numpy()
	min_length = min(lengths)
	if min_length == 0:
		return pd.DataFrame()
	df['message'] = df['tokenized'].apply(lambda toks: ' '.join(toks[:min_length]))
	df = df.drop(['tokenized', 'length'], axis=1)
	return df


def main():
	args = parse_args()
	con = db_connect(args.db)

	# Tokenize all the English pages
	msgs = pd.concat([tokenized_pages(tbl, con) for tbl in args.tbls])

	pages = msgs['unified_id'].drop_duplicates().to_numpy()
	truncated_df = pd.concat([unify_length(page, msgs) for page in tqdm(pages, desc='Unifying page lengths')])

	langs = msgs['lang'].drop_duplicates().to_numpy()
	for lang in langs:
		truncated_df.loc[truncated_df['lang'] == lang, :].to_sql('{}_trunc_{}'.format(args.trunc_tbl, lang), con, index=False, if_exists='replace')


if __name__ == '__main__':
	main()
