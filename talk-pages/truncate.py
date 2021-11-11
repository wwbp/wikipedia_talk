import argparse
import fugashi
import jieba
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
	opts.add_argument('tbl')
	args = opts.parse_args()
	return args


def db_connect(db):
	con = create_engine('mysql://127.0.0.1/{db}?read_default_file=~/.my.cnf&charset=utf8mb4'.format(db=db))
	return con


def tokenize(text, lang):
	if lang == 'en' or lang == 'es':
		return word_tokenize(text)
	if lang == 'zh':
		return jieba.cut(text)
	if lang == 'ja':
		return [w.surface for w in fugashi.Tagger()(text)]
	return None


def tokenized_pages(tbl, con):
	df = pd.read_sql(tbl, con)
	df['tokenized'] = df.parallel_apply(lambda r: list(tokenize(r['message'], r['lang'])), axis=1)
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

	msgs = tokenized_pages(args.tbl, con)
	pages = msgs['unified_id'].drop_duplicates().to_numpy()
	truncated_df = pd.concat([unify_length(page, msgs) for page in tqdm(pages, desc='Unifying page lengths')])
	truncated_df.to_sql('{}_trunc'.format(args.tbl), con, index=False, if_exists='replace')


if __name__ == '__main__':
	main()
