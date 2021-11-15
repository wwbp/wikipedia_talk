import argparse
import pandas as pd

from googletrans import Translator
from sqlalchemy import create_engine
from time import sleep
from tqdm import tqdm


tqdm.pandas()


def parse_args():
	opts = argparse.ArgumentParser()
	opts.add_argument('db')
	opts.add_argument('lang')
	opts.add_argument('--table', default='msgs')
	opts.add_argument('--table-trunc', default='msgs_trunc')
	args = opts.parse_args()
	return args


def db_connect(db):
	con = create_engine('mysql://127.0.0.1/{}?read_default_file=~/.my.cnf&charset=utf8mb4'.format(db))
	return con


def documents(table, table_trunc, lang, con):
	sql = """SELECT *
			 FROM {tbl} t 
			 WHERE lang = '{lang}'
			 AND message_id IN (SELECT message_id FROM {trunc})"""
	sql = sql.format(tbl=table, lang=lang, trunc=table_trunc)
	df = pd.read_sql(sql, con)
	return df


def translate(doc, chunk_size=3000, attempt=0):
	translated = ''
	for i in range(0, len(doc), chunk_size):
		doc_piece = doc[i:i+chunk_size]
		sleep(3)
		t = Translator()
		try:
			res = t.translate(doc_piece)
			translated += ' ' + res.text
		except:
			print('Error translating, will try again in 10 seconds...')
			sleep(7)
			translated += ' ' + translate(doc)
	return translated


def translate_docs(df):
	df['message_en'] = df['message'].progress_apply(translate)
	return df


def main():
	args = parse_args()
	con = db_connect(args.db)

	docs = documents(args.table, args.table_trunc, args.lang, con)
	translated = translate_docs(docs)
	translated.to_csv('/sandata/garrick/wiki-translated-{}.csv'.format(args.lang))
	translated.to_sql('{}_trans_{}'.format(args.table, args.lang), con, if_exists='replace', index=False, chunksize=500)


if __name__ == '__main__':
	main()
