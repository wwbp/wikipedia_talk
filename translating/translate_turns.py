import argparse
import pandas as pd

from googletrans import Translator
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.types import CHAR, INTEGER, VARCHAR
from time import sleep
from tqdm import tqdm


tqdm.pandas()


def parse_args():
    opts = argparse.ArgumentParser()
    opts.add_argument('db')
    opts.add_argument('lang')
    opts.add_argument('--table', default='msgs')
    args = opts.parse_args()
    return args


def db_connect(db):
    con = create_engine(
        'mysql://127.0.0.1/{}?read_default_file=~/.my.cnf&charset=utf8mb4'.format(db))
    return con


def documents(table, con):
    sql = """SELECT *
             FROM {tbl} t"""
    sql = sql.format(tbl=table)
    df = pd.read_sql(sql, con)
    return df


def translate(doc, chunk_size=3000, attempt=0):
    attempt += 1
    if attempt > 3:  # we've already tried waiting a full minute, let's give up
        print('Failed to translate :(')
        return ''

    translated = ''
    if doc is not None:
        for i in range(0, len(doc), chunk_size):
            doc_piece = doc[i:i+chunk_size]
            if doc_piece.strip() == '':
                continue
            sleep(2)
            t = Translator()
            try:
                res = t.translate(doc_piece)
                translated += ' ' + res.text
            except:
                pass
                #seconds_to_wait = attempt * 10
                #print('Error translating, will try again in {} seconds...'.format(seconds_to_wait))
                #sleep(seconds_to_wait - 2)
                #translated += ' ' + translate(doc, attempt=attempt)
    return translated


def translate_docs(df):
    df['turn_en'] = df['turn'].progress_apply(translate)
    return df


def main():
    args = parse_args()
    con = db_connect(args.db)

    docs = documents(args.table, con)
    translated = translate_docs(docs)

    # Get a fresh connection to avoid "MySQL server has gone away" error
    con = db_connect(args.db)

    translated.to_csv(
        '/sandata/garrick/wikipedia/wiki-translated-turns-{}-full.csv'.format(args.lang))
    translated.to_sql('{}_trans_{}_full'.format(args.table, args.lang), con,
		if_exists='replace', index=False, chunksize=500, dtype={
			'message_wiki_id': INTEGER,
			'turn_en': LONGTEXT,
			'turn': LONGTEXT
		}
	)


if __name__ == '__main__':
    main()
