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
    opts.add_argument('--langs', nargs='+', default=['es', 'ja', 'zh'])
    opts.add_argument('--table', default='msgs')
    args = opts.parse_args()
    return args


def db_connect(db):
    con = create_engine(
        'mysql://127.0.0.1/{}?read_default_file=~/.my.cnf&charset=utf8mb4'.format(db))
    return con


def documents(table, con):
    sql = """SELECT *
             FROM {tbl} t 
             WHERE lang = 'en'"""
    sql = sql.format(tbl=table)
    df = pd.read_sql(sql, con)
    return df


def translate(doc, lang, chunk_size=3000, attempt=0):
    attempt += 1
    if attempt > 3:  # we've already tried waiting a 30 seconds, let's give up
        print('Failed to translate :(')
        return ''

    translated = ''
    for i in range(0, len(doc), chunk_size):
        doc_piece = doc[i:i+chunk_size]
        sleep(2)
        t = Translator()
        try:
            res = t.translate(doc_piece, src='en', dest=lang)
            translated += ' ' + res.text
        except:
            seconds_to_wait = attempt * 10
            print('Error translating, will try again in {} seconds...'.format(seconds_to_wait))
            sleep(seconds_to_wait - 2)
            translated += ' ' + translate(doc, lang, attempt=attempt)
    return translated


def translate_docs(df, *langs):
    for lang in langs:
        print('Translating to {}...'.format(lang))
        df['message_{}'.format(lang)] = df['message'].progress_apply(lambda msg: translate(msg, lang))
    return df


def main():
    args = parse_args()
    con = db_connect(args.db)

    docs = documents(args.table, con)
    translated = translate_docs(docs, *args.langs)

    # Get a fresh connection to avoid "MySQL server has gone away" error
    con = db_connect(args.db)

    dtypes = {
        'unified_id': INTEGER,
        'message_wiki_id': INTEGER,
        'message': LONGTEXT,
        'lang': CHAR(2),
        'message_id': VARCHAR(126)
    }
    for lang in args.langs:
        dtypes['message_{}'.format(lang)] = LONGTEXT

    translated.to_csv(
        '/sandata/garrick/wikipedia/wiki-translated-en-{}-full.csv'.format(args.langs[0]))
    translated.to_sql('{}_trans_en_{}_full'.format(args.table, args.langs[0]), con,
		if_exists='replace', index=False, chunksize=1000, dtype=dtypes)


if __name__ == '__main__':
    main()
