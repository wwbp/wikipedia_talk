import argparse
import os.path
import pandas as pd
import sys

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from tqdm import tqdm
from xml.etree import ElementTree as ET


MATCHES_TABLE = 'en_matches'
TALK_PREFIXES = {
    'en': 'Talk',
    'es': 'Discusión',
    'ja': 'ノート',
    'zh': 'Talk'
}


def parse_args():
    opts = argparse.ArgumentParser()
    opts.add_argument('db')
    opts.add_argument('sql_file')
    opts.add_argument('talk_pages_file_base')
    opts.add_argument('--skip-mapping', action='store_true', help='Skip the SQL-based mapping step and simply extract files')
    args = opts.parse_args()
    return args


def db_connect(db):
    con = create_engine(
        'mysql://127.0.0.1/{db}?charset=utf8mb4&read_default_file=~/.my.cnf'.format(db=db))
    return con


def execute_sql_file(sql_file, con):
    with open(sql_file) as f:
        sql = f.read()
    with con.connect() as connection:
        query = text(sql)
        connection.execute(query)


def get_mappings(matches_table, con):
    df = pd.read_sql(matches_table, con)

    # Don't really know why I have to do this, but I do
    for col in ['ll_lang', 'll_title']:
        df[col] = df[col].str.decode('utf8')

    # Map page_id (in English) -> each language's page title equivalent
    # Drop any row with an NA or missing value, because we only want matches across all langs
    mappings = df \
        .drop('page_title', axis=1) \
        .pivot(index='page_id', columns='ll_lang', values='ll_title') \
        .dropna()
    return mappings


def extract_pages(pages_file, to_extract, title_element='title', save=True, save_location=None):
    """Extracts specified Talk pages from an XML file.

    Args:
        pages_file (str): Path to the XML file containing Talk pages.
        to_extract (dict): A dictionary of {prefix:title -> en_page_id} where prefix is the Talk prefix used in this language.
        save (bool): Whether to save the extracted pages.
        save_location (str, optional): File in which to save output. If None (by default), saves to the same directory as the XML file.

    Returns:
        set: A set containing the English page ID of the pages successfully extracted.
    """
    if save_location is None:
        save_location = os.path.join(os.path.split(pages_file)[0], 'matched_{}'.format(pages_file))

    num_to_extract = len(to_extract)
    extracted = set()

    with tqdm(desc='Extracting {} pages from {}'.format(num_to_extract, pages_file), total=num_to_extract) as prog_bar:
        with open(save_location, 'w') as out:
            if save:
                print('<pages>', file=out)
            for _, elem in ET.iterparse(pages_file):
                if elem.tag == 'page':
                    title = elem.find(title_element).text
                    if title in to_extract:
                        en_page_id = to_extract[title]
                        extracted.add(en_page_id)
                        prog_bar.update(1)
                        if save:
                            dlatk_id = ET.Element('dlatk_id')
                            dlatk_id.text = str(en_page_id)
                            elem.append(dlatk_id)
                            page_xml = ET.tostring(elem, encoding='unicode')
                            print(page_xml, file=out)
                    elem.clear()
            if save:
                print('</pages>', file=out)
    return extracted


def process_mappings(mappings, lang, talk_pages_file_base):
    pages = mappings[lang].to_dict()  # { en_page_id -> other_lang_title }
    page_prefix = '{}:'.format(TALK_PREFIXES[lang]) if lang != 'en' else ''
    pages = {'{}{}'.format(page_prefix, v): k for k, v in pages.items()}  # reverse the mapping so we can more easily iterate through page titles
    pages_file = lang + talk_pages_file_base
    return pages, pages_file


def extract_all_langs(mappings, talk_pages_file_base, langs=['es', 'ja', 'zh'], save=True):
    extracted_in_all = set(mappings.index.to_numpy())  # start with all pages
    for lang in langs:
        pages, pages_file = process_mappings(mappings, lang, talk_pages_file_base)
        extracted = extract_pages(pages_file, pages, save=save)
        extracted_in_all &= extracted
    return extracted_in_all


def get_english_ids(main_article_ids, db_con):
    """The ids in our mappings are for the non-talk pages. This will find us the Talk page ids.

    Args:
        main_article_ids (list): A list of main article IDs that need to be converted to Talk page IDs
        db_con (Engine): An SQLAlchemy Engine
    """
    talk_page_ids = []
    for page_id in tqdm(main_article_ids, desc='Getting English Talk page IDs'):
        with db_con.connect() as con:
            title = con.execute(text('SELECT page_title FROM enpage WHERE page_id = :pid'), pid=page_id).fetchone()[0].decode('utf8')
            res = con.execute(text('SELECT page_id FROM enpage WHERE page_title = :ptitle AND page_namespace = 1'), ptitle = title).fetchone()
            if res is None:
                talk_id = None
            else:
                talk_id = str(int(res[0]))
            talk_page_ids.append(talk_id)
    return talk_page_ids


def main():
    args = parse_args()
    con = db_connect(args.db)

    if not args.skip_mapping:
        print('Matching pages...', file=sys.stderr)
        execute_sql_file(args.sql_file, con)
    
    print('Getting mappings...', file=sys.stderr)
    mappings = get_mappings(MATCHES_TABLE, con)
    
    # First pass so we can identify the common pages
    print('First pass; not all pages will be extracted', file=sys.stderr)
    extracted = extract_all_langs(mappings, args.talk_pages_file_base, save=False)

    # Second pass, the one we actually want
    print('Second pass; all pages should be extracted', file=sys.stderr)
    universal_mappings = mappings.loc[list(extracted), :]

    # Start with English: first get_english_ids will only get ids for main articles with talk pages
    # and since universal_mappings only includes pages that exist in all other languages, if they
    # have a page in English, they have a page in all languages
    universal_mappings['en'] = get_english_ids(universal_mappings.index.to_numpy(), con)

    # Drop any that don't have a talk page in English; all other languages are already filtered
    universal_mappings = universal_mappings.dropna()

    # Finally, extract and save the English talk pages
    pages, pages_file = process_mappings(universal_mappings, 'en', args.talk_pages_file_base)
    extract_pages(pages_file, pages, title_element='id', save=True)

    # And then do the same for all other languages
    extract_all_langs(universal_mappings, args.talk_pages_file_base)


if __name__ == '__main__':
    main()
