import argparse
import re
from xml.etree import ElementTree as ET

import mwparserfromhell as mwp
import pandas as pd
from pandarallel import pandarallel
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.types import CHAR, INTEGER, VARCHAR
from tqdm import tqdm

pandarallel.initialize(nb_workers=5, progress_bar=True)

USER_DASHES_REGEX = r'(?:--|—)(?P<name>[^\s].+)$'
DATE_REGEX_ES = r'(?:^|\.|\?|!|--|—)(?P<name>(?:[^\.]{1,60}?|[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})(?: \([^)]+\))? )[0-9]{2}:[0-9]{2} [0-9]{1,2} [a-z]{3},? [0-9]{4} \([A-Z]{3,4}\)'
DATE_REGEX_ZH_JA = r'(?:^|\.|。|？|！|--|—)(?P<name>(?:[^\.。]{1,60}?|[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})(?: \([^)]+\))? ?)[0-9]{4} ?年 ?[0-9]{1,2} ?月 ?[0-9]{1,2} ?日 ?(?:[^0-9\.-;:,?!]+)?[0-9]{1,2}:[0-9]{1,2} ?[(（][^0-9\.()-;:,?!。]{3,4}[)）]'
DATE_REGEX_EN = r'(?:^|\.|\?|!|--|—)(?P<name>(?:[^.]{1,60}?|(Preceding unsigned comment added by )?[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})(?: \([^)]+\))? )[0-9]{1,2}:[0-9]{1,2},? ?[0-9]{1,2} ? [A-Za-z]+ [0-9]{4} \([A-Z]{3,4}\)'
DATE_REGEX = {
    'es': DATE_REGEX_ES,
    'zh': DATE_REGEX_ZH_JA,
    'ja': DATE_REGEX_ZH_JA,
    'en': DATE_REGEX_EN
}

def parse_args():
    opts = argparse.ArgumentParser()
    opts.add_argument('match_file')
    opts.add_argument('lang', choices=['es', 'zh', 'ja', 'en'])
    opts.add_argument('database')
    opts.add_argument('table')
    args = opts.parse_args()
    return args


def db_connect(db):
    con = create_engine(
        'mysql://127.0.0.1/{}?read_default_file=~/.my.cnf&charset=utf8mb4'.format(db))
    return con


def find_users_by_regex(text, lang):
    date_regex = DATE_REGEX[lang]
    matches = []
    for pattern in [date_regex, USER_DASHES_REGEX]:
        for user in re.finditer(pattern, text):
            matches.append(user)
        text = re.sub(pattern, '[SKIP]', text)  # so we don't match on the next regex
    return matches


def find_users_by_final_paragraph_signoff(text):
    last_paragraph = text.strip().split('\n')[-1]
    if len(last_paragraph.strip().split()) <= 3 and len(last_paragraph.strip()) > 0 and '[SKIP]' not in last_paragraph:
        return last_paragraph.strip()
    return None
    

def _split_message_to_turns(text, re_matches, signoff_match):
    turns = []

    # Sort the matches by occurrence order so we can split off chunks cleanly
    re_matches = sorted(re_matches, key=lambda m: m.end())

    # Each match is the end of a turn, so text up to the match is this turn,
    # then the match itself can be skipped since it's just the username.
    remaining = text
    prev_end = 0
    for i, match in enumerate(re_matches):
        end = match.end()
        if len(re_matches) < i + 1 and match.end() > re_matches[i+1].start():  # if this one is consuming the next for some reason, 
            end = re_matches[i+1].start()
        this_turn = remaining[prev_end:match.start()]#, remaining[end+1:]
        turns.append((match['name'].strip(), this_turn))
        prev_end = end
    
    if signoff_match is not None:
        turns.append((signoff_match, remaining[:remaining.rfind(signoff_match)]))

    return turns


def subsections(page):
    """Yields subsections of the wikitext page.

    Args:
        page (mwp.Wikicode): A parsed Wikicode page from mwparserfromhell

    Yields:
        str: Subsection of page
    """
    sections = page.get_sections(flat=True, include_headings=False)
    for section in sections:
        subsections = section.split('----')
        for subsection in subsections:
            yield subsection


def split_message_to_turns(msg, lang):
    page = mwp.parse(msg)

    turns = []
    
    # Get users iteratively over subsections.
    # Use subsections because they give us clues about where people are likely to sign off.
    for subsection in subsections(page):
        subsection = mwp.parse(subsection).strip_code()
        re_matches = find_users_by_regex(subsection, lang)
        final_match = find_users_by_final_paragraph_signoff(subsection) if lang == 'es' or lang == 'en' else None
        turns += _split_message_to_turns(subsection, re_matches, final_match)

    return turns


def pages(pages_file):
    root = ET.parse(pages_file)
    for page in root.iterfind('page'):
        yield page


def main():
    args = parse_args()

    # Accumulate all pages so we can split them in parallel later
    df = pd.DataFrame()
    i = 0
    for page in tqdm(pages(args.match_file)):
        i += 1
        #if page.find('title').text != 'Discusión:Astrología':
        #if page.find('title').text != 'Talk:生物学':
        #if page.find('title').text != 'Talk:Algeria':
            #continue
        title =  page.find('title').text
        msg_wiki_id = page.find('id').text
        content = page.find('revision').find('text').text
        df = pd.concat([df, pd.DataFrame.from_records([(title, msg_wiki_id, content)], columns=['title', 'message_wiki_id', 'message'])])

    # Split pages into turns and get DataFrame in shape
    df['turns'] = df['message'].parallel_apply(lambda msg: split_message_to_turns(msg, args.lang)).apply(list)
    df = df.explode('turns')
    df[['user', 'turn']] = pd.DataFrame(df['turns'].tolist(), index=df.index)
    df['user'] = df['user'].str.slice(-127)
    df.drop(['turns', 'message'], axis=1, inplace=True)
    df['turn_num'] = df.groupby('title').cumcount() + 1
    
    # Write to database
    eng = db_connect(args.database)
    df.to_sql(args.table, eng, if_exists='replace', index=False, chunksize=5000, dtype={
			'unified_id': INTEGER,
			'message_wiki_id': INTEGER,
            'turn': LONGTEXT,
            'user': VARCHAR(127),
            'datetime': VARCHAR(127),
			'lang': CHAR(2),
			'message_id': VARCHAR(126)
		})


if __name__ == '__main__':
    main()
