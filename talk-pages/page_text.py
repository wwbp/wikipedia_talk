import argparse
import csv
import mwparserfromhell
import sys

from joblib import Parallel, delayed
from tqdm import tqdm
from xml.etree import ElementTree as ET


def parse_args():
	opts = argparse.ArgumentParser()
	opts.add_argument('xml_file', help='Output of link.py')
	opts.add_argument('language')
	args = opts.parse_args()
	return args


def cleanup_wikitext(text):
	wikitext = mwparserfromhell.parse(text)
	stripped = wikitext.strip_code()
	one_line = stripped.replace('\n', ' ').strip()
	return one_line


def get_values(page):
	title = page.find('title').text
	wiki_id = page.find('id').text
	content = page.find('revision').find('text').text
	dlatk_id = page.find('dlatk_id').text

	content = cleanup_wikitext(content)
	return dlatk_id, wiki_id, title, content


def pages(pages_file, lang):
	csvout = csv.writer(sys.stdout)
	root = ET.parse(pages_file)
	pages = tqdm(root.findall('page'), desc=lang)
	extracted_data = Parallel(n_jobs=10)(delayed(get_values)(page) for page in pages)
	for page in extracted_data:
		dlatk_id, wiki_id, title, content = page
		csvout.writerow([dlatk_id, lang, wiki_id, title, content])


def main():
	args = parse_args()
	pages(args.xml_file, args.language)


if __name__ == '__main__':
	main()