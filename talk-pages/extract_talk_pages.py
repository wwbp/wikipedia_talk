import argparse
import bz2
import sys

import xml.etree.ElementTree as ET


PAGE_START = r'<page>'
PAGE_END = r'</page>'

def parse_args():
	opts = argparse.ArgumentParser(description='Extract Talk pages from Wikipedia dumps')
	opts.add_argument('dumpfile')
	opts.add_argument('--talk', default='Talk', help='The local form of "Talk" used')
	args = opts.parse_args()
	return args


def pages(dumpfile):
	stream = bz2.BZ2File(dumpfile, 'r')

	current_page = ''
	in_page = False

	for line in stream:
		line = line.decode('utf8')
		if PAGE_END in line:
			current_page += line
			yield current_page
			current_page = ''
			in_page = False
		elif PAGE_START in line:
			in_page = True

		if in_page:
			current_page += line			


def is_talk_page(page, talk):
	try:
		tree = ET.fromstring(page)
		title = tree.find('title').text
		if title.startswith('{}:'.format(talk)):
			return True
	except ET.ParseError:
		print('Error parsing: {}'.format(page), file=sys.stderr)
		return False


def main():
	args = parse_args()

	print('<pages>')
	n_pages = 0
	for page in pages(args.dumpfile):
		if is_talk_page(page, args.talk):
			print(page)
			n_pages += 1
	print('</pages>')
	print('{} talk pages found'.format(n_pages), file=sys.stderr)


if __name__ == '__main__':
	main()
