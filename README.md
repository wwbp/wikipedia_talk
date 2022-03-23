# Wikipedia Talk Pages

## Steps

1. Download data from Wikipedia for each language:
    - The full bz2-compressed XML dump of Wikipedia; named: `[lang]wiki-[date]-pages-meta-current.xml.bz2`. This gives us the actual page content.
    - The base per-page data SQL dump; named: `[lang]wiki-date-page.sql.gz`. This gives us the within-language mapping from ID to page title.
    - The langlinks SQL dump; named: `[lang]wiki-[date]-langlinks.sql`. This gives us the interlingual links between the base pages (not talk pages).
2. Pre-extract all Talk pages from each full dump: `python extract_talk_pages.py [lang]wiki-[date]-pages-meta-current.xml.bz2 > [lang]wiki-[date]-talk-pages.xml`. Note that you will need to specify the Talk prefix in some languages, e.g., Spanish uses "Discusión" where English uses "Talk" to designate a Talk page; you can do this by adding `--talk Discusión` to the command.
3. Load all of the SQL data into a database:
```{bash}
for lang in zh es ja en
do
  echo $lang
  mysql wikipedia < "${lang}wiki-*.sql"
  mysql wikipedia -e "rename table langlinks to ${lang}_langlinks"
done
```
4. Link the pages between languages: `python link.py wikipedia link.sql wiki-[date]-talk-pages.xml`. The full steps occurring here are described below. This will produce files called `matched_[lang]talk.xml`.
5. Extract the page text, stripping all wiki markup: `for lang in zh es ja en; do python page_text.py "matched_${lang}talk.xml" $lang; done > matched_all.csv`.
6. Import the CSV into MySQL. Do this any way you want, but here's how I did it using DLATK: `python ~/code/dlatk/dlatk/tools/importmethods.py -d wikipedia -t msgs --csv_to_mysql --csv_file matched_all.csv --column_description '(unified_id int, lang char(2), message_wiki_id int, title varchar(255), message text)'`. Add a unique ID for this page in this language: `mysql wikipedia -e 'alter table msgs add column message_id varchar(127); update msgs set message_id = concat(lang, unified_id)'`.
7. Translate the non-English pages into English: `python translate.py wikipedia [lang]`.
8. For each page topic, truncate all English text to match the length of the shortest page across languages: `python truncate.py wikipedia msgs_trans_es msgs_trans_ja msgs_trans_zh msgs_en`.

## Linking

This section contains details on the linking process from step 4 above. Linking refers to linking equivalent Talk pages across languages. For the purposes of the following, a Talk page is the page in which users discuss the contents of a Base page, where a Base page contains the article about a certain subject.

Wikipedia provides all of the data we need to link Talk pages across languages, but it is fragmented. For example, the langlinks tables link *from* a page ID, but they link *to* a page title in the second language. Only Base pages are linked across languages, but these can be used to identify matching Talk pages.

The `link.py` script performs the following steps to link Talk pages:

1. The `link.sql` file is run, producing the table `en_matches` by joining the base page table with the langlinks table. This provides a mapping from the English page ID and page title to the foreign language page title in each of the target languages.
2. The Base page mappings are read from `en_matches`. The mappings are from English Base page ID to the title of the equivalent Base page in each of the target languages. Pages that do not map across all of the target languages are dropped. Note that this only gives us the mappings between Base pages that occur in all languages; we now need to filter to only the Talk pages that occur in all languages (which will be a subset of the Base pages that occur in all languages).
3. A set containing all English Base page IDs from the mappings is created. This set is reduced iteratively for each language *besides English* via the following steps: 
    1. Convert the Base page title into a Talk page title by adding the Talk prefix for that language
    2. Iterate through the Talk pages XML file, identifying Talk pages from our list
    3. Save the page ID for the associated English Base page
    4. Subset the Base page IDs set to include only the IDs for which there are matching Talk pages in the current language
4. Use the results from step 3 to subset our overall mapping from English Base page ID to target language Base page title. Only pages with associated Talk pages in each of the target languages will remain.
5. Use these English Base page IDs to identify English Talk page IDs. The `enpages` table uses the same page title for both Base and Talk pages, but Talk versions of the pages have `page_namespace = 1`. We can convert the Base page ID to the Base page title, and then lookup the page ID where this title has a namespace of 1, indicating that it is a Talk page.
6. Extract and save English pages with the Talk page IDs identified in the previous step.
7. Extract and save the pages in all target languages where the page title is in our remaining set of mappings (but in Talk page format). This is a repeat of steps 3.1 and 3.2.

In steps 6 and 7, we add an element to each Talk page setting its "dlatk_id" to the ID of the associated English Base page. This ID links the Talk pages across languages.

Note that we skip English in step 3 because our data is in the form `english_page_id -> target_language_title`, making it difficult to cleanly account for English page titles. We address this in step 5.

Finally, it is worth noting that the Talk pages XML files are iterated over twice. In the first iteration, we identify not only the Talk pages that exist in this language (which would only take a single pass) but that also exist in all other languages. In the second iteration, we can extract and save the pages that remain.