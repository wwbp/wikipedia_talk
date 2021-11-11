/* 
Connect pages interlingually:
Main article ID -> Foreign language title
*/
DROP TABLE IF EXISTS en_matches;
CREATE TABLE en_matches AS
	SELECT en.page_id, en.page_title, ll.ll_lang, ll.ll_title
	FROM enpage en
	JOIN en_langlinks ll
	ON en.page_id = ll.ll_from
	WHERE page_namespace = 0
	AND ll.ll_lang IN ('es', 'zh', 'ja');