.DEFAULT_GOAL := default
#################### PACKAGE ACTIONS ###################
test_main_package:
	python -c 'from rss_scrapper.main import test; test()'

test_main_new_articles:
	python -c 'from rss_scrapper.main import get_latest_article; get_latest_article()'
