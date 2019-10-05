## environment
python 3.6.8+
PostgreSQL 10

## run procedure
1. install the required packages
pip install -r requirements.txt

2. create a postgresql db named 'yp'(or you can name it)
sudo -u postgres psql -c 'create database yp;'

3. change the database information(username/password) in the source code(postgresdb.py)
for example,
USERNAME    = "postgres"
PASSWORD    = "XXXXXX"      -> change required
DBNAME      = "yp"          -> change required(if you named your db)
HOSTNAME    = "localhost"
PORT        = "5432"

4. run scraper to get index pages
scrapy crawl linkspider

5. run scraper to get business pages after the linkspider finished
scrapy crawl ypspider

## Reference
1. First, crawl index pages.
2. Next, crawl business pages.