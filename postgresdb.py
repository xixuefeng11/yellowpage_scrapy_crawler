import psycopg2
from datetime import datetime


USERNAME    = "postgres"
PASSWORD    = "XXXXXX"
DBNAME      = "yp"
HOSTNAME    = "localhost"
PORT        = "5432"

TABLE_LINKS = "link"
TABLE_BUSINESS = "business"

class DBManager():

    def __init__(self):
        self.conn = psycopg2.connect(database=DBNAME, \
            user=USERNAME, password=PASSWORD, \
            host=HOSTNAME, port=PORT)
        cur = self.conn.cursor()

        # link table
        # url, main_cat, sub_cat, state, city, fetched
        cur.execute("select * from information_schema.tables where table_name=%s", (TABLE_LINKS,))
        if bool(cur.rowcount) == False:
            print ("create link table")
            cur.execute('''CREATE TABLE LINK
                (   URL         VARCHAR(255)   PRIMARY KEY NOT NULL,
                    MAIN_CAT    VARCHAR(64)    NOT NULL,
                    SUB_CAT     VARCHAR(64)    NOT NULL,
                    STATE       VARCHAR(32)    NOT NULL,
                    CITY        VARCHAR(32)    NOT NULL);''')

            self.conn.commit()

        # business table
        # id, name, description, main_category, sub_category, country, street, locality, region, postalcode,
        # latitude, longitude, telephone, business_years, opening_hours, image, website, email, payment, information, gallery, review, url
        cur.execute("select * from information_schema.tables where table_name=%s", (TABLE_BUSINESS,))
        if bool(cur.rowcount) == False:
            cur.execute('''CREATE TABLE BUSINESS
                (   ID              VARCHAR(255)    PRIMARY KEY    NOT NULL,
                    NAME            VARCHAR(255)    NOT NULL,
                    DESCRIPTION     TEXT,
                    MAIN_CATEGORY   VARCHAR(255)    NOT NULL,
                    SUB_CATEGORY    VARCHAR(255)    NOT NULL,
                    COUNTRY         VARCHAR(255),
                    STREET          VARCHAR(255),
                    LOCALITY        VARCHAR(255),
                    REGION          VARCHAR(255),
                    POSTALCODE      VARCHAR(255),
                    LATITUDE        VARCHAR(255),
                    LONGITUDE       VARCHAR(255),
                    TELEPHONE       VARCHAR(255),
                    BUSINESS_YEARS  VARCHAR(255),
                    OPENING_HOURS   VARCHAR,
                    IMAGE           VARCHAR,
                    WEBSITE         VARCHAR,
                    EMAIL           VARCHAR,
                    PAYMENT         VARCHAR,
                    INFORMATION     TEXT,
                    GALLERY         TEXT,
                    REVIEW          TEXT,
                    URL             VARCHAR(255)    NOT NULL,
                    CREATED_AT      VARCHAR(32)     NOT NULL,
                    UPDATED_AT      VARCHAR(32));''')

            self.conn.commit()


    # def __exit__(self, exc_type, exc_value, traceback):
    #     self.conn.close()
    #     print ("connection closed")


    def insertLink(self, linkInfo):
        cur = self.conn.cursor()

        link_url = linkInfo['url']
        cur.execute("SELECT url FROM LINK WHERE url = %s", (link_url,))

        if cur.fetchone():
            # update the row
            cur.execute("UPDATE LINK SET MAIN_CAT = %s, SUB_CAT = %s, STATE = %s, CITY = %s \
                            WHERE URL = %s", (linkInfo['main_category'], linkInfo['sub_category'], \
                                linkInfo['state'], linkInfo['city'], link_url,))
            # print ("updated", linkInfo)
            self.conn.commit()
            return

        # insert the row
        cur.execute("INSERT INTO LINK (URL, MAIN_CAT, SUB_CAT, STATE, CITY) VALUES (%s, %s, %s, %s, %s)", \
                    (link_url, linkInfo['main_category'], linkInfo['sub_category'], \
                    linkInfo['state'], linkInfo['city'],))
        # print ("inserted", linkInfo)
        self.conn.commit()


    def deleteLink(self, url):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM LINK WHERE url = %s", (url,))
        print ("deleted", url)
        self.conn.commit()


    def fetchLink(self, count=1):
        cur = self.conn.cursor()

        cur.execute("SELECT * FROM LINK LIMIT %s", (count,))
        self.conn.commit()
        rows = cur.fetchall()
        links = []
        for row in rows:
            link_info = {
                "url": row[0],
                "main_category": row[1],
                "sub_category": row[2],
                "state": row[3],
                "city": row[4]
            }
            links.append(link_info)

        for link in links:
            self.deleteLink(link['url'])

        return links


    def getLinkCount(self):
        cur = self.conn.cursor()

        cur.execute("SELECT COUNT(*) FROM LINK")
        result = cur.fetchone()
        return result[0]


    def insertBusiness(self, bizInfo):

        now = datetime.now()
        nowtime = now.strftime("%Y-%m-%d %H:%M:%S")

        cur = self.conn.cursor()

        biz_id = bizInfo['id']

        try:
            cur.execute("SELECT id FROM BUSINESS WHERE id = %s", (biz_id,))
            result = cur.fetchone()
        except:
            return

        if result:
            # update the row
            sqlcomm = """
                UPDATE BUSINESS SET
                    NAME            = %s,
                    DESCRIPTION     = %s,
                    MAIN_CATEGORY   = %s,
                    SUB_CATEGORY    = %s,
                    COUNTRY         = %s,
                    STREET          = %s,
                    LOCALITY        = %s,
                    REGION          = %s,
                    POSTALCODE      = %s,
                    LATITUDE        = %s,
                    LONGITUDE       = %s,
                    TELEPHONE       = %s,
                    BUSINESS_YEARS  = %s,
                    OPENING_HOURS   = %s,
                    IMAGE           = %s,
                    WEBSITE         = %s,
                    EMAIL           = %s,
                    PAYMENT         = %s,
                    INFORMATION     = %s,
                    GALLERY         = %s,
                    REVIEW          = %s,
                    URL             = %s,
                    UPDATED_AT      = %s
                WHERE   ID          = %s"""
            try:
                cur.execute(sqlcomm, \
                    ( \
                        bizInfo['name'], \
                        bizInfo['description'], \
                        bizInfo['main_category'], \
                        bizInfo['sub_category'], \
                        bizInfo['country'], \
                        bizInfo['street'], \
                        bizInfo['locality'], \
                        bizInfo['region'], \
                        bizInfo['postalcode'], \
                        bizInfo['latitude'], \
                        bizInfo['longitude'], \
                        bizInfo['telephone'], \
                        bizInfo['business_years'], \
                        bizInfo['opening_hours'], \
                        bizInfo['image'], \
                        bizInfo['website'], \
                        bizInfo['email'], \
                        bizInfo['payment'], \
                        bizInfo['information'], \
                        bizInfo['gallery'], \
                        bizInfo['review'], \
                        bizInfo['url'], \
                        nowtime, \
                        biz_id,))
                self.conn.commit()
            except:
                pass
            return

        # insert the row
        sqlcomm = """
            INSERT INTO BUSINESS (
                ID, NAME, DESCRIPTION, MAIN_CATEGORY, SUB_CATEGORY, 
                COUNTRY, STREET, LOCALITY, REGION, POSTALCODE, LATITUDE, 
                LONGITUDE, TELEPHONE, BUSINESS_YEARS, OPENING_HOURS, IMAGE, 
                WEBSITE, EMAIL, PAYMENT, INFORMATION, GALLERY, REVIEW, URL, 
                CREATED_AT, UPDATED_AT
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        try:
            cur.execute(sqlcomm, \
                ( \
                    bizInfo['id'], \
                    bizInfo['name'], \
                    bizInfo['description'], \
                    bizInfo['main_category'], \
                    bizInfo['sub_category'], \
                    bizInfo['country'], \
                    bizInfo['street'], \
                    bizInfo['locality'], \
                    bizInfo['region'], \
                    bizInfo['postalcode'], \
                    bizInfo['latitude'], \
                    bizInfo['longitude'], \
                    bizInfo['telephone'], \
                    bizInfo['business_years'], \
                    bizInfo['opening_hours'], \
                    bizInfo['image'], \
                    bizInfo['website'], \
                    bizInfo['email'], \
                    bizInfo['payment'], \
                    bizInfo['information'], \
                    bizInfo['gallery'], \
                    bizInfo['review'], \
                    bizInfo['url'], \
                    nowtime, \
                    "",))
            self.conn.commit()
        except:
            print ("bizinfo =========>>>>>>>>>>", bizInfo)
            pass

dbmanager = DBManager()