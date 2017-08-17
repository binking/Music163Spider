# -*-coding:utf8-*-
import traceback
from music163_utils import DBAccesor, get_now

"""
alter table music163_user_song_relation 
add update_date datetime default current_timestamp on update current_timestamp;

CREATE TABLE `music163_comment_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `uri` text,
  `comment_id` int(11) DEFAULT NULL,
  `song_id` int(11) DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `user_type` varchar(16) DEFAULT NULL,
  `vip_type` varchar(16) DEFAULT NULL,
  `user_name` varchar(64) DEFAULT NULL,
  `comment` text,
  `publish_time` datetime DEFAULT NULL,
  `create_date` datetime DEFAULT NULL,
  `update_date` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `INDEX_COMMENT_ID` (`comment_id`) USING BTREE,
  KEY `INDEX_SONG_ID` (`song_id`) USING BTREE,
  KEY `INDEX_PUBLISH_TIME` (`publish_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `music163_user_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) DEFAULT NULL,
  `user_type` varchar(16) DEFAULT NULL,
  `vip_type` varchar(16) DEFAULT NULL,
  `user_name` varchar(64) DEFAULT NULL,
  `image_url` varchar(255) DEFAULT NULL,
  `create_date` datetime DEFAULT NULL,
  `update_date` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `INDEX_USER_ID` (`user_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

class Music163Writer(DBAccesor):
    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)
        self.debug = False

    def insert_comment_list(self, comment_list):
        comment_sql = """
            INSERT INTO music163_comment_info (uri, page, song_id, comment_id, user_id, 
            like_num, content, publish_time, create_date)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s
            FROM DUAL WHERE NOT EXISTS (
                SELECT id FROM music163_comment_info WHERE comment_id=%s
            );
        """
        user_sql = """
            INSERT INTO music163_user_info (user_id, user_name, 
            user_type, vip_type, image_url, create_date)
            SELECT %s,%s,%s,%s,%s,%s
            FROM DUAL WHERE NOT EXISTS (    
                SELECT id FROM music163_user_info WHERE user_id=%s
            );
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        uri = comment_list['uri']
        page = comment_list['page']
        song_id = comment_list['song_id']
        for info in comment_list['comments']:
            try:
                if cursor.execute(comment_sql, (uri, page, song_id,
                    info['comment_id'], info['user_id'], info['like_num'], info['content'],
                    info['publish_time'], info['create_date'], info['comment_id']
                )):
                    conn.commit()
                    print "{now}$$$ Insert Comment {c_id} done .".format(now=get_now(), c_id=info['comment_id'])
                else:
                    print "{now}### Comment {c_id} exsited.".format(now=get_now(), c_id=info['comment_id'])
                if cursor.execute(user_sql, (
                    info['user_id'], info['user_name'], info['user_type'],
                    info['vip_type'], info['image_url'], info['create_date'], info['user_id']
                )):
                    conn.commit()
                    print "{now}$$$ Insert User {u_id} done .".format(now=get_now(), u_id=info['user_id'])
                else:
                    print "{now}### User {u_id} existed.".format(now=get_now(), u_id=info['user_id'])
            except Exception as e:
                traceback.print_exc()
        cursor.close()
        conn.close()

    def insert_song_list_of_user(self, song_list):
        relation_sql = """
            INSERT INTO music163_user_song_relation (
            user_id, song_id, category, score, create_date)
            SELECT %s,%s,%s,%s,%s
            FROM DUAL WHERE NOT EXISTS (
            SELECT id FROM music163_user_song_relation 
            WHERE user_id=%s AND song_id=%s AND category=%s)
        """
        insert_sql = """
            INSERT INTO music163_user_song_relation (
            user_id, song_id, category, score, create_date)
            VALUE (%s,%s,%s,%s,%s);
        """
        song_sql = """
            INSERT INTO music163_song_info (song_id, song_name, 
            artist_ids, artist_names, album_id, album_name, create_date)
            SELECT %s,%s,%s,%s,%s,%s,%s 
            FROM DUAL WHERE NOT EXISTS (
            SELECT id FROM music163_song_info WHERE song_id=%s)
        """
        user = song_list['user_id']
        conn = self.connect_database()
        for song in song_list['songs']:
            cursor = conn.cursor()
            if cursor.execute(insert_sql, (
                user, song['song_id'], song['cate'], song['score'],
                song['create_date']  # , user, song['song_id'], song['cate']
            )):
                conn.commit()
                print "{now}$$$ Insert Relation {uid} to {sid} done.".format(
                    now=get_now(),uid=user, sid=song['song_id'])
            else:
                print "{now}### Relation {uid} to {sid} existed.".format(
                    now=get_now(),uid=user, sid=song['song_id'])
            if cursor.execute(song_sql, (
                song['song_id'], song['song_name'], song['artist_id'], song['artist_name'],
                song['album_id'], song['album_name'], song['create_date'], song['song_id']
            )):
                conn.commit()
                print "{now}$$$ Insert Song {sid} done.".format(now=get_now(), sid=song['song_id'])
            else:
                print "{now}### Song {sid} existed.".format(now=get_now(), sid=song['song_id'])
            cursor.close()
        conn.close()

    def select_user_for_song(self):
        select_sql = """
            SELECT DISTINCT user_id 
            FROM music163_comment_info mci
            WHERE NOT EXISTS (
            SELECT id FROM music163_user_song_relation
            WHERE user_id=mci.user_id);
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_sql)
        for res in cursor.fetchall():
            yield res[0]

    def select_song_for_comment(self):
        select_sql = """
            SELECT DISTINCT song_id
            FROM music163_user_song_relation musr
            WHERE NOT EXISTS (
            SELECT id FROM music163_comment_info
            WHERE song_id=musr.song_id);
         """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_sql)
        for res in cursor.fetchall():
            yield res[0]