# -*-encoding: utf8 -*-
import time
import redis
import pickle
import traceback
import multiprocessing as mp
from music163_spider import Music163UserRankSpider
from music163_writer import Music163Writer
from music163_utils import get_now
from music163_config import LOCAL_REDIS, LOCAL_DATABASE, MUSIC163_USER_LIST, MUSIC163_SONG_LIST


def get_songs(cache):
    """
    :var data:dict, {table_id, ps_id, article_url}
    :param cache: 
    :return: 
    """
    spider = Music163UserRankSpider()
    while True:
        try:
            user = int(cache.blpop(MUSIC163_USER_LIST, 0)[1])
        except KeyboardInterrupt:
            print "Quit in silence .."
            break
        if not user:
            print "No more jobs ..."
            break
        try:
            status = spider.send_request_to_163(spider.MUSIC163_USER_RANK_API, user)
            if not status:
                continue
            song_list = spider.get_ranked_songs_of_user()
            if len(song_list) > 0:
                data = {
                    'user_id': user, 'songs': song_list,
                }
                cache.rpush(MUSIC163_SONG_LIST, pickle.dumps(data))
            time.sleep(4)
        except Exception as e:
            traceback.print_exc()
            cache.rpush(MUSIC163_USER_LIST, user)
            time.sleep(10)
        except KeyboardInterrupt:
            print "Quit in silence ..."
            break


def insert_songs(cache):
    dao = Music163Writer(LOCAL_DATABASE)
    while True:
        try:
            job = cache.blpop(MUSIC163_SONG_LIST, 0)[1]
        except KeyboardInterrupt:
            print "Quit in silence .."
            break
        if not job:
            print "No more jobs ..."
            break
        data = pickle.loads(job)
        try:
            dao.insert_song_list_of_user(data)
        except Exception as e:
            traceback.print_exc()
            cache.rpush(MUSIC163_SONG_LIST, job)
            time.sleep(10)
        except KeyboardInterrupt:
            print "Quir in Force ..."
            break


def add_job(cache):
    if cache.llen(MUSIC163_USER_LIST) > 0:
        print "*"*20, "Still busy now ..", "*"*20
        return
    dao = Music163Writer(LOCAL_DATABASE)
    for user in dao.select_user_for_song():
        cache.rpush(MUSIC163_USER_LIST, user)

def main():
    r = redis.StrictRedis(**LOCAL_REDIS)
    add_job(r)
    print "There are %d jobs .." % r.llen(MUSIC163_USER_LIST)
    job_pool = mp.Pool(
        processes=8, initializer=get_songs, initargs=(r, ))
    res_pool = mp.Pool(
        processes=8, initializer=insert_songs, initargs=(r, ))
    job_pool.close()
    res_pool.close()
    job_pool.join()
    res_pool.join()
    print "{now}Finally, there are {num} jobs left".format(
        now=get_now(), num=r.llen(MUSIC163_USER_LIST))
    print "{now}There are {num} results left".format(
        now=get_now(), num=r.llen(MUSIC163_SONG_LIST))

if __name__ == '__main__':
    print get_now()
    start = time.time()
    main()
    print "It takes %.2f seconds" % (time.time() - start)

