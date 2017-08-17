# -*-encoding: utf8 -*-
import time
import redis
import pickle
import traceback
import multiprocessing as mp
from music163_spider import Music163CommentSpider
from music163_writer import Music163Writer
from music163_utils import get_now
from music163_config import LOCAL_REDIS, LOCAL_DATABASE, MUSIC163_COMMENT_JOB, MUSIC163_COMMENT_RES


def get_article_status(cache):
    """
    :var data:dict, {table_id, ps_id, article_url}
    :param cache: 
    :return: 
    """
    spider = Music163CommentSpider()
    while True:
        try:
            job = cache.blpop(MUSIC163_COMMENT_JOB, 0)[1]
        except KeyboardInterrupt:
            print "Quit in silence .."
            break
        if not job:
            print "No more jobs ..."
            break
        data = pickle.loads(job)
        try:
            song_id = data['song_id']
            page = data['page']
            uri = spider.MUSIC163_COMMENT_URL.format(song_id=song_id)
            status = spider.send_request_to_163(uri, page=page)
            if not status:
                continue
            total, comment_list = spider.get_commernt_list()
            if page == 1 and total > 20:
                if total % 20 == 0:
                    next_pages = range(2, total/20 + 2)
                else:
                    next_pages = range(2, int(total/20.0) + 3)
                for next_page in next_pages:
                    next_job = {}
                    next_job['song_id'] = song_id
                    next_job['page'] = next_page
                    cache.rpush(MUSIC163_COMMENT_JOB, pickle.dumps(next_job))
            if len(comment_list) > 0:
                data = {
                    'song_id': song_id, 'page': page, 'uri': uri,
                    'comments': comment_list,
                }
                cache.rpush(MUSIC163_COMMENT_RES, pickle.dumps(data))
            time.sleep(4)
        except Exception as e:
            traceback.print_exc()
            cache.rpush(MUSIC163_COMMENT_JOB, job)
            time.sleep(10)
        except KeyboardInterrupt:
            print "Quit in silence ..."
            break


def delete_articles(cache):
    dao = Music163Writer(LOCAL_DATABASE)
    while True:
        try:
            job = cache.blpop(MUSIC163_COMMENT_RES, 0)[1]
        except KeyboardInterrupt:
            print "Quit in silence .."
            break
        if not job:
            print "No more jobs ..."
            break
        data = pickle.loads(job)
        try:
            dao.insert_comment_list(data)
        except Exception as e:
            traceback.print_exc()
            cache.rpush(MUSIC163_COMMENT_RES, job)
            time.sleep(10)
        except KeyboardInterrupt:
            print "Quir in Force ..."
            break


def add_job(cache):
    if cache.llen(MUSIC163_COMMENT_JOB) > 0:
        print "*"*20, "Still busy now ..", "*"*20
        return
    dao = Music163Writer(LOCAL_DATABASE)
    for sid in dao.select_song_for_comment():
        job = {
            'song_id': sid, "page": 1,
        }
        cache.rpush(MUSIC163_COMMENT_JOB, pickle.dumps(job))

def main():
    r = redis.StrictRedis(**LOCAL_REDIS)
    add_job(r)
    print "There are %d jobs .." % r.llen(MUSIC163_COMMENT_JOB)
    job_pool = mp.Pool(
        processes=8, initializer=get_article_status, initargs=(r, ))
    res_pool = mp.Pool(
        processes=4, initializer=delete_articles, initargs=(r, ))
    job_pool.close()
    res_pool.close()
    job_pool.join()
    res_pool.join()
    print "{now}Finally, there are {num} jobs left".format(
        now=get_now(), num=r.llen(MUSIC163_COMMENT_JOB))
    print "{now}There are {num} results left".format(
        now=get_now(), num=r.llen(MUSIC163_COMMENT_RES))

if __name__ == '__main__':
    print get_now()
    start = time.time()
    main()
    print "It takes %.2f seconds" % (time.time() - start)

