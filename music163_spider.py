# -*-coding=utf-8-*-
import json
import requests
import traceback
from Crypto.Cipher import AES
import base64
from music163_utils import gen_abuyun_proxy, retry, timestamp2datetime, get_now


class Music163CommentSpider:
    """
    Reference : https://www.zhihu.com/question/36081767; http://www.cnblogs.com/lyrichu/p/6635798.html
    """
    MUSIC163_COMMENT_URL = "http://music.163.com/weapi/v1/resource/comments/R_SO_4_{song_id}/?csrf_token="
    # 头部信息
    MUSIC163_HEADER = {
        'Host': "music.163.com",
        'Accept-Language': "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
        'Accept-Encoding': "gzip, deflate",
        'Content-Type': "application/x-www-form-urlencoded",
        'Cookie': "_ntes_nnid=754361b04b121e078dee797cdb30e0fd,1486026808627; _ntes_nuid=754361b04b121e078dee797cdb30e0fd; JSESSIONID-WYYY=yfqt9ofhY%5CIYNkXW71TqY5OtSZyjE%2FoswGgtl4dMv3Oa7%5CQ50T%2FVaee%2FMSsCifHE0TGtRMYhSPpr20i%5CRO%2BO%2B9pbbJnrUvGzkibhNqw3Tlgn%5Coil%2FrW7zFZZWSA3K9gD77MPSVH6fnv5hIT8ms70MNB3CxK5r3ecj3tFMlWFbFOZmGw%5C%3A1490677541180; _iuqxldmzr_=32; vjuids=c8ca7976.15a029d006a.0.51373751e63af8; vjlast=1486102528.1490172479.21; __gads=ID=a9eed5e3cae4d252:T=1486102537:S=ALNI_Mb5XX2vlkjsiU5cIy91-ToUDoFxIw; vinfo_n_f_l_n3=411a2def7f75a62e.1.1.1486349441669.1486349607905.1490173828142; P_INFO=m15527594439@163.com|1489375076|1|study|00&99|null&null&null#hub&420100#10#0#0|155439&1|study_client|15527594439@163.com; NTES_CMT_USER_INFO=84794134%7Cm155****4439%7Chttps%3A%2F%2Fsimg.ws.126.net%2Fe%2Fimg5.cache.netease.com%2Ftie%2Fimages%2Fyun%2Fphoto_default_62.png.39x39.100.jpg%7Cfalse%7CbTE1NTI3NTk0NDM5QDE2My5jb20%3D; usertrack=c+5+hljHgU0T1FDmA66MAg==; Province=027; City=027; _ga=GA1.2.1549851014.1489469781; __utma=94650624.1549851014.1489469781.1490664577.1490672820.8; __utmc=94650624; __utmz=94650624.1490661822.6.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; playerid=81568911; __utmb=94650624.23.10.1490672820",
        'Connection': "keep-alive",
        'Referer': 'http://music.163.com/'
    }
    # offset的取值为:(评论页数-1)*20,total第一页为true，其余页为false
    # first_param = '{rid:"", offset:"0", total:"true", limit:"20", csrf_token:""}' # 第一个参数
    second_param = "010001"  # 第二个参数
    # 第三个参数
    third_param = "00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7"
    # 第四个参数
    forth_param = "0CoJUm6Qyw8W8jud"
    # 设置代理服务器
    proxies = {
        'http:': 'http://121.232.146.184',
        'https:': 'https://144.255.48.197'
    }
    def __init__(self):
        self.uri = ''
        self.response = ''

    # 获取参数
    def get_params(self, page):
        """
        :param page: 传入页数
        :return: 
        """
        iv = "0102030405060708"
        first_key = self.forth_param
        second_key = 16 * 'F'
        if (page == 1):  # 如果为第一页
            first_param = '{rid:"", offset:"0", total:"true", limit:"20", csrf_token:""}'
            h_encText = self.AES_encrypt(first_param, first_key, iv)
        else:
            offset = str((page - 1) * 20)
            first_param = '{rid:"", offset:"%s", total:"%s", limit:"20", csrf_token:""}' % (offset, 'false')
            h_encText = self.AES_encrypt(first_param, first_key, iv)
        h_encText = self.AES_encrypt(h_encText, second_key, iv)
        return h_encText

    # 获取 encSecKey
    def get_encSecKey(self):
        encSecKey = "257348aecb5e556c066de214e531faadd1c55d814f9be95fd06d6bff9f4c7a41f831f6394d5a3fd2e3881736d94a02ca919d952872e7d0a50ebfa1769a7a62d512f5f1ca21aec60bc3819a9c3ffca5eca9a0dba6d6f7249b06f5965ecfff3695b54e1c28f3f624750ed39e7de08fc8493242e26dbc4484a01c76f739e135637c"
        return encSecKey

    # 解密过程
    def AES_encrypt(self, text, key, iv):
        pad = 16 - len(text) % 16
        text = text + pad * chr(pad)
        encryptor = AES.new(key, AES.MODE_CBC, iv)
        encrypt_text = encryptor.encrypt(text)
        encrypt_text = base64.b64encode(encrypt_text)
        return encrypt_text

    @retry((Exception,), tries=2, delay=2, backoff=2)
    def send_request_to_163(self, url, page):
        data = {
            "params": self.get_params(page),
            "encSecKey": self.get_encSecKey(),
        }
        self.uri = url
        r = requests.post(
            # url, headers=self.MUSIC163_HEADER, data=data, proxies=self.proxies)
            url, headers=self.MUSIC163_HEADER, data=data, proxies=gen_abuyun_proxy())
        if r.status_code == 404:
            print "404"
            return False
        elif r.status_code != 200:
            print "HTTP Code : ", r.status_code
            raise Exception("Retry")
        self.response = r.text
        return True

    def get_commernt_list(self):
        total = 0
        comment_list = []
        print "Parsing Comment List: ", self.uri,
        if len(self.response) < 1:
            return total, comment_list
        try:
            json_data = json.loads(self.response)
        except ValueError as e:
            print self.response
            return total, comment_list
        if json_data['code'] != 200:
            print "Incorrect Code: ", json_data['code']
            return total, comment_list
        for comment in json_data['comments']:
            info = {}
            info['comment_id'] = comment['commentId']
            info['user_id'] = comment['user']['userId']
            info['user_name'] = comment['user']['nickname']
            info['vip_type'] = comment['user']['vipType']
            info['user_type'] = comment['user']['userType']
            info['image_url'] = comment['user']['avatarUrl']
            info['like_num'] = comment['likedCount']
            info['publish_time'] = timestamp2datetime(comment['time'] / 1000)
            info['content'] = comment['content']
            info['create_date'] = get_now(tab=False)
            comment_list.append(info)
        print "has %d comments ." % len(comment_list)
        return json_data['total'], comment_list


class Music163UserRankSpider:
    MUSIC163_USER_HOME_URL = "http://music.163.com/#/user/home?id={user_id}"
    MUSIC163_USER_RANK_API = "http://music.163.com/weapi/v1/play/record?csrf_token="
    MUSIC163_USER_RANK_URL = "http://music.163.com/#/user/songs/rank?id={user_id}"
    MUSIC163_PLAY_LIST_API = "http://music.163.com/weapi/user/playlist?csrf_token="
    MUSIC163_PLAY_LIST_URL = "http://music.163.com/playlist?id={playlist_id}"
    # 头部信息
    MUSIC163_HEADER = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4',
        'Connection': 'keep-alive',
        'Content-Length': '474',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': '_ntes_nnid=133e5257871e773abaae7a2079e9f713,1478850768702; _ntes_nuid=133e5257871e773abaae7a2079e9f713; usertrack=ZUcIhlgpU6RC60EKBJbdAg==; P_INFO=zhicang_spider@163.com|1484721397|0|mail163|00&99|bej&1484662358&mail163#bej&null#10#0#0|&0|authcode&mail163&unireg|zhicang_spider@163.com; mail_psc_fingerprint=3ebc8133a2d8377a7bbf6c9e258cb3fa; vjuids=2831061fc.15a555fecac.0.776cf160e58fb; __utma=187553192.675879949.1479103415.1489731017.1489731017.1; __utmz=187553192.1489731017.1.1.utmcsr=open.163.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __oc_uuid=65375bf0-0ad8-11e7-95ea-59cf643d4527; UM_distinctid=15b5b91d23d784-093407cb91bc61-5e4f2b18-100200-15b5b91d23e31b; vjlast=1487491034.1491889936.23; vinfo_n_f_l_n3=5ca1e7ef56db64b9.1.1.1487491034396.1487491044266.1491890406768; _ga=GA1.2.675879949.1479103415; JSESSIONID-WYYY=rzW7%5CAnySi%2FxmO6rqtvFFNSSf0xUPGZ4GR%5CscvAH3vTks3zoaoMHdhWzADX8MNSVuXADN%2Boy2vVBK%5CgjZXXhdRizFJmfvQEa%2BlEEMDWyNyHa9EqahiU8T9fDgEAbNYMqfVwiFn5gxgwqF7tuTgUBBvmM8Jmz%5Cw85uNjoSq0Gm7M5mpj2%3A1495078191668; _iuqxldmzr_=32; __utma=94650624.675879949.1479103415.1494996305.1495076112.4; __utmb=94650624.7.10.1495076112; __utmc=94650624; __utmz=94650624.1494693885.2.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic',
        'Host': 'music.163.com',
        'Origin': 'http://music.163.com',
        'Referer': 'http://music.163.com/user/songs/rank?id=468126106',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36',
    }
    first_param_dict = {  # after several times of step in in Debugging with using Chrome console
        "csrf_token": "",
        "limit": "1000",
        "offset": "0",
        "total": "true",
        "type": "-1",
        "uid": "{user_id}",
    }
    first_param = '{"uid":"%s","type":"-1","limit":"1000","offset":"0","total":"true","csrf_token":""}'
    second_param = "010001"
    third_param = "00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7"
    forth_param = "0CoJUm6Qyw8W8jud"
    # 设置代理服务器
    proxies = {
        'http:': 'http://121.232.146.184',
        'https:': 'https://144.255.48.197'
    }
    def __init__(self):
        self.uri = ''
        self.user_id = ''
        self.response = ''

    # 解密过程
    def AES_encrypt(self, text, key, iv):
        pad = 16 - len(text) % 16
        text = text + pad * chr(pad)
        encryptor = AES.new(key, AES.MODE_CBC, iv)
        encrypt_text = encryptor.encrypt(text)
        encrypt_text = base64.b64encode(encrypt_text)
        return encrypt_text

    # 获取参数
    def get_params(self, user_id):
        """
        :param page: 传入user id
        :return: 
        """
        iv = "0102030405060708"
        first_key = self.forth_param
        second_key = 16 * 'F'
        first_param = self.first_param % user_id
        h_encText = self.AES_encrypt(first_param, first_key, iv)
        h_encText = self.AES_encrypt(h_encText, second_key, iv)
        return h_encText

    # 获取 encSecKey
    def get_encSecKey(self):
        encSecKey = "257348aecb5e556c066de214e531faadd1c55d814f9be95fd06d6bff9f4c7a41f831f6394d5a3fd2e3881736d94a02ca919d952872e7d0a50ebfa1769a7a62d512f5f1ca21aec60bc3819a9c3ffca5eca9a0dba6d6f7249b06f5965ecfff3695b54e1c28f3f624750ed39e7de08fc8493242e26dbc4484a01c76f739e135637c"
        return encSecKey

    @retry((Exception,), tries=2, delay=2, backoff=2)
    def send_request_to_163(self, url, user_id):
        data = {
            "params": self.get_params(user_id),
            "encSecKey": self.get_encSecKey(),
        }
        self.uri = url
        self.user_id = user_id
        r = requests.post(
            url, headers=self.MUSIC163_HEADER, data=data, proxies=self.proxies)
        if r.status_code == 404:
            print "404"
            return False
        elif u'无权限访问' in r.text:
            print u'无权限访问'
            return False
            # raise Exception('Retry')
        elif r.status_code != 200:
            print "HTTP Code : ", r.status_code
            raise Exception("Retry")
        self.response = r.text
        return True

    def get_ranked_songs_of_user(self):
        """
        Relation: user-song pairs
        :return: 
        """
        song_list = []
        print "Parsing Comment List of ", self.user_id,
        if len(self.response) < 1:
            return song_list
        try:
            json_data = json.loads(self.response)
        except ValueError as e:
            print self.response
            return song_list
        if json_data['code'] != 200:
            print "\nIncorrect Code: ", json_data['code']
            print json_data
            return song_list
        for song in json_data['allData']:
            # format song's attribute
            try:
                info = {}
                info['cate'] = 'all_time'
                info['score'] = song['score']
                info['song_name'] = song['song']['name']
                info['song_id'] = song['song']['id']
                info['artist_id'] = ';'.join([str(ele['id']) for ele in song['song']['ar']])
                info['artist_name'] = ';'.join([ele['name'] for ele in song['song']['ar']])
                info['album_id'] = song['song']['al']['id']
                info['album_name'] = song['song']['al']['name']
                info['create_date'] = get_now(tab=False)
                song_list.append(info)
            except Exception as e:
                traceback.print_exc()
        for song in json_data['allData']:
            # format song's attribute
            try:
                info = {}
                info['cate'] = 'last_week'
                info['score'] = song['score']
                info['song_name'] = song['song']['name']
                info['song_id'] = song['song']['id']
                info['artist_id'] = ';'.join([str(ele['id']) for ele in song['song']['ar']])
                info['artist_name'] = ';'.join([ele['name'] for ele in song['song']['ar']])
                info['album_id'] = song['song']['al']['id']
                info['album_name'] = song['song']['al']['name']
                info['create_date'] = get_now(tab=False)
                song_list.append(info)
            except Exception as e:
                traceback.print_exc()
        print "got %d songs." % len(song_list)
        return song_list

    def get_play_lists(self):
        play_lsits = []
        print "Parsing Play List of ", self.user_id,
        if len(self.response) < 1:
            return play_lsits
        try:
            json_data = json.loads(self.response)
        except ValueError as e:
            print self.response
            return play_lsits
        if json_data['code'] != 200:
            print "Incorrect Code: ", json_data['code']
            return play_lsits
        for play in json_data['playlist']:
            info = {}
            info['user_birth'] = timestamp2datetime(play['creator']['birthday']/1000)
            info['user_city'] = play['creator']['city']
            info['user_desc'] = play['creator']['description']
            info['user_province'] = play['creator']['province']
            info['user_sign'] = play['creator']['signature'].strip()
            if play['creator']['gender'] == 1:
                info['user_gender'] = 'male'
            elif play['creator']['gender'] == 2:
                info['user_gender'] = 'female'
            else:
                info['user_gender'] = ''
            info['list_id'] = play['id']
            info['list_name'] = play['name']
            info['list_play_count'] = play['playCount']
            info['list_tags'] = ';'.join(play['tags'])
            info['list_song_num'] = play['trackCount']
            info['list_followers_num'] = play['subscribedCount']
            info['list_followers'] = ';'.join(play['subscribers'])
            info['list_create_time'] = timestamp2datetime(play['createTime']/1000)
            info['comment_thread_id'] = play['commentThreadId']
            play_lsits.append(info)
        print "%d play lists." % len(play_lsits)
        return play_lsits



class Music162PlayListSpider:

    # 头部信息
    MUSIC163_HEADER = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4',
        'Connection': 'keep-alive',
        'Content-Length': '474',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': '_ntes_nnid=133e5257871e773abaae7a2079e9f713,1478850768702; _ntes_nuid=133e5257871e773abaae7a2079e9f713; usertrack=ZUcIhlgpU6RC60EKBJbdAg==; P_INFO=zhicang_spider@163.com|1484721397|0|mail163|00&99|bej&1484662358&mail163#bej&null#10#0#0|&0|authcode&mail163&unireg|zhicang_spider@163.com; mail_psc_fingerprint=3ebc8133a2d8377a7bbf6c9e258cb3fa; vjuids=2831061fc.15a555fecac.0.776cf160e58fb; __utma=187553192.675879949.1479103415.1489731017.1489731017.1; __utmz=187553192.1489731017.1.1.utmcsr=open.163.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __oc_uuid=65375bf0-0ad8-11e7-95ea-59cf643d4527; UM_distinctid=15b5b91d23d784-093407cb91bc61-5e4f2b18-100200-15b5b91d23e31b; vjlast=1487491034.1491889936.23; vinfo_n_f_l_n3=5ca1e7ef56db64b9.1.1.1487491034396.1487491044266.1491890406768; _ga=GA1.2.675879949.1479103415; JSESSIONID-WYYY=rzW7%5CAnySi%2FxmO6rqtvFFNSSf0xUPGZ4GR%5CscvAH3vTks3zoaoMHdhWzADX8MNSVuXADN%2Boy2vVBK%5CgjZXXhdRizFJmfvQEa%2BlEEMDWyNyHa9EqahiU8T9fDgEAbNYMqfVwiFn5gxgwqF7tuTgUBBvmM8Jmz%5Cw85uNjoSq0Gm7M5mpj2%3A1495078191668; _iuqxldmzr_=32; __utma=94650624.675879949.1479103415.1494996305.1495076112.4; __utmb=94650624.7.10.1495076112; __utmc=94650624; __utmz=94650624.1494693885.2.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic',
        'Host': 'music.163.com',
        'Origin': 'http://music.163.com',
        'Referer': 'http://music.163.com/user/songs/rank?id=468126106',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36',
    }
    first_param_dict = {  # after several times of step in in Debugging with using Chrome console
        "csrf_token": "",
        "limit": "1000",
        "offset": "0",
        "total": "true",
        "type": "-1",
        "uid": "{user_id}",
    }
    first_param = '{"uid":"%s","type":"-1","limit":"1000","offset":"0","total":"true","csrf_token":""}'
    second_param = "010001"
    third_param = "00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7"
    forth_param = "0CoJUm6Qyw8W8jud"
    # 设置代理服务器
    proxies = {
        'http:': 'http://121.232.146.184',
        'https:': 'https://144.255.48.197'
    }



"""
无权限访问
{u'msg': u'\u65e0\u6743\u9650\u8bbf\u95ee', u'code': -2}
"""