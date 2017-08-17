# -*- coding=utf8 -*-
import json
import time
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from music163_utils import DBAccesor
from music163_config import LOCAL_DATABASE

def generate_transmit_matrix():
    sql = """
        select distinct user_id, song_id, score from music163_user_song_relation
        where category = 'last_week';
    """ # 143978 * 10324
    index2user = {}  # {user: {song1: score1, song2: score2} }
    index2song = {}
    dao = DBAccesor(LOCAL_DATABASE)
    conn = dao.connect_database()
    data_set = pd.read_sql(sql, conn)
    data_set.to_csv("music163_user_song_relation.csv", index=False)
    for i, user in enumerate(set(data_set.user_id)):
        index2user[user] = i
    for j, song in enumerate(set(data_set.song_id)):
        index2song[song] = j
    print "Song size: ", len(index2song)
    print "User size", len(index2user)
    user_song_mat = np.zeros([len(index2song), len(index2user)])
    for i in range(data_set.shape[0]):
        the_user = index2user[data_set[i].user_id]
        the_song = index2song[data_set[i].song_id]
        score = data_set[i].score
        user_song_mat[the_song][the_user] = score
    return user_song_mat

def build_csr_matrix(data_frame):
    index2user = {}
    user_list = []
    index2song = {}
    song_list = []
    for i in range(data_frame.shape[0]):
        the_user = data_frame['user_id'].values[i]
        the_song = data_frame['song_id'].values[i]
        if the_user not in index2user:
            index2user[the_user] = len(index2user)
            user_list.append(the_user)
        if the_song not in index2song:
            index2song[the_song] = len(index2song)
            song_list.append(the_song)
    data = []
    indices = []
    indptr = [0]
    for user_idx in range(len(user_list)):
        songs_of_user = data_frame[data_frame['user_id'] == user_list[user_idx]]
        # print songs_of_user
        song_idx_score_dict = dict([(index2song[s1], s2) for s1, s2 in zip(
            songs_of_user['song_id'].values, songs_of_user['score'].values
        )])
        song_idx_score_items = sorted(song_idx_score_dict.items(), key=lambda x:x[0])
        # print song_idx_score_items
        data.extend([s[1] for s in song_idx_score_items])
        indices.extend([s[0] for s in song_idx_score_items])
        indptr.append(songs_of_user.shape[0] + indptr[-1])
    user_song_matrix = csr_matrix((data, indices, indptr), dtype=int)
    return user_song_matrix, user_list, song_list

def load_dataset_from_file():
    import sys
    data_set = pd.read_csv("music163_user_song_relation.csv")
    print sys.getsizeof(data_set)
    print sys.getsizeof(data_set['user_id'][0])
    print sys.getsizeof(data_set['song_id'][0])
    print sys.getsizeof(data_set['score'][0])

def test_build_csr_matrix():
    test_data = pd.DataFrame({
        "user_id": [123, 123, 456, 456, 789, 789],
        "song_id": [111, 555, 222, 444, 111, 333],
        "score": [100, 30, 50, 20, 20, 40],
    })

    # target_mat = np.matrix(
    #     [[100, 0, 0, 0, 30], [0, 50, 0, 20, 0], [20, 0, 40, 0, 0]]
    # )
    # print target_mat
    # target_data = [100, 30, 50, 20, 20, 40]
    # target_indices = [0, 4, 1, 3, 0, 2]
    # target_indptr = [0, 2, 4, 6]
    # print csr_matrix((target_data, target_indices, target_indptr), dtype=int).todense()
    mat, users, songs = build_csr_matrix(test_data)
    print mat.todense()
    print users
    print songs

def load_dataset():
    sql = """
        select distinct user_id, song_id, score from music163_user_song_relation
        where category = 'last_week';
    """
    dao = DBAccesor(USED_DATABASE)
    conn = dao.connect_database()
    data_frame = pd.read_sql(sql, conn)
    return data_frame

def main():
    start = time.time()
    dataset = load_dataset()  # shape: (1004072, 3)
    print "Load data costs %.2f seconds" % (time.time() - start)
    user_song_matrix, index2user, index2song = build_csr_matrix(dataset)
    # print "Saving matrix ..."
    save_sparse_csr("music163_users_songs_matrix", user_song_matrix)
    json.dump(index2user, open("music163_index2users.json", "w"))
    json.dump(index2song, open("music163_index2songs.json", "w"))
    # del dataset
    print user_song_matrix.shape  # 10324 * 143978 = 1486428872
    print len(index2user)
    print len(index2song)
    print "Build CSR matrix costs %.2f seconds" % (time.time() - start)
    # import ipdb; ipdb.set_trace()
    # assert user_song_matrix[0].todense()[0, 0] == dataset[np.logical_and(
    #     dataset.user_id == index2user[0], dataset.song_id == index2song[0])].score.values[0]
    # assert user_song_matrix[100].todense()[0, 100] == dataset[np.logical_and(
    #     dataset.user_id == index2user[100], dataset.song_id == index2song[100])].score.values[0]
    # the [100, 100] in dataset is not existed

def distributed_multiply(mat_A, mat_B):
    pass

def csr_matrix2tuple(mat):
    coordinate_data = []
    for coord, data in zip(zip(*mat.nonzero()), mat.data):
        coordinate_data.append((coord[0], coord[1], data))
    return coordinate_data

def calc_euclidean_similarity(vec_1, vec_2):
    """
    Compute euclidean distance between two array
    :param vec_1: 
    :param vec_2: 
    :return: 
    """
    sim = 0
    vec_1 = vec_1.reshape((vec_1.shape[1],))
    vec_2 = vec_2.reshape((vec_2.shape[1],))
    vec_1_nnz = np.nonzero(vec_1)[0]
    print vec_1_nnz
    # import ipdb; ipdb.set_trace()
    vec_2_nnz = np.nonzero(vec_2)[0]
    print vec_2_nnz
    intersect = set(vec_1_nnz) & set(vec_2_nnz)
    if len(intersect) > 0:
        error_squares = [pow(vec_1[arg] - vec_2[arg], 2) for arg in intersect]
        sim = 1.0 / (1 + np.sqrt(np.sum(error_squares)))
    return sim

def calc_pearson_correlation(vec_1, vec_2):
    sim = 0
    vec_1 = vec_1.reshape((vec_1.shape[1],))
    vec_2 = vec_2.reshape((vec_2.shape[1],))
    vec_1_nnz = np.nonzero(vec_1)[0]
    vec_2_nnz = np.nonzero(vec_2)[0]
    intersect = set(vec_1_nnz) & set(vec_2_nnz)
    if len(intersect) > 0:
        sum1 = np.sum(vec_1)
        sum2 = np.sum(vec_2)
        sum1_square = np.sum(np.pow(vec_1))
        sum2_square = np.sum(np.pow(vec_2))
        sum_product = np.sum([vec_1[arg]*vec_2[arg] for arg in intersect])
        # calculate pearson correlation score
        numerator = sum_product - (sum1 * sum2 * 1.0 / len(intersect))
        denominator = np.sqrt(
            (sum1_square - np.pow(sum1, 2)*1.0/len(intersect)) * (sum2_square - np.pow(sum2, 2)*1.0/len(intersect)))
        if denominator > 0:
            sim = numerator / denominator
    return sim


def save_sparse_csr(filename,array):
    np.savez(filename,data = array.data ,indices=array.indices,
             indptr =array.indptr, shape=array.shape )

def load_sparse_csr(filename):
    loader = np.load(filename)
    return csr_matrix((  loader['data'], loader['indices'], loader['indptr']),
                         shape = loader['shape'])

def save_sparse_mat(filename, mat):
    # Slower than np.savez
    from scipy import io
    io.mmwrite(filename, mat)

def load_sparse_mat(filename):
    from scipy import io
    temp_coo_mat = io.mmread(filename)
    return temp_coo_mat.tocsr()



def test_map_reduce_multiply():
    test_matrix = csr_matrix([
        [1,0,3,0,0],
        [0,0,2,0,4],
        [0,0,0,0,5],
        [9,0,0,0,2]
    ])
    # save_sparse_csr("music163_user_item.mat", test_matrix)
    # save_sparse_mat("music163_user_item.mat", test_matrix)
    # Test csr matrix 2 tuple

    data = csr_matrix2tuple(test_matrix)
    print data
    sim = calc_euclidean_similarity(test_matrix[0].toarray(), test_matrix[1].toarray())
    assert sim == 0.5
    print sim
    sim = calc_euclidean_similarity(test_matrix[0].toarray(), test_matrix[2].toarray())
    assert sim == 0
    print sim
    # product = test_matrix * test_matrix.T
    # print product
    # print test_matrix.todense()
    # import ipdb; ipdb.set_trace()
    # test_prod = distributed_multiply(test_matrix, test_matrix.T)


if __name__ == '__main__':
    # generate_transmit_matrix()
    # load_dataset()
    # test_build_csr_matrix()
    main()
    # test_map_reduce_multiply()