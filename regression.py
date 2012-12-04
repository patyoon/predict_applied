import pickle
from sklearn.linear_model.sparse import SGDRegressor
from scipy.sparse import lil_matrix, spdiags, csr_matrix
from MySQLdb import connect

if __name__ == "__main__":
    # good_year = pickle.load(open("good_year_index_list_teufel_m_1970.pkl", "rb"))
    # X = pickle.load(open("regression_X_teufel_m.pkl", "rb"))
    # print X.get_shape()
    # print X

    cited_id_index= pickle.load(open("reg_cited_id_index_dict_teufel_m.pkl", "rb"))
    
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()

    cursor.execute('SELECT cited_id FROM cited_papers where year > 1970')
    print "got"
    result = cursor.fetchall()
    pickle.dump(result, open("1970result.pkl", "wb"))

