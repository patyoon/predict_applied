#!/usr/bin/python2.7

from MySQLdb import connect, escape_string



def get_year_count_dict(pub_year_list, ncit_list):
    year_count_dict = {}
    for pair in zip(pub_year_list, ncit_list):
        if None in pair:
            continue
        if pair[0] in year_count_dict.keys():
            (num_paper, num_count) = year_count_dict[pair[0]]
            year_count_dict[pair[0]] = (num_paper+1, float(num_count) + pair[1])
        else:
            year_count_dict[pair[0]] = (1, float(pair[1]))    
    for year in year_count_dict.keys():
        year_count_dict[year] = year_count_dict[year][1]/year_count_dict[year][0]
    return year_count_dict


if __name__ == "__main__":
    """
    Calculate average number of citation for papers published in each year.
    """

    
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')
    cursor = conn.cursor()
    cursor.execute("SELECT cited_id, year, ncit07, ncit FROM cited_papers where year > 1970 and ncit is not null" )

    res = cursor.fetchall()

    cited_id_list = map(lambda x: x[0], res)
    pub_year_list = map(lambda x: x[1], res)
    ncit_07_list = map(lambda x: x[2], res)
    ncit_tot_list = map(lambda x: x[3], res)

    print "fetched records"

    year_count_07_dict = get_year_count_dict(pub_year_list, ncit_07_list)
    year_count_tot_dict = get_year_count_dict(pub_year_list, ncit_tot_list)
    print year_count_07_dict
    print year_count_tot_dict
    print "calculated year dict"

    print year_count_07_dict
    print year_count_tot_dict
        
    for pair in zip(cited_id_list, pub_year_list, ncit_tot_list):
        print pair
        adj_ncit = float(pair[2]) / year_count_tot_dict[pair[1]]
        cursor.execute("UPDATE cited_papers SET ncit_tot_adj1=" + str(adj_ncit) + " WHERE cited_id=" + str(pair[0]))
    conn.close()
