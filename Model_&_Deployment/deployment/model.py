import numpy as np
import pandas as pd

# data loading
user_matrix = pd.read_hdf("users_matrix.h5", key='df')
product_matrix = pd.read_csv("products_matrix.csv", index_col='product_name')
df = pd.read_csv("data_cust_product.csv")
data_cust = pd.read_csv("data_output.csv")

def cosine_sim(vect1, vect2):
        '''
        fungsi ini berguna untuk mennghitung cosine similarity
        '''
        # normalisasi vektor
        norm_1 = np.linalg.norm(vect1)
        norm_2 = np.linalg.norm(vect2)

        # menghitung cosine similarity
        cos_sim = (vect1 @ vect2) / (norm_1 * norm_2)
        return cos_sim

def product_based(product):
    '''
    Fungsi ini berguna untuk memunculkan hasil content-based filtering yang mengambil nama product sebagai parameter fungsi
    '''

    # pembuatan series dengan pemanggilan cosine_sim
    cossim = pd.Series([cosine_sim(product_matrix.loc[product], x) for x in product_matrix.values],
                       index=product_matrix.index).drop(index=product)

    # list untuk container hasil
    recommendations = []
    for i, pro in enumerate(cossim.sort_values(ascending=False)[:10].index):
        recommendations.append(f'{i+1}. {pro}')
    return recommendations

def collaborative(id):

    sim_user = user_matrix[id].sort_values(ascending=False)[1:].index[0]
    return sim_user
    