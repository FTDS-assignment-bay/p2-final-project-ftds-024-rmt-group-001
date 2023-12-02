import numpy as np
import pandas as pd



def recommend_system(product):
    def cosine_sim(vect1, vect2):
        norm_1 = np.linalg.norm(vect1)
        norm_2 = np.linalg.norm(vect2)
        cos_sim = (vect1 @ vect2) / (norm_1 * norm_2)
        return cos_sim

    product_vector = pd.read_csv("products_vector.csv", index_col='product_name')
    cossim = pd.Series([cosine_sim(product_vector.loc[product], x) for x in product_vector.values],
                       index=product_vector.index).drop(index=product)

    recommendations = []
    recommendations.append(f'You like {product}, you may also like:')
    for i, pro in enumerate(cossim.sort_values(ascending=False)[:10].index):
        recommendations.append(f'{i+1}. {pro}')

    return recommendations
    