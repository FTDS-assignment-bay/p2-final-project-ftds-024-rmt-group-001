import streamlit as st
from PIL import Image
# import beberapa function dari modul.py dan beberapa variable
from model import product_based, collaborative, user_matrix, product_matrix, df


def run():
    # judul
    st.title("Fashion Product Recommender System")
    
    # image
    image = Image.open('image.webp')  
    st.image(image, caption='Fashion Product')
    # formulir
    with st.form("Recommender system"):
        # user id berdasarkan index matrix
        user_id = st.selectbox('User ID', options=user_matrix.index.tolist())

        # product name form berdasarkan index product matrix
        product = st.selectbox('Select Product Name', options=product_matrix.index.unique().tolist())

        # tombol rekomendasi
        submitted = st.form_submit_button('Recommend')
    
    if submitted:
        # pemanggilan collaborative filtering
        similar_user = collaborative(user_id)

        # result 10 teratas
        user_result = df[df.customer_id == similar_user].product_name[:10]

        # pemanggilan product_based filtering function
        result = product_based(product)

        # hasil product based
        st.write(f'You like {product}, you may also like:')
        for res in result:
            st.write(res)

        # hasil collaborative filtering
        st.write('Others also like:')
        for i,re in enumerate(user_result):
            st.write(f"{i+1}. {re}")
if __name__ == '__main__':
    run()