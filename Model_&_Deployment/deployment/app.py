import streamlit as st
import pandas as pd
from PIL import Image
import joblib 

# Import beberapa function dari model.py dan beberapa variable
from model import product_based, collaborative, user_matrix, product_matrix, df

# Load model pipeline
pipeline = joblib.load('pipeline.pkl')

def run():
    # Inisialisasi session state untuk hasil cluster dan recommender
    if 'cluster_result' not in st.session_state:
        st.session_state.cluster_result = None

    if 'recommender_result' not in st.session_state:
        st.session_state.recommender_result = None

    # Judul aplikasi
    st.write("#### Fashion Product Customer Clustering and Recommender System")
    
    # Tampilkan gambar
    image = Image.open('image.webp')  
    st.image(image)

    # Form untuk melakukan clustering
    with st.form("Cluster"):

        # Input slider untuk beberapa fitur
        max_price = st.slider(label='Max Price of Items', min_value=0, max_value=50000000, value=10000)
        trans_count = st.slider(label='Transaction Count', min_value=1, max_value=1000, value=1)

        # Membagi kolom dengan 4 bagian untuk input jumlah barang, promo, pengiriman, dan total pembayaran
        column1, column2, column3, column4 = st.columns([2,2,2,2])
        with column1:
            total_items = st.number_input(label='Total Items', min_value=1,max_value= 1000, value=1)
        with column2:
            total_promo = st.number_input(label='Total Promo Amount', min_value=0 ,max_value= 20000000, value=10000)
        with column3:
            total_shipment = st.number_input(label='Total Shipment Amount', min_value=10000 ,max_value= 20000000, value=10000)
        with column4:
            total_amount = st.number_input(label='Total Paid Amount', min_value=0 ,max_value= 5000000000, value=10000)

        # Tombol untuk memprediksi cluster
        submit_cluster = st.form_submit_button('Predict Cluster')

        # Menampilkan data hasil input
        data_display = {
                'max_price': max_price,
                'transaction_count': (int(trans_count)),
                'total_items': total_items,
                'total_promo':total_promo,
                'total_shipment':total_shipment,
                'total_amount':total_amount
                }
        data_disp = pd.DataFrame([data_display]).reset_index(drop=True)
        st.dataframe(data_disp)

    # Proses saat tombol cluster ditekan
    if submit_cluster:
        avg_item_price = (total_amount - total_shipment - total_promo) / total_items
        avg_t_amount = total_amount / trans_count
        avg_promo = total_promo / trans_count
        avg_shipment = total_shipment / trans_count
        data = {
                'max_price': max_price,
                'transaction_count': (int(trans_count)),
                'total_items': total_items,
                'total_promo':total_promo,
                'total_shipment':total_shipment,
                'total_amount':total_amount,
                'avg_item_price': avg_item_price,
                'avg_t_amount': avg_t_amount, 
                'avg_promo' : avg_promo,
                'avg_shipment' : avg_shipment
                }
        dict_cluster = {0: "Moderate Spenders",
                        1: "Frugal Shoppers",
                        2: "High-Spending Enthusiasts"}

        # Data inference ke dataframe
        data_inf = pd.DataFrame([data]).reset_index(drop=True)
        prediction = pipeline.predict(data_inf)
        st.session_state.cluster_result = f'#### Category: {dict_cluster[int(prediction)]}'
    
    # Menampilkan hasil cluster
    if st.session_state.cluster_result:
        st.write(st.session_state.cluster_result)

    # Form recommender system
    with st.form("Recommender system"):
        
        col1, col2 = st.columns([2,2])
        with col1:
            # User ID berdasarkan index matrix
            user_id = st.selectbox('User ID', options=user_matrix.index.tolist())
        with col2:
            # Product name form berdasarkan index product matrix
            product = st.selectbox('Select Product Name', options=product_matrix.index.unique().tolist())

        # Tombol rekomendasi
        submit_recommender = st.form_submit_button('Recommend')
        
    # Proses saat tombol recommender ditekan
    if submit_recommender:
        # Pemanggilan collaborative filtering
        similar_user = collaborative(user_id)

        # Result 10 teratas
        user_result = df[df.customer_id == similar_user].product_name[:10]

        # Pemanggilan product_based filtering function
        result = product_based(product)
        # Hasil content-based filtering
        st.session_state.recommender_result = f'You like {product}, you may also like:\n' + '\n'.join(result)

        # Hasil collaborative filtering
        st.session_state.recommender_result += '\n\nOthers also like:\n' + '\n'.join([f"{i+1}. {re}" for i, re in enumerate(user_result)])

    # Menampilkan hasil recommender
    if st.session_state.recommender_result:
        st.write(st.session_state.recommender_result)

if __name__ == '__main__':
    run()
