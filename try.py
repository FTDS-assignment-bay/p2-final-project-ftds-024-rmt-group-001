import streamlit as st
from model import recommend_system

def run():
    with st.form("Recommender system"):
        product = st.text_input('Input product name')
        submitted = st.form_submit_button('Recommend')
    if submitted:
        result = recommend_system(product)
        for res in result:
            st.write(res)

if __name__ == '__main__':
    run()