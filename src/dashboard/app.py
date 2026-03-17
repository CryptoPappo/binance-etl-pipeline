import streamlit as st

page_0 = st.Page("pages/page_0.py", title="Homepage")
page_1 = st.Page("pages/page_1.py", title="Market Overview")
page_2 = st.Page("pages/page_2.py", title="Correlations")
page_3 = st.Page("pages/page_3.py", title="Distributions")

pg = st.navigation([page_0, page_1, page_2, page_3])
st.set_page_config(page_title="Trades Dashboard")
pg.run()
