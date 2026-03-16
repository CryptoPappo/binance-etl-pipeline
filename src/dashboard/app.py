import streamlit as st

page_1 = st.Page("pages/page_1.py", title="Time Interval Charts")
page_2 = st.Page("pages/page_2.py", title="Time Correlations")
page_3 = st.Page("pages/page_3.py", title="Histograms")

pg = st.navigation([page_1, page_2, page_3])
st.set_page_config(page_title="Trades Dashboard")
pg.run()
