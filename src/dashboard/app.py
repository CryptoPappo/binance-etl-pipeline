import streamlit as st

page_0 = st.Page("pages/page_0.py", title=":house_with_garden: Homepage")
page_1 = st.Page("pages/page_1.py", title=":chart_with_upwards_trend: Market Overview")
page_2 = st.Page("pages/page_2.py", title=":chart_with_downwards_trend: Correlations")
page_3 = st.Page("pages/page_3.py", title=":bar_chart: Distributions")

pg = st.navigation([page_0, page_1, page_2, page_3])
st.set_page_config(page_title="Trades Dashboard")
pg.run()
