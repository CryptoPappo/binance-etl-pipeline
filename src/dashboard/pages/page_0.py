import streamlit as st

st.title(":bank: Binance Trades Analysis Dashboard")

st.markdown("""
Welcome to the **Binance Trades Analysis Dashboard**.


Use the sidebar to navigate between pages:
- **Market Overview** — price action and trading activity
- **Correlations** — temporal dependencies in order flow
- **Distributions** — statistical properties of trades and returns


This application provides an exploratory and quantitative analysis of high-frequency cryptocurrency trade data extracted from the Binance exchange.
It is organized into three main sections, each focusing on a different aspect of market microstructure: **price dynamics, temporal correlations, and statistical distributions**.


The dashboard is designed to allow interactive exploration of large datasets while remaining computationally efficient.
""")

st.header(":chart_with_upwards_trend: Page 1 — Market Overview & Price Dynamics", divider=True)

st.markdown("""
This page focuses on **price formation and trading activity over time**.


Charts included:


- **Candlestick chart with volume**  
    Displays OHLC price candles for a selected time interval, together with traded volume. This provides a compact view of price evolution and liquidity.
- **Trade volume and trade count**  
    Shows how trading activity evolves over time by plotting:  
   - Total traded volume   
   - Number of trades per interval  
    This helps distinguish between high-volume trading and high-frequency activity.  
- **Normalized delta volume**  
    Measures the imbalance between buy and sell volume, normalized by total volume.  
    This chart highlights periods of buying or selling pressure and potential order-flow dominance.  
""")

st.header(":chart_with_downwards_trend: Page 2 — Correlations & Order Flow", divider="red")

st.markdown("""
This section explores **orders dependencies** in trading behavior. Most of the correlations studied follow **an exponential function, a power law or a combination of both**.


**Charts included**:


- **Trade sign autocorrelation**  
    Measures persistence in buy/sell order flow over orders, revealing long-memory effects in trade direction.  
- **Trade size autocorrelation**  
    Shows whether large or small trades tend to cluster.  
- **Trade sign–size cross-correlation**  
    Examines the relationship between trade direction and trade size, providing insight into asymmetric trading behavior.  
- **Absolute returns autocorrelation**  
    Captures volatility clustering by measuring correlations in absolute price returns.  


Together, these plots characterize the market’s microstructure beyond simple price movements.
""")

st.header(":bar_chart: Page 3 — Statistical Distributions", divider="green")

st.markdown("""
This page focuses on **distributional properties** of trades and price changes.


**Charts included**:


- **Inter-trade time histogram**  
    Distribution of time differences between consecutive trades, highlighting bursty and non-Poisson trading behavior.  
- **Trade size histogram**  
    Shows the distribution of trade sizes, typically heavy-tailed and informative about liquidity and market impact.  
- **Absolute returns histogram**  
    Displays the distribution of price changes, useful for identifying fat tails and potential power-law behavior.  


Logarithmic scaling is provided where appropriate to better visualize tail behavior.
""")

