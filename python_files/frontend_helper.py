import numpy as np
import pandas as pd
import requests
import statsmodels.api as sm
import streamlit as st
import plotly.express as px

bar_color = '#bb3b3b'
bg_color = '#262730'
red_palette = ['#f2b4b4', '#e57373', '#bb3b3b', '#8b1a1a', '#4b0f0f']

back_btn_config = """
<a href='{link}' target='_self' 
    style='
        display: inline-block;
        padding: 12px 15px;
        font-size: 16px;
        font-weight: bold;
        color: white;
        text-decoration: none;
        background-color: #262730;
        border-radius: 8px;
        border: 1px solid #FF4B4B;
        transition: all 0.3s ease-in-out;
        text-align: center;'>
    ⬅ Back to {page}
</a>
<style>
    a:hover {{
        color: #FF4B4B !important;
        border-color: white !important;
        transform: scale(0.95);
    }}
</style>
"""

category_page_config = """
        <style>
        .block-container { 
            padding-left: 20px !important;
            margin-left: 0px !important;
            margin-right: 0px !important;
            padding-right: 20px !important;
            padding-top: 50px !important;
            margin-top: 0px !important;
            padding-bottom: 0px !important;
            margin-bottom: 0px !important;
            max-width: 100% !important;
        }
        </style>
        """



btn_str = """
        <form action="/" method="get">
            <button class="{class_} btn-{btn_name}" name="button" value="{value}">{text}</button>
        </form>
        """

def create_header(heading):
    back, title = st.columns([1,1.5], border=False, gap='large')
    text = f"Analyzing :red[{heading}]..."
    subheading = ":red[Select] What You Want To See:"

    with back:
        st.markdown(back_btn_config.format(link = "/", page = "Homepage"), unsafe_allow_html=True)
    with title:
        st.header(text)
        st.subheader(subheading)

def create_analysis_header(category):
    back, title = st.columns([1,1.5], border=False, gap='large')

    with back:
        st.markdown(back_btn_config.format(link = f"/?button={category.lower()}", page = category), unsafe_allow_html=True)

def create_tile(class_,btn_name,value,text):
    st.markdown(btn_str.format(class_=class_, btn_name=btn_name, value=value,text=text), unsafe_allow_html=True)

def hit_api(link):
    return requests.get(link).json()

def update_layout(fig, title, x_title=None):
    fig.update_layout(
        plot_bgcolor=bg_color,
        paper_bgcolor=bg_color,
        xaxis_title = x_title,
        title={
            'text': title,
            'x': 0.02,
            'y': 0.95,
            'font': {'size': 25}
        }
    )

def feature_imp(y,X):
    model = sm.OLS(y,X).fit()
    coefs = model.params.drop('const').abs().sort_values(ascending=False)
    return pd.DataFrame(np.round(coefs*100/coefs.sum(),2), columns=['Feature Importance (%)'])

def preprocess_laptops_ols(brand=None):
    if brand is None:
        try:
            df = pd.DataFrame(hit_api('http://127.0.0.1:5000/prep-laptop'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    else:
        try:
            df = pd.DataFrame(hit_api(f'http://127.0.0.1:5000/prep-laptop-brand?brand={brand}'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    # Extract X and y for OLS
    X = sm.add_constant(df.copy().iloc[:,:-1])
    y = df.copy().iloc[:,-1]

    # Handle missing values
    X['gpu_vram'] = X['gpu_vram'].fillna(0)
    X['ppi'] = X['ppi'].fillna(X['ppi'].mean())
    X['ram_capacity'] = X['ram_capacity'].fillna(X['ram_capacity'].mean())

    # Fix indices
    idx = X.dropna().index
    X = X.dropna().reset_index(drop=True)
    y = y[idx].reset_index(drop=True)

    return y, X

def preprocess_mobiles_ols(brand=None):
    if brand is None:
        try:
            df = pd.DataFrame(hit_api('http://127.0.0.1:5000/prep-laptop'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    else:
        try:
            df = pd.DataFrame(hit_api(f'http://127.0.0.1:5000/prep-laptop-brand?brand={brand}'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    # Extract X and y for OLS
    X = sm.add_constant(df.copy().iloc[:,:-1])
    y = df.copy().iloc[:,-1]

    # Handle missing values
    X['gpu_vram'] = X['gpu_vram'].fillna(0)
    X['ppi'] = X['ppi'].fillna(X['ppi'].mean())
    X['ram_capacity'] = X['ram_capacity'].fillna(X['ram_capacity'].mean())

    # Fix indices
    idx = X.dropna().index
    X = X.dropna().reset_index(drop=True)
    y = y[idx].reset_index(drop=True)

    return y, X

def preprocess_tablets_ols(brand=None):
    if brand is None:
        try:
            df = pd.DataFrame(hit_api('http://127.0.0.1:5000/prep-laptop'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    else:
        try:
            df = pd.DataFrame(hit_api(f'http://127.0.0.1:5000/prep-laptop-brand?brand={brand}'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    # Extract X and y for OLS
    X = sm.add_constant(df.copy().iloc[:,:-1])
    y = df.copy().iloc[:,-1]

    # Handle missing values
    X['gpu_vram'] = X['gpu_vram'].fillna(0)
    X['ppi'] = X['ppi'].fillna(X['ppi'].mean())
    X['ram_capacity'] = X['ram_capacity'].fillna(X['ram_capacity'].mean())

    # Fix indices
    idx = X.dropna().index
    X = X.dropna().reset_index(drop=True)
    y = y[idx].reset_index(drop=True)

    return y, X

def ols_chart(category, brand=None):
    if category == 'laptops':
        if brand is None:
            y, X = preprocess_laptops_ols()
        else:
            y, X = preprocess_laptops_ols(brand)
    elif category == 'mobiles':
        if brand is None:
            y, X = preprocess_mobiles_ols()
        else:
            y, X = preprocess_mobiles_ols(brand)
    else:
        if brand is None:
            y, X = preprocess_tablets_ols()
        else:
            y, X = preprocess_tablets_ols(brand)

    ft_imp = feature_imp(y, X)
    ols_ = px.bar(ft_imp, x=ft_imp.index.str.upper(), y=ft_imp.iloc[:, 0], text_auto=True, color_discrete_sequence=[bar_color])

    update_layout(ols_, "Importance of specs")
    st.plotly_chart(ols_)

    return ft_imp

def get_data(link,group_by=True, n=None):
    data = hit_api(link)
    data = pd.DataFrame(data)
    if group_by:
        data = data.iloc[:,0].value_counts().sort_values(ascending=False)[:n]
    return data

def plot_bar(data, title, h=False):
    if h:
        chart = px.bar(data, y=data.index.str.upper(), x=data.iloc[:], orientation='h', text_auto=True, color_discrete_sequence=[bar_color])
    else:
        chart = px.bar(data, x=data.index.str.upper(), y=data.iloc[:], text_auto=True, color_discrete_sequence=[bar_color])
    update_layout(chart, title)
    st.plotly_chart(chart)

def plot_pie(data, title):
    chart = px.pie(data, names=data.index.str.upper(), values=data.values, color_discrete_sequence=red_palette)
    chart.update_traces(textinfo='percent+label')
    update_layout(chart, title)
    st.plotly_chart(chart, use_container_width=True)

def plot_box(data, title):
    chart = px.box(data, color_discrete_sequence=[bar_color])
    update_layout(chart, title)
    st.plotly_chart(chart)