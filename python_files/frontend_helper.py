import numpy as np
import pandas as pd
import requests
import statsmodels.api as sm
from sklearn.metrics import f1_score
from statsmodels.stats.outliers_influence import variance_inflation_factor
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff

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

f1 = "Market Analysis"
f2 = "Know Your Brand"
f3 = "Interesting Price Dynamics"
f4 = "Worth Your Money?"

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

def preprocess_laptops_ols(on, brand=None):
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
    if on == 'specs':
        df.drop(columns=['price'], inplace=True)
        y = df.copy()['spec_score']
        X = sm.add_constant(df.copy().drop(columns=['spec_score']))
    elif on == 'price':
        df.drop(columns=['spec_score'], inplace=True)
        y = df.copy()['price']
        X = sm.add_constant(df.copy().drop(columns=['price']))

    # Handle missing values
    X['gpu_vram'] = X['gpu_vram'].fillna(0)
    X['ppi'] = X['ppi'].fillna(X['ppi'].mean())
    X['ram_capacity'] = X['ram_capacity'].fillna(X['ram_capacity'].mean())

    # Fix indices
    idx = X.dropna().index
    X = X.dropna().reset_index(drop=True)
    y = y[idx].reset_index(drop=True)

    # Check for multi-collinearity
    vif_data = pd.DataFrame()
    vif_data["feature"] = X.columns
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    if np.isinf(vif_data['VIF']).sum() + vif_data['VIF'].isna().sum() > 0:
        X.drop(
            columns=vif_data[np.isinf(vif_data['VIF'])]['feature'].to_list() + vif_data[np.isnan(vif_data['VIF'])]['feature'].to_list(),
            inplace=True
        )
        vif_data = pd.DataFrame()
        vif_data["feature"] = X.columns
        vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

    columns_to_drop = list(vif_data[vif_data['VIF'] > 5]['feature'].values)
    if 'const' in columns_to_drop:
        columns_to_drop.remove('const')
    X.drop(columns=columns_to_drop, inplace=True)

    return y, X

def preprocess_mobiles_ols(on, brand=None):
    if brand is None:
        try:
            df = pd.DataFrame(hit_api('http://127.0.0.1:5000/prep-smartphones'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    else:
        try:
            df = pd.DataFrame(hit_api(f'http://127.0.0.1:5000/prep-smartphones-brand?brand={brand}'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    # Extract X and y for OLS
    if on == 'specs':
        df.drop(columns=['price'], inplace=True)
        y = df.copy()['spec_score']
        X = sm.add_constant(df.copy().drop(columns=['spec_score']))
    elif on == 'price':
        df.drop(columns=['spec_score'], inplace=True)
        y = df.copy()['price']
        X = sm.add_constant(df.copy().drop(columns=['price']))

    # Handle missing values
    X['ram'] = X['ram'].fillna(X['ram'].median())
    X['storage'] = X['storage'].fillna(X['storage'].median())
    X['battery'] = X['battery'].fillna(X['battery'].median())
    X['screen_size'] = X['screen_size'].fillna(X['screen_size'].median())
    X['ppi'] = X['ppi'].fillna(X['ppi'].median())
    X['rear_primary'] = X['rear_primary'].fillna(X['rear_primary'].median())
    X['front_primary'] = X['front_primary'].fillna(X['front_primary'].median())
    X['cpu'] = X['cpu'].fillna(X['cpu'].median())

    # Check for multi-collinearity
    vif_data = pd.DataFrame()
    vif_data["feature"] = X.columns
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    if np.isinf(vif_data['VIF']).sum() + vif_data['VIF'].isna().sum() > 0:
        X.drop(
            columns=vif_data[np.isinf(vif_data['VIF'])]['feature'].to_list() + vif_data[np.isnan(vif_data['VIF'])]['feature'].to_list(),
            inplace=True
        )
        vif_data = pd.DataFrame()
        vif_data["feature"] = X.columns
        vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

    columns_to_drop = list(vif_data[vif_data['VIF'] > 5]['feature'].values)
    if 'const' in columns_to_drop:
        columns_to_drop.remove('const')
    X.drop(columns=columns_to_drop, inplace=True)

    return y, X

def preprocess_tablets_ols(on, brand=None):
    if brand is None:
        try:
            df = pd.DataFrame(hit_api('http://127.0.0.1:5000/prep-tablets'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    else:
        try:
            df = pd.DataFrame(hit_api(f'http://127.0.0.1:5000/prep-tablets-brand?brand={brand}'))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    # Extract X and y for OLS
    if on == 'specs':
        df.drop(columns=['price'], inplace=True)
        y = df.copy()['spec_score']
        X = sm.add_constant(df.copy().drop(columns=['spec_score']))
    elif on == 'price':
        df.drop(columns=['spec_score'], inplace=True)
        y = df.copy()['price']
        X = sm.add_constant(df.copy().drop(columns=['price']))

    # Handle missing values
    X['ram'] = X['ram'].fillna(X['ram'].median())
    X['inbuilt_storage'] = X['inbuilt_storage'].fillna(X['inbuilt_storage'].median())
    X['battery_capacity'] = X['battery_capacity'].fillna(X['battery_capacity'].median())
    X['fast_charging'] = X['fast_charging'].fillna(X['fast_charging'].median())
    X['screen_size'] = X['screen_size'].fillna(X['screen_size'].median())
    X['ppi'] = X['ppi'].fillna(X['ppi'].median())
    X['rear_primary'] = X['rear_primary'].fillna(X['rear_primary'].median())
    X['front_primary'] = X['front_primary'].fillna(X['front_primary'].median())
    X['expandable'] = X['expandable'].fillna(X['expandable'].median())
    X['cpu'] = X['cpu'].fillna(X['cpu'].median())

    # Check for multi-collinearity
    vif_data = pd.DataFrame()
    vif_data["feature"] = X.columns
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    if np.isinf(vif_data['VIF']).sum() + vif_data['VIF'].isna().sum() > 0:
        X.drop(
            columns=vif_data[np.isinf(vif_data['VIF'])]['feature'].to_list() + vif_data[np.isnan(vif_data['VIF'])]['feature'].to_list(),
            inplace=True
        )
        vif_data = pd.DataFrame()
        vif_data["feature"] = X.columns
        vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

    columns_to_drop = list(vif_data[vif_data['VIF'] > 5]['feature'].values)
    if 'const' in columns_to_drop:
        columns_to_drop.remove('const')
    X.drop(columns=columns_to_drop, inplace=True)

    return y, X

def ols_chart(category,on='specs', brand=None):
    if category == 'laptops':
        if brand is None:
            y, X = preprocess_laptops_ols(on=on)
        else:
            y, X = preprocess_laptops_ols(on=on, brand=brand.lower())
    elif category == 'smartphones':
        if brand is None:
            y, X = preprocess_mobiles_ols(on=on)
        else:
            y, X = preprocess_mobiles_ols(on=on, brand=brand.lower())
    else:
        if brand is None:
            y, X = preprocess_tablets_ols(on=on)
        else:
            y, X = preprocess_tablets_ols(on=on, brand=brand.lower())

    ft_imp = feature_imp(y, X)
    ols_ = px.bar(ft_imp, x=ft_imp.index.str.upper(), y=ft_imp.iloc[:, 0], text_auto=True, color_discrete_sequence=[bar_color])

    update_layout(ols_, f"Impact on {on.capitalize()}")
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
    if isinstance(data.index,str):
        chart = px.pie(data, names=data.index.str.upper(), values=data.values, color_discrete_sequence=red_palette)
    else:
        chart = px.pie(data, names=data.index, values=data.values, color_discrete_sequence=red_palette)
    chart.update_traces(textinfo='percent+label')
    update_layout(chart, title)
    st.plotly_chart(chart, use_container_width=True)

def plot_box(data, title):
    chart = px.box(data, color_discrete_sequence=[bar_color])
    update_layout(chart, title)
    st.plotly_chart(chart)

def plot_hist(data, title):
    chart = px.histogram(data,x=data.values, color_discrete_sequence=[bar_color], nbins=50)
    update_layout(chart, title)
    st.plotly_chart(chart)

def plot_merged_hist(data, brand, metric, title):
    fig = go.Figure()
    brand_data = data[data['brand'] == brand.lower()]

    fig.add_trace(go.Histogram(
        x=data[metric],
        name="All Brands",
        opacity=0.5,
        marker_color='lightgray'
    ))

    fig.add_trace(go.Histogram(
        x=brand_data[metric],
        name=brand.capitalize(),
        opacity=0.7,
        marker_color=bar_color
    ))

    fig.update_layout(
        barmode='overlay',
        xaxis_title='Price',
        yaxis_title='Count',
        legend_title='Brand'
    )

    update_layout(fig, title)
    st.plotly_chart(fig)

def plot_merged_box(data, brand, metric, title):
    fig = go.Figure()
    brand_data = data[data['brand'] == brand.lower()]

    fig.add_trace(go.Box(
        x=data[metric],
        name="All Brands",
        opacity=0.5,
        marker_color='lightgray'
    ))

    fig.add_trace(go.Box(
        x=brand_data[metric],
        name=brand.capitalize(),
        opacity=0.7,
        marker_color=bar_color
    ))

    fig.update_layout(
        barmode='overlay',
        xaxis_title='Price',
        yaxis_title='Count',
        legend_title='Brand'
    )

    update_layout(fig, title)
    st.plotly_chart(fig)

def plot_multiple_box(data, col, title, only=[]):
    fig = go.Figure()
    data = data.copy()
    if 0 in data[col].dropna().unique():
        data[col] = data[col].map({0: 'Does not have', 1:'Has'})
    if only:
        for value in only:
            fig.add_trace(go.Box(
                x=data[data[col] == value]['price'],
                name=value.upper(),
                opacity=0.7,
                marker_color=bar_color
            ))
    else:
        for value in data[col].dropna().unique():
            fig.add_trace(go.Box(
                x=data[data[col] == value]['price'],
                name=value.upper(),
                opacity=0.7,
                marker_color=bar_color
            ))

    fig.update_layout(
        barmode='overlay',
        xaxis_title='Price',
        yaxis_title='Count',
        legend_title='Brand'
    )

    update_layout(fig, title)
    st.plotly_chart(fig)

def plot_merged_bar(data, brand, metric, title, h=False):
    brand_data = data[data['brand'] == brand.lower()][metric].value_counts()
    data = data[metric].value_counts()
    fig = go.Figure()

    if h:
        fig.add_trace(go.Bar(x=data.values, y=data.index, name="All Brands", orientation='h', marker_color='lightgray'))
        fig.add_trace(go.Bar(x=brand_data.values, y=brand_data.index, name=brand.capitalize(), orientation='h', marker_color=bar_color))
    else:
        fig.add_trace(go.Bar(y=data.values, x=data.index, name="All Brands", marker_color='lightgray'))
        fig.add_trace(go.Bar(y=brand_data.values, x=brand_data.index, name=brand.capitalize(), marker_color=bar_color))

    fig.update_layout(
        xaxis_title=metric.capitalize(),
        barmode='group'
    )
    update_layout(fig, title)
    st.plotly_chart(fig)

def plot_two_pies(data, brand, metric, title, n=None):
    brand_data = data[data['brand'].str.lower() == brand.lower()]

    all_counts = data[metric].value_counts().sort_values(ascending=False).head(n)
    brand_counts = brand_data[metric].value_counts().sort_values(ascending=False).head(n)

    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{'type': 'domain'}, {'type': 'domain'}]],
        subplot_titles=("All Brands", f"{brand.capitalize()} Only")
    )

    fig.add_trace(go.Pie(
        labels=all_counts.index,
        values=all_counts.values,
        name="All Brands",
        marker=dict(colors=red_palette)
    ), row=1, col=1)

    fig.add_trace(go.Pie(
        labels=brand_counts.index,
        values=brand_counts.values,
        name=brand.capitalize(),
        marker=dict(colors=red_palette)
    ), row=1, col=2)

    fig.update_layout(
        legend_title=metric.title()
    )
    update_layout(fig, title)
    st.plotly_chart(fig)

def leave_lines(lines):
    for i in range(lines):
        st.text('')

def filter_data(df,col,value):
    return df[df[col] == value.lower()]

def create_section_heading(heading):
    st.divider()
    st.header(heading)
    leave_lines(1)

def get_top_items(df, n=5, ascending=False):
    if ascending:
        list_ = df.loc[:, ['name', 'price']].copy().sort_values(by='price', ascending=True).head(n)
        list_.rename({'price': 'Price', 'name': 'Name'}, axis=1, inplace=True)
        list_['Name'] = list_['Name'].str.title()
        list_['Price'] = list_['Price'].apply(lambda x: f"{x:,}")
    else:
        list_ = df.loc[:,['name', 'price']].copy().sort_values(by = 'price', ascending = False).head(n)
        list_.rename({'price': 'Price', 'name': 'Name'}, axis = 1, inplace = True)
        list_['Name'] = list_['Name'].str.title()
        list_['Price'] = list_['Price'].apply(lambda x: f"{x:,}")

    return list_

def f3_part1(category, df):
    top_5 = get_top_items(df)
    bottom_5 = get_top_items(df, ascending=True)

    top, bottom = st.columns(2)
    with top:
        st.subheader("5 Most Expensive Products")
        st.dataframe(top_5, hide_index=True)
    with bottom:
        st.subheader("5 Least Expensive Products")
        st.dataframe(bottom_5, hide_index=True)

    st.subheader("The range of price is quite big. Let's look at some basic stats.")

    stats = pd.DataFrame(df.describe()['price'])
    stats.reset_index(inplace=True)
    stats['index'] = stats['index'].str.upper()
    stats.rename({'Measure': 'Name', 'price': 'Price'}, axis=1, inplace=True)
    stats['Price'] = stats['Price'].apply(lambda x: f"{np.round(x,2):,}")

    stat, box = st.columns([1,2], gap='medium')
    with stat:
        st.dataframe(stats, hide_index=True)
    with box:
        plot_hist(df['price'], "Histogram for Price")

    st.subheader("Let's analyze the impact of numerical features on price.")
    chart, df_ = st.columns([1,2])
    with chart:
        st.plotly_chart(px.imshow(df.select_dtypes(['int', 'float64']).corr(), color_continuous_scale='Reds'))
    with df_:
        st.dataframe(df.select_dtypes(['int', 'float64']).corr())

    leave_lines(2)
    ols_col1, ols_col2 = st.columns([2,1], gap='large')
    with ols_col1:
        try:
            ft_imp = ols_chart(category, on='price')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    with ols_col2:
        st.text(f"""
        This bar chart highlights the relative importance of various specifications in determining a {category}'s overall price.
        '{ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).index[0].capitalize()}' functionality stands out as the most influential factor, contributing approximately {ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).values[0][0]}% to the price. In contrast, '{ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).index[-1].capitalize()}' has minimal impact, accounting for just {ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).values[-1][0]}% of the overall importance.
        """)

    st.subheader("Now, we may move to analyzing categorical features.")