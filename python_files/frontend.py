import pandas as pd
import numpy as np
import requests
import streamlit as st
from frontend_helper import *
import plotly.express as px

st.set_page_config(layout='wide',page_title='Electronics Analysis')

st.markdown("""
<style>

    /* Homepage Button Config */
    .hmpg-button {
        width: 29vw;
        height: 27vw;
        border-radius: 50%;
        font-size: 2vw;
        font-weight: 700;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #f9fafa;
        background-color: #262730;
        border: 1px solid #f9fafa;
        box-shadow: 0px 0px 2px 2px #f9fafa;
        position: relative;
        transition: all 300ms ease-in-out;
        left: 0;
        top: 0;
    }

    .hmpg-button:hover {
        left: 4px;
        top: 4px;
        box-shadow: 0px 0px 0px 0px white;
        cursor: pointer;
        color: #FF4B4B;
        border-color: #FF4B4B;
        background-color: #1a1b1e;
    }

    /* Category Button Config */
    .category-button {
        border-radius: 3%;
        font-size: 1.75vw;
        font-weight: 700;
        letter-spacing: 2px;
        border: none;
        background-color: #262730;
        position: relative;
        transition: background-color 400ms linear;
        transition: transform 400ms linear;        
    }

    .category-button:hover {
        cursor: pointer;
        color: white;
        background-color: #bb3b3b;
        transform: scale(0.975);
    }

    .btn-narrow-short{
        width: 25vw;
        height: 10vw;
        margin-left: 16vw !important; 
        margin-bottom: 0.1vw !important;
    }

    .btn-narrow-long{
        width: 25vw;
        height: 15vw;
        margin-left: 16vw !important; 
        margin-top: 0.1vw !important;
    }

    .btn-wide-short{
        width: 40vw;
        height: 10vw;  
        margin-left: -8.24vw !important;  
        margin-top: 0.1vw !important;
    }

    .btn-wide-long{
        width: 40vw;
        height: 15vw;    
        margin-left: -8.25vw !important;  
        margin-bottom: 0.1vw !important;
    }

</style>
""", unsafe_allow_html=True)

def laptops():
    st.markdown(category_page_config, unsafe_allow_html=True)
    create_header("Laptops")
    st.markdown("<div style='margin-bottom: 65px;'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        create_tile("category-button", "narrow-short", "laptop-first-name", "<span>Market Analysis</span>")
        create_tile("category-button", "narrow-long", "laptop-second-name", "<span>Second Function</span>")

    with col2:
        create_tile("category-button", "wide-long", "laptop-third-name", "<span>Third Function</span>")
        create_tile("category-button", "wide-short", "laptop-fourth-name", "<span>Fourth Function</span>")

def laptop_first():
    st.title("Laptops - Market Analysis")
    create_analysis_header("Laptop")

    ols_col1, ols_col2 = st.columns([2,1], gap='large')
    with ols_col1:
        try:
            ft_imp = ols_chart('laptops')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    with ols_col2:
        st.header("Feature Importance")
        st.text(f"""
        This bar chart highlights the relative importance of various specifications in determining a laptop's overall spec score.
        '{ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).index[0].capitalize()}' functionality stands out as the most influential factor, contributing approximately {ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).values[0][0]}% to the score. In contrast, '{ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).index[-1].capitalize()}' has minimal impact, accounting for just {ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).values[-1][0]}% of the overall importance.
        """)
    st.text('')
    st.text('')

    brand1, brand2 = st.columns([1.05,1], gap='small')
    with brand1:
        try:
            st.text('Brands with the most laptops with their GPU in the market. "Intel" GPU are all integrated GPUs. Among Dedicated GPUs, "Nvidia" leads.')
            plot_bar(get_data('http://127.0.0.1:5000/column?category=laptops&col=gpu_brand'), "Dominant GPU Brands")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with brand2:
        try:
            st.text('Brands with the most laptop models in the market. The top 5 brands cover about 70% laptops in the market according to our data.')
            plot_bar(get_data('http://127.0.0.1:5000/column?category=laptops&col=brand',n=20), "Dominant Laptop Brands")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    cpu1, cpu2 = st.columns([1,1.25], gap='small')
    with cpu1:
        try:
            plot_pie(get_data('http://127.0.0.1:5000/column?category=laptops&col=cpu_type',n=7), "CPU Types")
            st.text('Distribution of CPU types in the market. The low-end "H" and "U" are more available in the market, whereas the high-end "HX" and "HS" make up for only 10% of the laptops.')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with cpu2:
        try:
            plot_bar(get_data('http://127.0.0.1:5000/column?category=laptops&col=cpu_brand'), "Dominant CPU Brands")
            st.text('Brands with the most laptops with their CPU in the market. "Intel" is clearly the most dominant brand for CPUs.')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    ram, _, mt1, mt2, mt3 = st.columns([3,0.2,0.8,0.8,1], gap='small')
    with ram:
        try:
            st.text('Distribution of RAM DDR types in the market. The newer version "DDR5" is still not much adapted, but slowly gaining pace. As of now, the old "DDR4" remains dominant with 65% laptops.')
            plot_pie(get_data('http://127.0.0.1:5000/column?category=laptops&col=ram_ddr_type', n=7), "RAM DDR Types")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with mt1:
        st.text('')
        st.text('')
        st.text('')
        st.text('')
        st.text('')
        try:
            st.metric(label="Average Number of CPU Cores",
                      value=np.floor(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=cpu_cores', False))))
            st.text('')
            st.text('')
            st.text('')
            st.text('')
            st.metric(label="Average RAM Capacity",
                      value=np.floor(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=ram_capacity', False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with mt2:
        st.text('')
        st.text('')
        st.text('')
        st.text('')
        st.text('')
        try:
            st.metric(label="Average Number of CPU Threads",
                      value=np.floor(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=cpu_threads', False))))
            st.text('')
            st.text('')
            st.text('')
            st.text('')
            st.metric(label="Average Display Pixel Per Inch (PPI)",
                      value=np.floor(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=ppi', False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with mt3:
        st.text('')
        st.text('')
        st.text('')
        st.text('')
        st.text('')
        try:
            st.metric(label="Average GPU VRAM",
                      value=np.floor(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=gpu_vram', False))))
            st.text('')
            st.text('')
            st.text('')
            st.text('')
            st.metric(label="Most Common Aspect Ratio",
                      value=get_data('http://127.0.0.1:5000/column?category=laptops&col=aspect_ratio_category').sort_values(ascending=False).index[0])
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    screen1, screen2, screen3 = st.columns(3, gap='small')
    with screen1:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=laptops&col=touchscreen')
            data.index = data.index.map({0:'Does not have', 1:'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "Touchscreen Laptops",
                h=True)
            st.text("The count of laptops that don't have touchscreen vs that have touchscreen. About 90% of the laptops do not come with touchscreen functionality.")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with screen2:
        try:
            plot_box(get_data('http://127.0.0.1:5000/column?category=laptops&col=ppi', group_by=False), "PPI Distribution")
            st.text('Pixel Per Inch (PPI) of the screen. There are many high end laptops which provide more than 200 PPI. Generally, it seems PPI ranges from 130 to 170 for most laptops.')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with screen3:
        try:
            plot_box(get_data('http://127.0.0.1:5000/column?category=laptops&col=screen_size', group_by=False), "Distribution for Size of the Screen")
            st.text('Laptop screen sizes. Most laptops have screen size in the range of 13 to 17 inches.')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

def smartphones():
    st.markdown(category_page_config, unsafe_allow_html=True)
    create_header("Smartphones")
    st.markdown("<div style='margin-bottom: 65px;'></div>", unsafe_allow_html=True)

def tablets():
    st.markdown(category_page_config, unsafe_allow_html=True)
    create_header("Tablets")
    st.markdown("<div style='margin-bottom: 65px;'></div>", unsafe_allow_html=True)

def main_screen():
    st.markdown("<h1 style='text-align: center; color: white;'>Let's <span style='color: #FF4B4B;'>Analyze</span>!</h1>", unsafe_allow_html=True)
    st.markdown("<h2 style='text-align: center; color: white;'><span style='color: #FF4B4B;'>Choose</span> A Category:</h2>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 100px;'></div>", unsafe_allow_html=True)

    st.markdown("""
    <style>
    .block-container { 
        padding-left: 30px !important;
        margin-left: 0px !important;
        margin-right: 0px !important;
        padding-right: 0px !important;
        padding-top: 30px !important;
        margin-top: 0px !important;
        padding-bottom: 0px !important;
        margin-bottom: 0px !important;
        max-width: 100% !important;
    }
    </style>
    """, unsafe_allow_html=True)

    btn1, btn2, btn3 = st.columns(3, gap='large')

    with btn1:
        st.markdown(btn_str.format(class_ = "hmpg-button", btn_name = 'l', value = 'laptop', text = 'Laptops'), unsafe_allow_html=True)
    with btn2:
        st.markdown(btn_str.format(class_ = "hmpg-button", btn_name = 's', value = 'smartphone', text = 'Smartphones'), unsafe_allow_html=True)
    with btn3:
        st.markdown(btn_str.format(class_ = "hmpg-button", btn_name = 't', value = 'tablet', text = 'Tablets'), unsafe_allow_html=True)

if st.query_params:
    selected = st.query_params['button']

    if selected == "laptop":
        laptops()
    elif selected == "smartphone":
        smartphones()
    elif selected == "tablet":
        tablets()
    elif selected == "laptop-first-name":
        laptop_first()
    else:
        main_screen()

else:
    main_screen()