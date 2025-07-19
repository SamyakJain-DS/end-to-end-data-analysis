import pandas as pd
import numpy as np
import requests
import streamlit as st
from frontend_helper import *
import plotly.express as px
from scipy.cluster.hierarchy import leaves_list

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
        create_tile("category-button", "narrow-short", "laptops-market", f"<span>{f1}</span>")
        create_tile("category-button", "narrow-long", "laptops-brand", f"<span>{f2}</span>")

    with col2:
        create_tile("category-button", "wide-long", "laptops-price", f"<span>{f3}</span>")
        create_tile("category-button", "wide-short", "laptops-spec", f"<span>{f4}</span>")

def laptops_f1():
    st.title(f"Laptops - :red[{f1}]")
    create_analysis_header("Laptops")

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
    leave_lines(2)

    txt1, txt2, txt3 = st.columns(3, gap='small')
    with txt1:
        st.text("The count of laptops that don't have touchscreen vs that have touchscreen. About 90% of the laptops do not come with touchscreen functionality.")
    with txt2:
        st.text('Pixel Per Inch (PPI) of the screen. There are many high end laptops which provide more than 200 PPI. Generally, it seems PPI ranges from 130 to 170 for most laptops.')
    with txt3:
        st.text('Laptop screen sizes. Most laptops have screen size in the range of 13 to 17 inches.')

    screen1, screen2, screen3 = st.columns(3, gap='small')
    with screen1:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=laptops&col=touchscreen')
            data.index = data.index.map({0:'Does not have', 1:'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "Touchscreen Laptops",
                h=True)
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with screen2:
        try:
            plot_box(get_data('http://127.0.0.1:5000/column?category=laptops&col=ppi', group_by=False), "PPI Distribution")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with screen3:
        try:
            plot_box(get_data('http://127.0.0.1:5000/column?category=laptops&col=screen_size', group_by=False), "Distribution for Size of the Screen (Inches)")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

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
            plot_pie(get_data('http://127.0.0.1:5000/column?category=laptops&col=ram_ddr_type', n=7), "RAM DDR Types")
            st.text('Distribution of RAM DDR types in the market. The newer version "DDR5" is still not much adapted, but slowly gaining pace. As of now, the old "DDR4" remains dominant with 65% laptops.')

        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with mt1:
        leave_lines(3)
        try:
            st.metric(label="Average Number of CPU Cores",
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=cpu_cores', False))))
            leave_lines(4)
            st.metric(label="Average RAM Capacity (GB)",
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=ram_capacity', False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with mt2:
        leave_lines(3)
        try:
            st.metric(label="Average Number of CPU Threads",
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=cpu_threads', False))))
            leave_lines(4)
            st.metric(label="Average Display Pixel Per Inch (PPI)",
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=ppi', False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with mt3:
        leave_lines(3)
        try:
            st.metric(label="Average GPU VRAM (GB)",
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=laptops&col=gpu_vram', False))))
            leave_lines(4)
            st.metric(label="Most Common Aspect Ratio",
                      value=get_data('http://127.0.0.1:5000/column?category=laptops&col=aspect_ratio_category').sort_values(ascending=False).index[0])
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

def laptops_f2():
    st.title(f"Laptops - :red[{f2}]")
    create_analysis_header("Laptops")

    try:
        brands = get_data('http://127.0.0.1:5000/column?category=laptops&col=brand')
    except:
        st.text('Server not responding. Please initiate the server before continuing.')

    brands_list = brands.sort_values(ascending=False)[:12].index.str.capitalize().tolist()

    leave_lines(2)

    dropbox, _ = st.columns([1,2])
    with dropbox:
        brand = st.selectbox(
            "Choose the Brand you want to analyze! (These are only the top few brands)",
            brands_list,
            placeholder="Select Brand To Analyze"
        )

    leave_lines(2)

    ols_col1, ols_col2 = st.columns([2,1], gap='large')
    with ols_col1:
        try:
            ft_imp = ols_chart('laptops', brand=brand)
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    with ols_col2:
        st.header("Feature Importance")
        st.text(f"""
        This bar chart highlights the relative importance of various specifications in determining a laptop's overall spec score.
        '{ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).index[0].capitalize()}' functionality stands out as the most influential factor, contributing approximately {ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).values[0][0]}% to the score. In contrast, '{ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).index[-1].capitalize()}' has minimal impact, accounting for just {ft_imp.sort_values(by = 'Feature Importance (%)', ascending=False).values[-1][0]}% of the overall importance.
        """)

def laptops_f3():
    st.title(f"Laptops - :red[{f3}]")
    create_analysis_header("Laptops")

def laptops_f4():
    st.title(f"Laptops - :red[{f4}]")
    create_analysis_header("Laptops")

def smartphones():
    st.markdown(category_page_config, unsafe_allow_html=True)
    create_header("Smartphones")
    st.markdown("<div style='margin-bottom: 65px;'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        create_tile("category-button", "narrow-short", "smartphones-market", f"<span>{f1}</span>")
        create_tile("category-button", "narrow-long", "smartphones-brand", f"<span>{f2}</span>")

    with col2:
        create_tile("category-button", "wide-long", "smartphones-price", f"<span>{f3}</span>")
        create_tile("category-button", "wide-short", "smartphones-spec", f"<span>{f4}</span>")

def smartphones_f1():
    st.title(f"Smartphones - :red[{f1}]")
    create_analysis_header("Smartphones")

    ols_col1, ols_col2 = st.columns([2, 1], gap='large')
    with ols_col1:
        try:
            ft_imp = ols_chart('smartphones')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    with ols_col2:
        st.header("Feature Importance")
        st.text(f"""
            This bar chart highlights the relative importance of various specifications in determining a smartphone's overall spec score.
            '{ft_imp.sort_values(by='Feature Importance (%)', ascending=False).index[0].capitalize()}' functionality stands out as the most influential factor, contributing approximately {ft_imp.sort_values(by='Feature Importance (%)', ascending=False).values[0][0]}% to the score. In contrast, '{ft_imp.sort_values(by='Feature Importance (%)', ascending=False).index[-1].capitalize()}' has minimal impact, accounting for just {ft_imp.sort_values(by='Feature Importance (%)', ascending=False).values[-1][0]}% of the overall importance.
            """)
    leave_lines(2)

    sc1, _, card, sc2 = st.columns([2.4, 0.1, 1, 2.5], gap='small')
    with sc1:
        try:
            st.text('Different Screen Resolutions. Majority phones have "HD+" or "FHD+" (60%+)')
            plot_pie(get_data('http://127.0.0.1:5000/column?category=mobiles&col=screen_res'), "Screen Resolutions")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    with card:
        try:
            leave_lines(6)
            st.metric(label='Most Common Aspect Ratio',
                      value=get_data('http://127.0.0.1:5000/column?category=mobiles&col=aspect_ratio_category').sort_values(ascending=False).index[0])
            leave_lines(10)
            st.metric(label='Expected Front Cameras',
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=mobiles&col=front_cameras', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with sc2:
        try:
            st.text('Screen Size Distribution. Most phones have a screen of size between 5.5 and 7 inches.')
            plot_box(get_data('http://127.0.0.1:5000/column?category=mobiles&col=screen_size', group_by=False), "Distribution for Screen Size (Inches)")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    sc3, _, card2, sc4 = st.columns([2.4, 0.1, 1, 2.5], gap='small')
    with sc3:
        try:
            st.text('PPI Distribution. Most phones have PPI between 250 and 450.')
            plot_box(get_data('http://127.0.0.1:5000/column?category=mobiles&col=ppi', group_by=False), "Distribution for Pixels Per Inch (PPI)")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card2:
        try:
            leave_lines(6)
            st.metric(label='Expected Rear Cameras',
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=mobiles&col=rear_cameras', group_by=False))))
            leave_lines(10)
            st.metric(label='Expected MP for Front Cameras',
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=mobiles&col=front_primary', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with sc4:
        try:
            st.text('Screen Refresh Rates. Most phones still have a 60 Hz screen (60%).')
            plot_pie(get_data('http://127.0.0.1:5000/column?category=mobiles&col=refresh_rate'), "Screen Refresh Rates")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    leave_lines(2)
    card1, card2, card3, card4, card5 = st.columns(5, gap='large')
    with card1:
        try:
            st.metric(label='Expected MP for Rear Cameras',
                    value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=mobiles&col=front_primary', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card2:
        try:
            st.metric(label='Expected CPU Cores',
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=mobiles&col=cpu_cores', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card3:
        try:
            st.metric(label='Expected CPU Speed (GHz)',
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=mobiles&col=cpu_speed', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card4:
        try:
            st.metric(label='Expected RAM (GB)',
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=mobiles&col=ram', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card5:
        try:
            st.metric(label='Expected Storage (GB)',
                      value=np.round(np.mean(get_data('http://127.0.0.1:5000/column?category=mobiles&col=storage', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    horz1, horz2, horz3 = st.columns(3, gap='large')
    with horz1:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=mobiles&col=5g')
            data.index = data.index.map({0: 'Does not have', 1: 'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "5G Mobiles",
                h=True)
            st.text('Proportion of 5G mobiles. About 60% still do not have 5G.')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with horz2:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=mobiles&col=nfc')
            data.index = data.index.map({0: 'Does not have', 1: 'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "NFC feature",
                h=True)
            st.text('Mobiles with NFC feature. About 60% do not have NFC feature.')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with horz3:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=mobiles&col=ir_blaster')
            data.index = data.index.map({0: 'Does not have', 1: 'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "IR Blaster feature",
                h=True)
            st.text('Mobiles with IR Blaster feature. About 90% do not have IR Blaster feature.')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    bar1, bar2 = st.columns([1.5,1])
    with bar1:
        try:
            st.text('Mobile brands in the market. One-Third of the smartphones in the market belong the only the first seven brands.')
            plot_bar(get_data('http://127.0.0.1:5000/column?category=mobiles&col=brand',n=20), "Dominant Mobiles Brands")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with bar2:
        try:
            st.text('Almost all mobiles in the data have android OS.')
            plot_bar(get_data('http://127.0.0.1:5000/column?category=mobiles&col=os'), "Operating System")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    leave_lines(1)

    bar3, bar4 = st.columns([1,1.5])
    with bar3:
        try:
            plot_box(get_data('http://127.0.0.1:5000/column?category=mobiles&col=battery', group_by=False), "Battery Capacity (mAh)")
            st.text('There are extreme outliers. Usually, battery capacity lies between 2000 and 6000 mAh.')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with bar4:
        try:
            plot_bar(get_data('http://127.0.0.1:5000/column?category=mobiles&col=cpu_brand'), "CPU Brands")
            st.text('Dominant CPU brands in the market. Most phones have CPU of "Snapdragon", "Helio" or "Mediatek".')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

def smartphones_f2():
    st.title(f"Smartphones - :red[{f2}]")
    create_analysis_header("Smartphones")

def smartphones_f3():
    st.title(f"Smartphones - :red[{f3}]")
    create_analysis_header("Smartphones")

def smartphones_f4():
    st.title(f"Smartphones - :red[{f4}]")
    create_analysis_header("Smartphones")

def tablets():
    st.markdown(category_page_config, unsafe_allow_html=True)
    create_header("Tablets")
    st.markdown("<div style='margin-bottom: 65px;'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        create_tile("category-button", "narrow-short", "tablets-market", f"<span>{f1}</span>")
        create_tile("category-button", "narrow-long", "tablets-brand", f"<span>{f2}</span>")

    with col2:
        create_tile("category-button", "wide-long", "tablets-price", f"<span>{f3}</span>")
        create_tile("category-button", "wide-short", "tablets-spec", f"<span>{f4}</span>")

def tablets_f1():
    st.title(f"Tablets - :red[{f1}]")
    create_analysis_header("Tablets")

    ols_col1, ols_col2 = st.columns([2, 1], gap='large')
    with ols_col1:
        try:
            ft_imp = ols_chart('tablets')
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    with ols_col2:
        st.header("Feature Importance")
        st.text(f"""
            This bar chart highlights the relative importance of various specifications in determining a tablet's overall spec score.
            '{ft_imp.sort_values(by='Feature Importance (%)', ascending=False).index[0].capitalize()}' functionality stands out as the most influential factor, contributing approximately {ft_imp.sort_values(by='Feature Importance (%)', ascending=False).values[0][0]}% to the score. In contrast, '{ft_imp.sort_values(by='Feature Importance (%)', ascending=False).index[-1].capitalize()}' has minimal impact, accounting for just {ft_imp.sort_values(by='Feature Importance (%)', ascending=False).values[-1][0]}% of the overall importance.
            """)
    leave_lines(2)

    card1, card2, card3, card4, card5 = st.columns(5, gap='large')
    with card1:
        try:
            st.metric(label='Expected Front Cameras',
                      value=np.round(np.mean(
                          get_data('http://127.0.0.1:5000/column?category=tablets&col=front_cameras', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card2:
        try:
            st.metric(label='Expected Rear Cameras',
                      value=np.round(
                          np.mean(get_data('http://127.0.0.1:5000/column?category=tablets&col=rear_cameras', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card3:
        try:
            st.metric(label='Expected MP for Front Cameras',
                      value=np.round(
                          np.mean(get_data('http://127.0.0.1:5000/column?category=tablets&col=front_primary', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card4:
        try:
            st.metric(label='Expected MP for Rear Cameras',
                      value=np.round(
                          np.mean(get_data('http://127.0.0.1:5000/column?category=tablets&col=rear_primary', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card5:
        try:
            st.metric(label='Expected CPU Cores',
                      value=np.round(
                          np.mean(get_data('http://127.0.0.1:5000/column?category=tablets&col=cpu_cores', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    hz1, _, txt, __, hz2 = st.columns([2, 0.1, 1, 0.1, 2], gap='small')
    with hz1:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=tablets&col=has_5g')
            data.index = data.index.map({0: 'Does not have', 1: 'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "5G",
                h=True)
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with txt:
        leave_lines(6)
        st.text('Only about 8% tablets have a 5G Sim.')
        leave_lines(12)
        st.text('A good 65% of the tablets have a Sim holder.')
    with hz2:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=tablets&col=has_sim')
            data.index = data.index.map({0: 'Does not have', 1: 'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "SIM Feature",
                h=True)
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    _, card6, card7, card8, card9 = st.columns([0.5,0.75,0.75,0.75,1], gap='large')
    with card6:
        try:
            st.metric(label='Expected CPU Speed (GHz)',
                      value=np.round(
                          np.mean(get_data('http://127.0.0.1:5000/column?category=tablets&col=cpu_speed', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card7:
        try:
            st.metric(label='Common Aspect Ratio',
                      value=get_data('http://127.0.0.1:5000/column?category=mobiles&col=aspect_ratio_category').sort_values(ascending=False).index[0])
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card8:
        try:
            st.metric(label='Expected RAM (GB)',
                      value=np.round(
                          np.mean(get_data('http://127.0.0.1:5000/column?category=tablets&col=ram', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with card9:
        try:
            st.metric(label='Expected Storage (GB)',
                      value=np.round(
                          np.mean(get_data('http://127.0.0.1:5000/column?category=tablets&col=inbuilt_storage', group_by=False))))
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    hz3, _, txt2, __, hz4 = st.columns([2, 0.1, 1, 0.1, 2], gap='small')
    with hz3:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=tablets&col=has_nfc')
            data.index = data.index.map({0: 'Does not have', 1: 'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "NFC Feature",
                h=True)
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with txt2:
        leave_lines(6)
        st.text('Only about 3% tablets have a NFC Feature.')
        leave_lines(12)
        st.text('Only 0.5% of tablets here have an IR Blaster.')
    with hz4:
        try:
            data = get_data('http://127.0.0.1:5000/column?category=tablets&col=has_irblaster')
            data.index = data.index.map({0: 'Does not have', 1: 'Has'})
            plot_bar(
                data.sort_values(ascending=True),
                "IR Blaster Feature",
                h=True)
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    t1, t2, t3 = st.columns(3, gap='medium')
    with t1:
        st.text('About 42% tablets have a CPU from only the first 3 brands: "Helio", "Snapdragon", "Bionic".')
    with t2:
        st.text('Most tablets have a screen size between 6 inches and 12 inches. The largest tablet here has an 18 inch screen')
    with t3:
        st.text('Distribution for PPI. Most tablets have PPI in the range of 150 and 300.')

    cpu_b, box1, box2 = st.columns(3, gap='medium')
    with cpu_b:
        try:
            plot_bar(get_data('http://127.0.0.1:5000/column?category=tablets&col=cpu_brand'), "CPU Brands")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with box1:
        try:
            plot_box(get_data('http://127.0.0.1:5000/column?category=tablets&col=screen_size', group_by=False), "Distribution for Size of the Screen (Inches)")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with box2:
        try:
            plot_box(get_data('http://127.0.0.1:5000/column?category=tablets&col=ppi', group_by=False), "Distribution for Pixels Per Inch (PPI)")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    brd, os = st.columns(2, gap='medium')
    with brd:
        try:
            st.text('Most devices have Android for Operating System.')
            plot_bar(get_data('http://127.0.0.1:5000/column?category=tablets&col=os'), "Operating Systems")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with os:
        try:
            st.text('About 45% of tablets in the market are from the first 4 brands: "Apple","Lenovo","Samsung","iBall"')
            plot_bar(get_data('http://127.0.0.1:5000/column?category=tablets&col=brand', n=20), "Dominant Brands")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

    btry, chrg = st.columns(2, gap='large')
    with btry:
        try:
            st.text('Distribution for Battery Capacity. Most tablets have capacity in the range of 3,000 mAh and 9,000 mAh. There are a couple of tablets with capacity more than 20,000 mAh.')
            plot_box(get_data('http://127.0.0.1:5000/column?category=tablets&col=battery_capacity', group_by=False), "Battery Capacity (mAh)")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')
    with chrg:
        try:
            st.text('Distribution for Fast Charging Capacity. Most tablets do not have fast charging. Usually within 10Ws of Fast Charging. A few models have extremely fast charging capacity.')
            plot_box(get_data('http://127.0.0.1:5000/column?category=tablets&col=fast_charging', group_by=False), "Fast Charging (W)")
        except:
            st.text('Server not responding. Please initiate the server before continuing.')

def tablets_f2():
    st.title(f"Tablets - :red[{f2}]")
    create_analysis_header("Tablets")

def tablets_f3():
    st.title(f"Tablets - :red[{f3}]")
    create_analysis_header("Tablets")

def tablets_f4():
    st.title(f"Tablets - :red[{f4}]")
    create_analysis_header("Tablets")

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
        st.markdown(btn_str.format(class_ = "hmpg-button", btn_name = 'l', value = 'laptops', text = 'Laptops'), unsafe_allow_html=True)
    with btn2:
        st.markdown(btn_str.format(class_ = "hmpg-button", btn_name = 's', value = 'smartphones', text = 'Smartphones'), unsafe_allow_html=True)
    with btn3:
        st.markdown(btn_str.format(class_ = "hmpg-button", btn_name = 't', value = 'tablets', text = 'Tablets'), unsafe_allow_html=True)

if st.query_params:
    selected = st.query_params['button']

    if selected == "laptops":
        laptops()
    elif selected == "smartphones":
        smartphones()
    elif selected == "tablets":
        tablets()
    elif selected == "laptops-market":
        laptops_f1()
    elif selected == "laptops-brand":
        laptops_f2()
    elif selected == "laptops-price":
        laptops_f3()
    elif selected == "laptops-spec":
        laptops_f4()
    elif selected == "smartphones-market":
        smartphones_f1()
    elif selected == "smartphones-brand":
        smartphones_f2()
    elif selected == "smartphones-price":
        smartphones_f3()
    elif selected == "smartphones-spec":
        smartphones_f4()
    elif selected == "tablets-market":
        tablets_f1()
    elif selected == "tablets-brand":
        tablets_f2()
    elif selected == "tablets-price":
        tablets_f3()
    elif selected == "tablets-spec":
        tablets_f4()
    else:
        main_screen()
else:
    main_screen()