import dash
from dash import html, dcc, callback, Input, Output
import dash_mantine_components as dmc

dash.register_page(__name__, path='/')

layout = dmc.Container([
    dmc.Grid([
        dmc.Col([
            html.H1("VIE Scraper Dashboard"),
            html.Hr(),
            dmc.Button("Start Scraping", id="start-scrape-btn", color="primary"),
            html.Div(id="scrape-status")
        ])
    ])
])
