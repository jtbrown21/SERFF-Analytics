"""
Competitive Intelligence Dashboard for Insurance Rate Filings
Built with Plotly Dash for rapid prototyping and internal use
"""

import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import duckdb
from datetime import datetime, timedelta
import numpy as np

# Initialize Dash app
app = dash.Dash(__name__)

# Database connection - update with your DuckDB file path
DB_PATH = "data/insurance_filings.db"

# Top 10 carriers for focus
TOP_CARRIERS = [
    'Allstate',
    'State Farm Insurance Cos.',
    'USAA',
    'Berkshire Hathaway Group',
    'Liberty Mutual Group',
    'Farmers Insurance Group',
    'Nationwide Insurance',
    'The Hartford Ins. Group',
    'Farmers',
    'Allstate Ins Grp'
]

# Color palette for carriers (consistent across charts)
CARRIER_COLORS = {
    'Allstate': '#003282',
    'State Farm Insurance Cos.': '#E31837',
    'USAA': '#002855',
    'Berkshire Hathaway Group': '#70653E',
    'Liberty Mutual Group': '#FDB913',
    'Farmers Insurance Group': '#CE1126',
    'Nationwide Insurance': '#0038A8',
    'The Hartford Ins. Group': '#CC0000',
    'Farmers': '#8B0000',
    'Allstate Ins Grp': '#4169E1'
}

def get_data(query):
    """Execute query and return pandas DataFrame"""
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute(query).fetchdf()
    conn.close()
    return df

# Layout
app.layout = html.Div([
    html.Div([
        html.H1("Competitive Intelligence Dashboard", 
                style={'textAlign': 'center', 'marginBottom': 30}),
        html.P("Insurance Rate Filing Analysis - Top 10 Carriers", 
               style={'textAlign': 'center', 'fontSize': 18, 'color': '#666'})
    ]),
    
    # Control Panel
    html.Div([
        html.Div([
            html.Label("Select Carriers to Compare:"),
            dcc.Dropdown(
                id='carrier-selector',
                options=[{'label': c, 'value': c} for c in TOP_CARRIERS],
                value=TOP_CARRIERS[:5],  # Default to top 5
                multi=True,
                style={'width': '100%'}
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
        
        html.Div([
            html.Label("Time Period:"),
            dcc.DatePickerRange(
                id='date-range',
                start_date='2020-01-01',
                end_date=datetime.now().date(),
                display_format='YYYY-MM-DD',
                style={'width': '100%'}
            )
        ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'})
    ], style={'marginBottom': 30}),
    
    # KPI Cards
    html.Div(id='kpi-cards', style={'marginBottom': 30}),
    
    # Main Charts
    html.Div([
        # Performance Index Chart
        dcc.Graph(id='performance-index-chart', style={'height': '500px'})
    ], style={'marginBottom': 30}),
    
    html.Div([
        # Market Share Evolution
        html.Div([
            dcc.Graph(id='market-share-chart', style={'height': '400px'})
        ], style={'width': '48%', 'display': 'inline-block'}),
        
        # Competitive Positioning Matrix
        html.Div([
            dcc.Graph(id='positioning-matrix', style={'height': '400px'})
        ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'})
    ], style={'marginBottom': 30}),
    
    # State Heatmap
    html.Div([
        dcc.Graph(id='state-heatmap', style={'height': '600px'})
    ], style={'marginBottom': 30}),
    
    # Recent Competitive Moves Table
    html.Div([
        html.H3("Recent Competitive Moves", style={'marginBottom': 15}),
        html.Div(id='competitive-moves-table')
    ])
])

# Callbacks
@callback(
    Output('kpi-cards', 'children'),
    [Input('carrier-selector', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date')]
)
def update_kpi_cards(selected_carriers, start_date, end_date):
    """Update KPI summary cards"""
    if not selected_carriers:
        return html.Div("Please select at least one carrier")
    
    # Query for KPIs
    carrier_list = "','".join(selected_carriers)
    query = f"""
    SELECT 
        COUNT(DISTINCT Company) as carriers_analyzed,
        COUNT(*) as total_filings,
        AVG(Premium_Change_Number) as avg_rate_change,
        SUM(Policyholders_Affected_Number) as total_customers_affected
    FROM filings
    WHERE Company IN ('{carrier_list}')
        AND Effective_Date BETWEEN '{start_date}' AND '{end_date}'
    """
    
    df = get_data(query)
    
    cards = []
    kpis = [
        ('Carriers Analyzed', df['carriers_analyzed'].iloc[0], ''),
        ('Total Filings', f"{df['total_filings'].iloc[0]:,}", ''),
        ('Avg Rate Change', f"{df['avg_rate_change'].iloc[0]:.2f}%", 
         '↑' if df['avg_rate_change'].iloc[0] > 0 else '↓'),
        ('Customers Affected', f"{df['total_customers_affected'].iloc[0]:,.0f}", '')
    ]
    
    for title, value, arrow in kpis:
        card = html.Div([
            html.H4(title, style={'margin': 5, 'fontSize': 14, 'color': '#666'}),
            html.H2([value, html.Span(arrow, style={'color': 'red' if arrow == '↑' else 'green'})], 
                   style={'margin': 5})
        ], style={
            'backgroundColor': '#f8f9fa',
            'padding': 20,
            'borderRadius': 8,
            'width': '23%',
            'display': 'inline-block',
            'margin': '0 1%',
            'textAlign': 'center'
        })
        cards.append(card)
    
    return cards

@callback(
    Output('performance-index-chart', 'figure'),
    [Input('carrier-selector', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date')]
)
def update_performance_index(selected_carriers, start_date, end_date):
    """Update carrier performance index chart"""
    if not selected_carriers:
        return go.Figure()
    
    df = get_data("SELECT * FROM carrier_performance_index")
    df['month'] = pd.to_datetime(df['month'])
    
    # Filter data
    df = df[df['Company'].isin(selected_carriers)]
    df = df[(df['month'] >= start_date) & (df['month'] <= end_date)]
    
    fig = go.Figure()
    
    for carrier in selected_carriers:
        carrier_data = df[df['Company'] == carrier]
        fig.add_trace(go.Scatter(
            x=carrier_data['month'],
            y=carrier_data['performance_index'],
            name=carrier,
            mode='lines+markers',
            line=dict(color=CARRIER_COLORS.get(carrier, '#333'), width=2),
            hovertemplate='%{x|%b %Y}<br>Index: %{y:.1f}<br>Rate: %{customdata[0]:.2f}%<br>Filings: %{customdata[1]}<extra></extra>',
            customdata=carrier_data[['avg_rate_change', 'filing_count']]
        ))
    
    fig.update_layout(
        title="Carrier Performance Index (Base 100 = Q1 2020)",
        xaxis_title="Date",
        yaxis_title="Performance Index",
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor='white',
        yaxis=dict(gridcolor='lightgray')
    )
    
    # Add reference line at 100
    fig.add_hline(y=100, line_dash="dash", line_color="gray", 
                  annotation_text="Baseline", annotation_position="right")
    
    return fig

@callback(
    Output('market-share-chart', 'figure'),
    [Input('carrier-selector', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date')]
)
def update_market_share(selected_carriers, start_date, end_date):
    """Update market share evolution chart"""
    if not selected_carriers:
        return go.Figure()
    
    df = get_data("SELECT * FROM market_share_evolution")
    df['quarter'] = pd.to_datetime(df['quarter'])
    
    # Filter and pivot data
    df = df[df['Company'].isin(selected_carriers)]
    df = df[(df['quarter'] >= start_date) & (df['quarter'] <= end_date)]
    
    # Create stacked area chart
    fig = go.Figure()
    
    for carrier in selected_carriers:
        carrier_data = df[df['Company'] == carrier].sort_values('quarter')
        fig.add_trace(go.Scatter(
            x=carrier_data['quarter'],
            y=carrier_data['market_share_pct'],
            name=carrier,
            mode='lines',
            stackgroup='one',
            fillcolor=CARRIER_COLORS.get(carrier, '#333'),
            line=dict(width=0.5, color=CARRIER_COLORS.get(carrier, '#333')),
            hovertemplate='%{y:.2f}%<extra></extra>'
        ))
    
    fig.update_layout(
        title="Market Share Evolution (% of Total Premium)",
        xaxis_title="Quarter",
        yaxis_title="Market Share %",
        hovermode='x unified',
        plot_bgcolor='white',
        yaxis=dict(gridcolor='lightgray', tickformat='.1f')
    )
    
    return fig

@callback(
    Output('positioning-matrix', 'figure'),
    [Input('carrier-selector', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date')]
)
def update_positioning_matrix(selected_carriers, start_date, end_date):
    """Update competitive positioning scatter plot"""
    if not selected_carriers:
        return go.Figure()
    
    # Get latest quarter data
    query = f"""
    SELECT 
        Company,
        avg_rate_change,
        premium_volume,
        filing_activity,
        aggressiveness_score
    FROM competitive_positioning
    WHERE Company IN ('{"','".join(selected_carriers)}')
        AND quarter = (SELECT MAX(quarter) FROM competitive_positioning)
    """
    
    df = get_data(query)
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['avg_rate_change'],
        y=df['aggressiveness_score'],
        mode='markers+text',
        marker=dict(
            size=df['premium_volume'] / df['premium_volume'].max() * 100,
            color=[CARRIER_COLORS.get(c, '#333') for c in df['Company']],
            line=dict(width=2, color='white')
        ),
        text=df['Company'],
        textposition="top center",
        hovertemplate='<b>%{text}</b><br>Avg Rate Change: %{x:.2f}%<br>Aggressiveness: %{y:.1f}<br>Premium: $%{customdata:,.0f}<extra></extra>',
        customdata=df['premium_volume']
    ))
    
    fig.update_layout(
        title="Competitive Positioning Matrix (Latest Quarter)",
        xaxis_title="Average Rate Change %",
        yaxis_title="Competitive Aggressiveness Score",
        plot_bgcolor='white',
        xaxis=dict(gridcolor='lightgray', zeroline=True, zerolinecolor='black', zerolinewidth=1),
        yaxis=dict(gridcolor='lightgray'),
        showlegend=False
    )
    
    # Add quadrant labels
    fig.add_annotation(x=5, y=80, text="Aggressive Growth", showarrow=False, font=dict(size=12, color="gray"))
    fig.add_annotation(x=-5, y=80, text="Aggressive Competition", showarrow=False, font=dict(size=12, color="gray"))
    fig.add_annotation(x=5, y=20, text="Conservative Growth", showarrow=False, font=dict(size=12, color="gray"))
    fig.add_annotation(x=-5, y=20, text="Conservative Competition", showarrow=False, font=dict(size=12, color="gray"))
    
    return fig

@callback(
    Output('state-heatmap', 'figure'),
    [Input('carrier-selector', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date')]
)
def update_state_heatmap(selected_carriers, start_date, end_date):
    """Update state-level competitive intensity heatmap"""
    if not selected_carriers:
        return go.Figure()
    
    # Get state-level competition data
    query = f"""
    SELECT 
        State,
        Company,
        AVG(Premium_Change_Number) as avg_rate_change
    FROM filings
    WHERE Company IN ('{"','".join(selected_carriers)}')
        AND Effective_Date BETWEEN '{start_date}' AND '{end_date}'
        AND State IS NOT NULL
    GROUP BY State, Company
    ORDER BY State, Company
    """
    
    df = get_data(query)
    
    # Pivot for heatmap
    pivot_df = df.pivot(index='Company', columns='State', values='avg_rate_change')
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='RdBu_r',
        zmid=0,
        text=np.round(pivot_df.values, 2),
        texttemplate='%{text}',
        textfont={"size": 10},
        hoverongaps=False,
        hovertemplate='State: %{x}<br>Carrier: %{y}<br>Avg Rate Change: %{z:.2f}%<extra></extra>'
    ))
    
    fig.update_layout(
        title="State-Level Rate Changes by Carrier",
        xaxis_title="State",
        yaxis_title="Carrier",
        height=600,
        xaxis={'side': 'bottom'},
        yaxis={'autorange': 'reversed'}
    )
    
    return fig

@callback(
    Output('competitive-moves-table', 'children'),
    [Input('carrier-selector', 'value')]
)
def update_competitive_moves(selected_carriers):
    """Update recent competitive moves table"""
    if not selected_carriers:
        return html.Div("Please select at least one carrier")
    
    df = get_data("SELECT * FROM competitive_alerts LIMIT 20")
    df = df[df['Company'].isin(selected_carriers)]
    
    # Format the table
    table_header = [
        html.Thead([
            html.Tr([
                html.Th("Date"),
                html.Th("Carrier"),
                html.Th("State"),
                html.Th("Product"),
                html.Th("Rate Change"),
                html.Th("Impact"),
                html.Th("Type")
            ])
        ])
    ]
    
    table_rows = []
    for _, row in df.iterrows():
        color = 'red' if row['Premium_Change_Number'] > 0 else 'green'
        table_rows.append(
            html.Tr([
                html.Td(row['Effective_Date'].strftime('%Y-%m-%d')),
                html.Td(row['Company']),
                html.Td(row['State']),
                html.Td(row['Product_Line']),
                html.Td(f"{row['Premium_Change_Number']:.2f}%", style={'color': color, 'fontWeight': 'bold'}),
                html.Td(f"{row['impact_score']:.1f}"),
                html.Td(row['move_type'])
            ])
        )
    
    table_body = [html.Tbody(table_rows)]
    
    return html.Table(
        table_header + table_body,
        style={'width': '100%', 'fontSize': 14},
        className='table table-striped'
    )

if __name__ == '__main__':
    app.run(debug=True)