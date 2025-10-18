import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objs as go

# Function to load and preprocess the data
def load_data(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path, parse_dates=['date'])
    return df

# Main Streamlit app
def main():
    st.title('Cluster Node Usage Dashboard')

    # File uploader
    # uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if True:#uploaded_file is not None:
        # Load the data
        # df = load_data(uploaded_file)
        df = load_data("queue_daily_usage.csv")
        
        # Sidebar for queue selection
        st.sidebar.header('Queue Selection')
        
        # Get unique queues
        unique_queues = df['queuetotal'].unique().tolist()
        
        # Multiselect for queues
        selected_queues = st.sidebar.multiselect(
            'Select Queues to Analyze',
            unique_queues,
            default=unique_queues
        )
        
        # Filter data based on selected queues
        filtered_df = df[df['queuetotal'].isin(selected_queues)]
        
        # Group by date and calculate mean utilization
        # grouped_df = filtered_df.groupby('date')['util'].mean().reset_index()
        # rather than mean the util, we need to sum cores_util, cores_total
        grouped_df = filtered_df.groupby('date').agg({'cores_util':'sum', 'cores_total':'sum'}).reset_index()
        grouped_df['util'] = grouped_df['cores_util'] / grouped_df['cores_total']
        
        # Create interactive plotly line chart
        fig = px.line(
            grouped_df, 
            x='date', 
            y='util',
            title=f'Average Daily Utilization for Selected Queues',
            labels={'util': 'Average Utilization', 'date': 'Date'}
        )
        
        # Customize the chart
        fig.update_layout(
            xaxis_title='Date',
            yaxis_title='Average Utilization',
            hovermode='x unified'
        )
        
        # Display the chart
        st.plotly_chart(fig, use_container_width=True)
        
        # Optional: Display summary statistics
        st.subheader('Summary Statistics')
        st.dataframe(grouped_df)

if __name__ == '__main__':
    main()