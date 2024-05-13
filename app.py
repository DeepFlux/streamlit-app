import os
import pandas as pd
import warnings
import sys
import streamlit as st
import unidecode
from sentry_sdk import capture_exception
from PIL import Image
from io import BytesIO
import pygwalker as pyg
import streamlit.components.v1 as components
import matplotlib.pyplot as plt

st.set_page_config(layout="wide")


from pygwalker.api.streamlit import init_streamlit_comm, get_streamlit_html
init_streamlit_comm()



def get_pyg_html(df: pd.DataFrame) -> str:
    return get_streamlit_html(df)

warnings.filterwarnings("ignore")
# Get the directory of the current file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path relative to the current file
# For example, if the directory to add is the parent directory of the current file
parent_dir = os.path.join(current_dir, "..")

# Add the parent directory to sys.path
sys.path.insert(0, parent_dir)

from llm import create_agent, dbchain_movies, generate_query
# from gen_final_output import display_text_with_images
from utils import raw_query

tab_titles = ['PivotConsult KFC', 'Data Walk']
tabs = st.tabs(tab_titles)

with tabs[0]:
    st.title("KFC Consult Chatbot")

with tabs[1]:
    st.title("Data Walk")

# st.set_page_config(page_title="PivotConsult Movie")
#Add logo on the top
# Define the correct password
PASSWORD = "Pivot@234"

# Function to check if the entered password is correct
def authenticate(password):
    return password == PASSWORD

# Function to reset the password input field
def reset_password_field():
    st.session_state.password = ""

# Check if the user is authenticated
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# Check if the user is authenticated
with tabs[0]:
    image_path = "./Consult-Logo.png"  
    image = Image.open(image_path)
    with tabs[0]:
        st.image(image, use_column_width=False, width=200)

    # Add image of Box Office Chatbot at the top
    image_path = "./videotape-with-3d-glasses-cinema-tickets.jpg"  
    image = Image.open(image_path)
    image = image.resize((int(image.width * 0.5), int(image.height * 0.5)))  # Reduce the size of the image to 50%
    with tabs[0]:
        st.image(image, use_column_width=True)
        
    if not st.session_state.authenticated:
        st.title("Authentication Required")
        password = st.text_input("Please enter the password:", type="password", value=st.session_state.get("password", ""))
        st.session_state.password = password

        if st.button("Authenticate"):
            if authenticate(password):
                st.session_state.authenticated = True
                st.success("Authentication successful!")
            else:
                st.error("Authentication failed. Please try again.")


    

# Create a Streamlit sidebar
st.sidebar.title("App Instructions")

# Add directions on how to use the app
st.sidebar.markdown("1. Enter your password to authenticate.")
st.sidebar.markdown("2. Use the chat interface to interact with the chatbot.")
st.sidebar.markdown("3. Ask questions on Revenue, Advance Collection, Occupancy Percentage, Occupied Seats, Average Ticket Price and Others.")
st.sidebar.markdown("4. Once the Query is generated, modify the query if needed and hit Run Query button.")


def reset_conversation():
    st.session_state.messages = []
    st.session_state.agent = create_agent()

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

if "agent" not in st.session_state:
    st.session_state.agent = create_agent()

with tabs[0]:
    st.title("PivotConsult Movie Data Chatbot")
    col1, col2 = st.columns([3, 1])
    with col2:
        st.button("Reset Chat", on_click=reset_conversation)

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                if isinstance(message["content"], pd.DataFrame):
                    st.table(message["content"])
                else:
                    st.markdown(message["content"])
            else:
                st.markdown(message["content"])

    if "current_query" not in st.session_state:
        st.session_state.current_query = ""

    if "updated_query" not in st.session_state:
        st.session_state.updated_query = ""

def change_query():
    st.session_state.current_query = st.session_state.updated_query

# Accept user input
# query_editable = st.session_state.get("query_editable", "")
# if prompt := st.chat_input("Please ask your question"):
if st.session_state.authenticated and (prompt := st.chat_input("Please ask your question like What is Day 1 Revenue of Animal")):
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    #### Changes
    result_query = dbchain_movies.invoke(prompt)
    query = generate_query(result_query)
    st.session_state.updated_query = query

# Display the generated SQL query
if st.session_state.authenticated and prompt:
    st.text_area("Generated SQL Query:", key="updated_query", value=st.session_state.updated_query, on_change=change_query, height=200)
st.session_state.updated_query

# Execute the SQL query when the "Run Query" button is clicked
if st.button("Run Query"):
    run_query = st.session_state.get("updated_query", "")  # Define query_editable here
    if run_query.strip():  # Check if query_editable is not empty
        print('button_clicked')
        with st.spinner("Running query..."):
            try:
                print('Calling raw_query()')
                result_data = raw_query(run_query, as_df=True)  # Execute the query and return as DataFrame#print(result_data.shape)
                # with st.chat_message("assistant"):
                #     if isinstance(result_data, pd.DataFrame):
                #         st.table(result_data)  # Display the result as a table
                #     else:
                #         st.markdown(result_data)  # Display the result as markdown
                #     st.session_state.messages.append({"role": "assistant", "content": result_data})  # Add assistant response to chat history
                if isinstance(result_data, pd.DataFrame):
                    try:
                        result_data=result_data.rename(columns={'date':'Date'})
                        result_data['Date'] = pd.to_datetime(result_data['Date']).dt.strftime('%Y/%m/%d')
                    except:
                        pass 
                else:
                    pass                
                       
                with st.chat_message("assistant"):
                    if isinstance(result_data, pd.DataFrame):
                        st.table(result_data)  # Display the result as a table
                    else:
                        st.markdown(result_data)  # Display the result as markdown
                    st.session_state.messages.append({"role": "assistant", "content": result_data})  # Add assistant response to chat history
                    with tabs[1]:
                        st.caption('Drag the fields from the Fields List to create the charts in the respective chart components as required')
                        html = get_pyg_html(result_data)
                        components.html(html, height=800)
            except Exception as e:
                st.error(f"Error executing the query: {e}")
    else:
        st.error("Please enter a valid SQL query before running.")