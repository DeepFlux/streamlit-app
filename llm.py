from langchain_openai import ChatOpenAI,OpenAI
from langchain_core.prompts import ChatPromptTemplate
 
from langchain_core.output_parsers import StrOutputParser
 
import time
from langchain.agents.format_scratchpad.openai_tools import format_to_openai_tool_messages
from langchain.agents.output_parsers.openai_tools import OpenAIToolsAgentOutputParser
 
from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferMemory
 
 
from langchain_core.prompts import (
    ChatPromptTemplate,
    FewShotPromptTemplate,
    MessagesPlaceholder,
    PromptTemplate,
    SystemMessagePromptTemplate,
)
 
from langchain_community.vectorstores import FAISS
from langchain_core.example_selectors import SemanticSimilarityExampleSelector
from langchain_openai import OpenAIEmbeddings
 
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain.chains import LLMChain
import pandas as pd
from sqlalchemy import create_engine
import io
 
from langchain.chains import create_sql_query_chain
 
from langchain.agents import (AgentExecutor, Tool, ZeroShotAgent,
                              initialize_agent, load_tools)
from langchain.utilities import PythonREPL
from langchain_experimental.tools import PythonREPLTool
import json
 
with open('config.json', 'r') as f:
    # Load the JSON data
    requirement_details = json.load(f)
 
open_ai_api_key = requirement_details['open_ai_api_key']
movies_table = requirement_details['table_name']
MOVIE_INFO_DATA_PROMPT=f"""
You are a PostgreSQL expert. Given an input question, form a correct PostgreSQL query to be used to retreive data by also using relevant information from chat history
Never query for all columns from a table. You must query only the columns that are needed to answer the question. Wrap each column name in double quotes (") to denote them as delimited identifiers.
To calculate occupancy percentage, use sum(occupied_seats)/sum(total_seats) as occupancy_perc
 
Pay attention include a 'GROUP BY' clause when utilizing aggregation functions like SUM(),MIN(),MAX(),AVG() in queries for accurate data grouping across multiple columns.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist.
 
Release date is the day on which movie is released. In the data every movie has a distinct release date which will be the minimum show_date that particular movie has.
 
First weekend is the first 3 days of from the release of the movie.
 
Pay attention to use functions like EXTRACT(ISODOW FROM SHOW_DATE) whenever asked for a particular day of the week
Pay attention to not use aggregation functions like SUM(), AVG() within the MAX() or MIN() function, instead retrieve the required data first and then do the MAX() or MIN() over that
 
 
 
Only use the following tables:
CREATE TABLE {movies_table} (
movie_name VARCHAR(64),
show_date DATE,
crawl_date DATE,
city_name VARCHAR(64),
total_seats BIGINT,
occupied_seats BIGINT,
shows BIGINT,
total_rev_in_cr NUMERIC,
category_rev NUMERIC,
avgprice NUMERIC,
release_date DATE
)
 
/*
movie_name show_date city_name total_seats occupied_seats shows occupancy_perc total_rev_in_cr category_rev avgprice
"12th Fail" "2023-10-28"    "2023-10-28"    "Ahmedabad" 10629   1548    52  14  0.031   314950  203.46
"12th Fail" "2023-10-28"    "2023-10-28"    "Amritsar"  1285    216 11  16  0.005   47550   220.14
"12th Fail" "2023-10-28"    "2023-10-28"    "Bengaluru" 9445    3680    56  38  0.085   848460  230.56
*/
 
Additional Information about the table and columns:
This table contains movie's occupancy data with respect to every city.
It shows how much are the shows for that movie occupied and total seats on theaters for the particular movie
Each movie has multiple show dates and cities. use group by clause if all of them are not mentioned in the query
Following are the columns and their description:
 
movie_name VARCHAR(64),
show_date: Date of the show. This unique for each movie,
city_name: name of an indian city,
total_seats: total seats given to the movie in that city,
occupied_seats: total number of occupied seats of the movie,
shows: total shows of the movie,
total_rev_in_cr: total advance tickets sold (in crore) of movie,
category_rev: revenue of that category of the movie,
avgprice: average ticket price
release_date: the date on which movie is released
 
use few examples to understand how the database works :
["input": "What are the total seats of dunki in pune on release date",
"SQLQuery": "SELECT sum(total_seats) from {movies_table} where show_date=release_date and movie_name='Dunki' and city_name='Pune'"
],
["input":"What was day 1 collection of Animal?",
"SQLQuery": "SELECT sum(total_rev_in_cr) from {movies_table} where show_date=release_date and movie_name='Animal'",
],
["input":"What was average ticket price of Dunki?",
"SQLQuery": "SELECT (sum(occupied_seats*avgprice)/sum(occupied_seats)) FROM {movies_table} WHERE movie_name='Dunki'",
],
["input":"What is Animal advance day 2 citywise?",
"SQLQuery": "SELECT city_name, sum(total_rev_in_cr) FROM {movies_table} WHERE show_date = release_date+ interval '1 day' and movie_name = 'Animal' GROUP BY city_name",
],
["input":"What is Animal advance day 2?",
"SQLQuery": "SELECT sum(total_rev_in_cr) FROM {movies_table} WHERE show_date = release_date+ interval '1 day' and movie_name = 'Animal'",
],
["input":"What is the total advance Booking for the weekend for Animal?",
"SQLQuery": "SELECT sum(total_rev_in_cr) FROM {movies_table} WHERE show_date >= release_date AND show_date <= release_date+ interval '2 days'AND movie_name = 'Animal'",
],
["input":"What is the total occupied seats for the weekend for Animal?",
"SQLQuery": "SELECT sum(occupied_seats) FROM {movies_table} WHERE show_date >= release_date AND show_date <= release_date + interval '2 days' AND movie_name = 'Animal'",
],
["input":"what is the occupied seats for on release day for Animal?",
"SQLQuery": "SELECT SUM(occupied_seats) FROM {movies_table} WHERE show_date = release_date AND movie_name = 'Animal'",
],
["input":"What is the highest advance booking movie?",
"SQLQuery": "SELECT movie_name, MAX(total_rev_in_cr) as total_advance_booking FROM {movies_table} GROUP BY movie_name,show_date ORDER BY total_advance_booking DESC limit 1",
],
["input":"What is the average occupancy percentage of Animal on Day 1?",
"SQLQuery": "SELECT sum(occupied_seats)*100/sum(total_seats) as occupied_perc from {movies_table} where show_date=release_date and movie_name='Animal'",
],
["input":"What is avg occupancy percentage by movies over last weekend?",
"SQLQuery":"SELECT DISTINCT MOVIE_NAME,SUM(OCCUPIED_SEATS)/SUM(TOTAL_SEATS) AS AVG_OCCUPANCY FROM {movies_table} WHERE SHOW_DATE >=(SELECT DISTINCT MAX(SHOW_DATE) FROM {movies_table} WHERE EXTRACT(ISODOW FROM SHOW_DATE) = 5) AND SHOW_DATE <=(SELECT DISTINCT MAX(SHOW_DATE) FROM {movies_table} WHERE EXTRACT(ISODOW FROM SHOW_DATE) = 7) GROUP BY MOVIE_NAME"
],
["input":"which movie made the most revenue yesterday?",
"SQLQuery":"SELECT movie_name, sum(total_rev_in_cr) as total_revenue FROM movie_occupancy_all_data WHERE show_date = current_date - interval '1 day' GROUP BY movie_name ORDER BY total_revenue DESC LIMIT 1;"
],
["input":"which movies made the most advance sale revenue on their release day?",
"SQLQuery":"SELECT distinct movie_name,SUM(total_rev_in_cr) OVER (PARTITION BY movie_name, show_date) AS total_revenue FROM {movies_table} WHERE show_date=release_date ORDER BY total_revenue desc;"
]
["input":"what is the highest occupancy percentage movie?",
"SQLQuery":"SELECT movie_name, MAX(occupancy_percentage)*100 AS highest_occupancy_percentage FROM (SELECT movie_name, SUM(occupied_seats) * 100 / SUM(total_seats) AS occupancy_percentage FROM {movies_table} GROUP BY movie_name) as a GROUP BY movie_name ORDER BY highest_occupancy_percentage DESC LIMIT 1"
]
 
 
 
Relevant pieces of previous conversation:
{str('{history}')}
(You do not use aggregation functions like SUM(), AVG() within the MAX() or MIN() function, instead retrieve the required data first and then do the MAX() or MIN() over that)
(You need to calculate occupancy percentage, use sum(occupied_seats)/sum(total_seats) as occupancy_perc)
(You do not need to use these pieces of information if not relevant)
(You return the sql statement that is starting with 'SELECT')
(You do not return sql statement '[SQL: AI:')
 
 
Question: {str('{input}')}
 
"""
 
#### Changes
DATABASE_URI=requirement_details['db_details']['db_uri']
memory = ConversationBufferMemory(input_key='input', memory_key="history")
llm = ChatOpenAI(openai_api_key=open_ai_api_key,temperature=0)
PROMPT_X=PromptTemplate.from_template(MOVIE_INFO_DATA_PROMPT)
db2 = SQLDatabase.from_uri(DATABASE_URI, include_tables=[movies_table],)
dbchain_movies = SQLDatabaseChain(
        llm_chain=LLMChain(llm=llm, prompt=PROMPT_X, memory=memory),
        database=db2,
        verbose=True,
        # return_direct = True,
        return_sql = True
        )
 
def generate_query(result):
    user_query = result['result']
    user_query = user_query.replace('/n',' ')
    # print('QUERY --- ', user_query)
    # Implement logic to generate SQL query based on user input
    # For simplicity, assume the user input is a valid SQL query
    return user_query
####
 
 
def create_agent():
    # try:
   
    llm = OpenAI(temperature=0.0, openai_api_key=open_ai_api_key)
    DATABASE_URI=requirement_details['db_details']['db_uri']
 
    db1 = SQLDatabase.from_uri(DATABASE_URI, include_tables=[movies_table])
 
    context = db1.get_context()
    orders_table_info=context["table_info"]
    print(orders_table_info)
    memory = ConversationBufferMemory(input_key='input', memory_key="history")
    PROMPT_X=PromptTemplate.from_template(MOVIE_INFO_DATA_PROMPT)
    dbchain = SQLDatabaseChain(
        llm_chain=LLMChain(llm=llm, prompt=PROMPT_X, memory=memory),
        database=db1,
        verbose=False,
        #agent_scratchpad= []
        )
    tools = [PythonREPLTool()]
    description = (
        "Useful for when you need to answer questions about movies. "
        "You must not input SQL. Use this more than the Python tool if the question "
        "is about movies data, like 'how many top performing cities are there?' or 'total occupied seats in top 5 cities'"
    )
 
    movie_data = Tool(
    name="Data",  # We'll just call it 'Data'
    func=dbchain.run,
    description=description,    
    )  
 
    tools.append(movie_data)
    # Standard prefix
    prefix = "Fulfill the following request as best you can. You have access to the following tools:"
 
    # Remind the agent of the Data tool, and what types of input it expects
    suffix = (
        "Begin! When looking for data, do not write a SQL query. "
        "Pass the relevant portion of the request directly to the Data tool in its entirety."
        "\n\n"
        "Request: {input}\n"
        "{agent_scratchpad}"
    )
 
    # The agent's prompt is built with the list of tools, prefix, suffix, and input variables
    prompt = ZeroShotAgent.create_prompt(
    tools, prefix=prefix, suffix=suffix, input_variables=["input", "agent_scratchpad"]
    )
    # Set up the llm_chain
    llm_chain = LLMChain(llm=llm, prompt=prompt)
    # Specify the tools the agent may use
    tool_names = [tool.name for tool in tools]
    agent = ZeroShotAgent(llm_chain=llm_chain, allowed_tools=tool_names,verbose=True,return_intermediate_steps=True)
    # Create the AgentExecutor
    agent_executor = AgentExecutor.from_agent_and_tools(
    agent=agent, tools=tools, verbose=True      
    )
   
   
   
    return agent_executor