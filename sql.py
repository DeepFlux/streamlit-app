from langchain.agents.agent_toolkits import SQLDatabaseToolkit

from langchain.agents.agent_types import AgentType
from langchain.agents import create_sql_agent
from langchain.memory import ConversationBufferMemory
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import (
    ChatPromptTemplate,
    FewShotPromptTemplate,
    MessagesPlaceholder,
    PromptTemplate,
    SystemMessagePromptTemplate,
)

from utils import get_chat_openai
#from tools.functions_tools import sql_agent_tools
##from database.sql_db_langchain import db
##from .agent_constants import CUSTOM_SUFFIX
import json
with open('config.json', 'r') as f:
    # Load the JSON data
    requirement_details = json.load(f)

movies_table = requirement_details['table_name']
MOVIE_INFO_DATA_PROMPT=f"""
You are a PostgreSQL expert. Given an input question, form a correct PostgreSQL query to be used to retreive data by also using relevant information from chat history
Never query for all columns from a table. You must query only the columns that are needed to answer the question. Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention include a 'GROUP BY' clause when utilizing aggregation functions like SUM(),MIN(),MAX(),AVG() in queries for accurate data grouping across multiple columns.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist.
Pay attention to use MIN(SHOW_DATE) as SHOW_DATE in where condition when question asks for "release date"
Pay attention to Whenever requesting information on release date or first day box office, incorporate MIN(SHOW_DATE) as the method for determining the release date, use that date as the show_date
Pay attention to Whenever requesting information on weekend, incorporate MIN(SHOW_DATE) + interval '2 days' as the method for determining the weekend date, use that date as the show_date


Only use the following tables:
CREATE TABLE {movies_table} (
movie_name VARCHAR(64),
show_date DATE,
crawl_date DATE,
city_name VARCHAR(64),
total_seats BIGINT,
occupied_seats BIGINT,
shows BIGINT,
occupancy_perc BIGINT,
total_rev_in_cr NUMERIC,
category_rev NUMERIC,
avgprice NUMERIC
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
show_date: Date of the show,
city_name: name of an indian city,
total_seats: total seats given to the movie in that city,
occupied_seats: total number of occupied seats of the movie,
shows: total shows of the movie,
occupancy_perc: percentage of seats occupied,
total_rev_in_cr: total advance tickets sold (in crore) of movie,
category_rev: revenue of that category of the movie,
avgprice: average ticket price

use few examples to understand how the database works :
["input": "What are the total seats of dunki in pune on release date",
"SQLQuery": "SELECT sum(total_seats) from {movies_table} where show_date=(select min(show_date) from {movies_table} where movie_name='Dunki' and city_name='Pune') and movie_name='Dunki' and city_name='Pune'"
],
["input":"What was day 1 collection of Animal?",
"SQLQuery": "SELECT sum(total_rev_in_cr) from {movies_table} where show_date=(select min(show_date) from {movies_table} where movie_name='Animal') and movie_name='Animal'",
],
["input":"What was average ticket price of Dunki?",
"SQLQuery": "SELECT avg(avgprice) FROM {movies_table} WHERE movie_name='Dunki'",
],
["input":"What is Animal advance day 2 citywise?",
"SQLQuery": "SELECT city_name, sum(total_rev_in_cr) FROM {movies_table} WHERE show_date = (SELECT MIN(show_date) FROM {movies_table} WHERE movie_name='Animal')+ interval '1 day' and movie_name = 'Animal' GROUP BY city_name",
],
["input":"What is Animal advance day 2?",
"SQLQuery": "SELECT city_name, sum(total_rev_in_cr) FROM {movies_table} WHERE show_date = (SELECT MIN(show_date) FROM {movies_table} WHERE movie_name='Animal')+ interval '1 day' and movie_name = 'Animal'",
],
["input":"What is Animal occupancy percentage day 1?",
"SQLQuery": "SELECT city_name, sum(occupancy_perc) FROM {movies_table} WHERE show_date = (SELECT MIN(show_date) FROM {movies_table} WHERE movie_name='Animal')+ interval '1 day' and movie_name = 'Animal'",
],
["input":"What is the total advance Booking for the weekend for Animal?",
"SQLQuery": "SELECT sum(total_rev_in_cr) FROM {movies_table} WHERE show_date >= (SELECT MIN(show_date) FROM {movies_table} WHERE movie_name = 'Animal') AND show_date <= (SELECT MIN(show_date) FROM {movies_table} WHERE movie_name = 'Animal') + interval '2 days'AND movie_name = 'Animal'",
],
["input":"What is the total occupied seats for the weekend for Animal?",
"SQLQuery": "SELECT sum(occupied_seats) FROM {movies_table} WHERE show_date >= (SELECT MIN(show_date) FROM {movies_table} WHERE movie_name = 'Animal') AND show_date <= (SELECT MIN(show_date) FROM {movies_table} WHERE movie_name = 'Animal') + interval '2 days' AND movie_name = 'Animal'",
],
["input":"what is the average occupied seats for day 1 for Animal?",
"SQLQuery": "SELECT AVG(occupied_seats) FROM {movies_table} WHERE show_date = (SELECT MIN(show_date) FROM {movies_table} WHERE movie_name = 'Animal') AND movie_name = 'Animal'",
],
["input":"What is the highest advance booking movie?",
"SQLQuery": "SELECT movie_name, MAX(total_rev_in_cr) as total_advance_booking FROM {movies_table} GROUP BY movie_name,show_date ORDER BY total_advance_booking DESC limit 1",
],




Relevant pieces of previous conversation:
{str('{history}')}
(You do not need to use these pieces of information if not relevant)
(You return the sql statement that is starting with 'SELECT')
(You do not return sql statement '[SQL: AI:')


Question: {str('{input}')}

"""


def get_sql_toolkit(tool_llm_name: str):
    """
    Get the SQL toolkit for a given tool LLM name.

    Parameters:
        tool_llm_name (str): The name of the tool LLM.

    Returns:
        SQLDatabaseToolkit: The SQL toolkit object.
    """
    llm_tool = get_chat_openai(model_name=tool_llm_name)
    DATABASE_URI=requirement_details['db_details']['db_uri']
    db = SQLDatabase.from_uri(DATABASE_URI, include_tables=movies_table)
    toolkit = SQLDatabaseToolkit(db=db, llm=llm_tool)
    return toolkit


def get_agent_llm(agent_llm_name: str):
    """
    Retrieves the LLM agent with the specified name.

    Parameters:
        agent_llm_name (str): The name of the LLN agent.

    Returns:
        llm_agent: The LLM agent object.
    """
    llm_agent = get_chat_openai(model_name=agent_llm_name)
    return llm_agent


def create_agent(
    tool_llm_name: str = "gpt-4-1106-preview",
    agent_llm_name: str = "gpt-4-1106-preview",
):
    """
    Creates a SQL agent using the specified tool and agent LLM names.

    Args:
        tool_llm_name (str, optional): The name of the SQL toolkit LLM. Defaults to "gpt-4-1106-preview".
        agent_llm_name (str, optional): The name of the agent LLM. Defaults to "gpt-4-1106-preview".

    Returns:
        agent: The created SQL agent.
    """

    #agent_tools = sql_agent_tools()
    llm_agent = get_agent_llm(agent_llm_name)
    toolkit = get_sql_toolkit(tool_llm_name)
    memory = ConversationBufferMemory(memory_key="history", input_key="input")
    custom_suffix =  (
        "Begin! When looking for data, do not write a SQL query. "
        "Pass the relevant portion of the request directly to the Data tool in its entirety."
        "\n\n"
        "Request: {input}\n"
        "{agent_scratchpad}"
    )
    agent = create_sql_agent(
        llm=llm_agent,
        toolkit=toolkit,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        input_variables=["input", "agent_scratchpad", "history"],
        prompt=MOVIE_INFO_DATA_PROMPT,
        suffix=custom_suffix,
        agent_executor_kwargs={"memory": memory},
        verbose=True,
    )
    return agent
