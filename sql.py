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
movies_table = 'movie_occupancy_all_data'
MOVIE_INFO_DATA_PROMPT=f"""
You are a PostgreSQL expert. Given an input question, form a correct PostgreSQL query to be used to retreive data by also using relevant information from chat history
Never query for all columns from a table. You must query only the columns that are needed to answer the question. Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention include a 'GROUP BY' clause when utilizing aggregation functions like SUM(),MIN(),MAX(),AVG() in queries for accurate data grouping across multiple columns.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist.
Pay attention to use MIN(SHOW_DATE) as SHOW_DATE in where condition when question asks for "release date"
Pay attention to Whenever requesting information on release date or first day box office, incorporate MIN(SHOW_DATE) as the method for determining the release date, use that date as the show_date

Only use the following tables:
CREATE TABLE {movies_table} (
movie_name VARCHAR(64),
show_date DATE,
crawl_date DATE,
city_name VARCHAR(64),
total_seats BIGINT,
occupied_seats BIGINT,
reserved_quota BIGINT,
shows BIGINT,
occupancy_perc BIGINT,
total_rev_in_cr NUMERIC,
avgprice NUMERIC
)

/*
movie_name show_date city_name total_seats occupied_seats reserved_quota shows occupancy_perc total_rev_in_cr avgprice
All India Rank 2024-02-23 Ahmedabad 2888 548 90 3 18 0.005 99.07
All India Rank 2024-02-23 Bengaluru 16493 4747 674 14 28 0.058 122.09
All India Rank 2024-02-23 Chandigarh 1624 323 136 2 19 0.004 139.25
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
reserved_quota: total number of seats reserved ny the theatre,
shows: total shows of the movie,
occupancy_perc: percentage of seats occupied,
total_rev_in_cr: total advance tickets sold (in crore) of movie,
avgprice: average ticket price

use few examples to understand how the database works :
["input": "What are the total seats of dunki in pune on release date",
"SQLQuery": "SELECT sum(total_seats) from {movies_table} where show_date=(select min(show_date) from {movies_table} where movie_name='Dunki' and city_name='Pune') and movie_name='Dunki' and city_name='Pune'"
"Answer": "'Dunki' has total 73088 seats in pune on the date of release"],
["input":"What was day 1 collection of Animal?",
"SQLQuery": "SELECT sum(total_rev_in_cr) from {movies_table} where show_date=(select min(show_date) from {movies_table} where movie_name='Animal') and movie_name='Animal'",
"Answer": "'Animal' day 1 collection was 43 Cr"],
["input":"What was average ticket price of Dunki?",
"SQLQuery": "SELECT avg(avgprice) FROM {movies_table} WHERE movie_name='Dunki'",
"Answer": "'Dunki' average ticket price was 308 Rs "]


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
    DATABASE_URI="postgresql://jupyterhub:dligv37940ptou@df-godfather.cqpsdafhgvgc.ap-southeast-1.rds.amazonaws.com:3282/movies_launch"
    db = SQLDatabase.from_uri(DATABASE_URI, include_tables=['movie_occupancy_6_months_table_1'])
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
