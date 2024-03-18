from langchain.chat_models import ChatOpenAI
import os
from sqlalchemy import create_engine, text
from constants import chat_openai_model_kwargs, langchain_chat_kwargs
from sentry_sdk import capture_exception
import pandas as pd


# Optional, set the API key for OpenAI if it's not set in the environment.
os.environ["OPENAI_API_KEY"] = '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'

#### CHANGES
host = 'df-godfather.cqpsdafhgvgc.ap-southeast-1.rds.amazonaws.com' 
database = 'movies_launch' 
user = 'jupyterhub'   
password = 'dligv37940ptou'  
port = '3282'
db_uri_formatter = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
db_uri = db_uri_formatter.format(
            user=user, password=password, host=host, port=port,
            database=database)
# print('DB URI - ', db_uri)
_db_engine = create_engine(db_uri, echo=False)

def raw_query(query, as_dict=False, as_df=False):
    """This method will be used for getting the result of
    any sql query and return the output as required

    Arguments:
        query {str} -- SQL query which need to be executed

    Keyword Arguments:
        as_dict {bool} -- Return result as list of dict (default: {True})
        as_df {bool} -- Return result as pandas df (default: {False})
    """
    # TODO: Whitelist the query for potential threats
    print('!!!!!!!!!!!!! RUNNING RAW QUERY FUNCTION !!!!!!!!!!!!!!!!')
    final_output = None

    if (as_df == as_dict):
        raise Exception('Select output result as dict or dataframe')

    query = text(query)
    if as_df:
        try:
            output_df = pd.read_sql(query, con=_db_engine)
            output_df = output_df.fillna('')
            final_output = output_df
        except Exception as err_msg:
            print('---- Error in DB Query ----')
            print(err_msg)
            capture_exception(err_msg)

    elif as_dict:
        with _db_engine.connect() as connection:
            trans = connection.begin()
            try:
                results = connection.execute(query)
                if results.returns_rows:
                    final_output = [row.items() for row in results]
                else:
                    final_output = True
                trans.commit()
            except Exception as err_msg:
                trans.rollback()
                print('---- Error in DB Query ----')
                print(traceback.format_exc())
                print(err_msg)
                capture_exception(err_msg)
    else:
        raise Exception('Select the output format')
    print('FINAL OUTPUT --- ', final_output.shape)
    return final_output

####

def get_chat_openai(model_name):
    """
    Returns an instance of the ChatOpenAI class initialized with the specified model name.

    Args:
        model_name (str): The name of the model to use.

    Returns:
        ChatOpenAI: An instance of the ChatOpenAI class.

    """
    llm = ChatOpenAI(
        model_name=model_name,
        model_kwargs=chat_openai_model_kwargs,
        **langchain_chat_kwargs
    )
    return llm
