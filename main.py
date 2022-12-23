import psycopg2
import pandas as pd
from sqlalchemy import create_engine


def get_raw_data(curr, query):
    curr.execute(query)
    tuples = cursor.fetchall()
    column_names = [col.name for col in cursor.description]

    return pd.DataFrame(tuples, columns=column_names)


def explode_json(df, func, columns):
    df["query_details"] = df.apply(func, axis=1)

    exploded = df.explode("query_details")
    df = pd.concat([exploded[columns].reset_index(drop=True),
                    pd.json_normalize(exploded["query_details"])], axis=1)

    return df


def get_execute_result(curr):
    query = """select s."id" as db_id, s."executionId", s."userId", s."orgId", s."createdAt", s."sessionId", s."value" 
    from "rogo-auth"."SearchEvent" as s where "type" = 'ExecuteResult'; """
    df_init = get_raw_data(curr, query)

    def return_json(x):
        return x['value']["query"]

    return explode_json(df_init, return_json, ["db_id", "executionId", "userId", "orgId", "createdAt", "sessionId"])


def get_user_types(curr):
    query = """select s."id" as db_id, s."executionId", s."userId", s."orgId", s."createdAt", s."sessionId", s."value" 
    from "rogo-auth"."SearchEvent" as s where "type" = 'UserTypes'; """
    df_init = get_raw_data(curr, query)

    df_init["currentSearchValue"] = df_init.apply(lambda x: x['value']['currentSearchValue'], axis=1)

    def return_json(x):
        # foo = x['value']["currentSuggestions"][0]['terms']
        try:
            tmp = x['value']["currentSuggestions"][0]['terms']
            return tmp
        except IndexError:
            return None
        except KeyError:
            return None

    return explode_json(df_init, return_json, ["db_id", "executionId", "userId", "orgId", "createdAt",
                                               "currentSearchValue", "sessionId"])


if __name__ == "__main__":
    conn = psycopg2.connect(
       database="auth",
       user='postgres',
       password='Jtu6Q6KeA37oQnJvNVXN',
       host='bjb-prototyping.ch8k24g0z0fb.us-east-1.rds.amazonaws.com',
       port='5432'
    )
    cursor = conn.cursor()

    engine = create_engine('postgresql://postgres:Jtu6Q6KeA37oQnJvNVXN@bjb-prototyping.ch8k24g0z0fb.us-east-1.rds.amazonaws.com:5432/auth')
    user_types_df: pd.DataFrame = get_user_types(cursor)
    # print(user_types_df.to_string())
    user_types_df.to_sql("UserTypes", engine, schema="public")

    exec_results_df: pd.DataFrame = get_execute_result(cursor)
    exec_results_df.to_sql("ExecuteResult", engine, schema="public")