import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from typing import List


def get_raw_data(curr, query: str) -> pd.DataFrame:
    curr.execute(query)
    tuples = cursor.fetchall()
    column_names = [col.name for col in cursor.description]

    return pd.DataFrame(tuples, columns=column_names)


def explode_json(df: pd.DataFrame, func, columns: List[str]) -> pd.DataFrame:
    df["query_details"] = df.apply(func, axis=1)

    exploded = df.explode("query_details")
    df = pd.concat([exploded[columns].reset_index(drop=True), pd.json_normalize(exploded["query_details"])], axis=1)

    df["Rank"] = df.groupby("executionId")["createdAt"].rank(method="first", ascending=True)

    messy_columns = df.columns
    clean_columns = ["".join(e for e in col if e.isalnum()) for col in messy_columns]
    df.columns = clean_columns
    return df


def get_possible_selections(curr) -> pd.DataFrame:
    query = """ select s."id" as db_id, s."executionId", s."userId", s."orgId", s."createdAt", s."sessionId", s."value" 
        from "rogo-auth"."SearchEvent" as s where "type" = 'ChooseSuggestion'; """

    df_init = get_raw_data(curr, query)

    def return_json(x):
        tmp = x["value"]["suggestions"]
        rez = []
        for elem in tmp:
            if "terms" not in elem:
                rez.append(elem)

        return rez

    return explode_json(df_init, return_json, ["db_id", "executionId", "userId", "orgId", "createdAt", "sessionId"])


def get_chosen_selection(curr) -> pd.DataFrame:
    query = """ select s."id" as db_id, s."executionId", s."userId", s."orgId", s."createdAt", s."sessionId", s."value" 
    from "rogo-auth"."SearchEvent" as s where "type" = 'ChooseSuggestion'; """

    df_init = get_raw_data(curr, query)

    def return_json(x):
        return x["value"]["suggestion"]

    return explode_json(df_init, return_json, ["db_id", "executionId", "userId", "orgId", "createdAt", "sessionId"])


def get_deletes(curr) -> pd.DataFrame:
    query = """select s."id" as db_id, s."executionId", s."userId", s."orgId", s."createdAt", s."sessionId", s."value" 
    from "rogo-auth"."SearchEvent" as s where "type" = 'DeleteVisualQueryItem'; """

    df_init = get_raw_data(curr, query)

    def return_json(x):
        return [x["value"]["deletedTerm"]]

    flattened_df = explode_json(
        df_init, return_json, ["db_id", "executionId", "userId", "orgId", "createdAt", "sessionId"]
    )

    flattened_df["id"] = flattened_df["key"].apply(lambda x: x.split("-")[0])

    return flattened_df


def get_execute_result(curr) -> pd.DataFrame:
    query = """select s."id" as db_id, s."executionId", s."userId", s."orgId", s."createdAt", s."sessionId", s."value" 
    from "rogo-auth"."SearchEvent" as s where "type" = 'ExecuteResult'; """
    df_init = get_raw_data(curr, query)

    def return_json(x):
        return x["value"]["query"]

    return explode_json(df_init, return_json, ["db_id", "executionId", "userId", "orgId", "createdAt", "sessionId"])


def get_user_types(curr):
    query = """select s."id" as db_id, s."executionId", s."userId", s."orgId", s."createdAt", s."sessionId", s."value" 
    from "rogo-auth"."SearchEvent" as s where "type" = 'UserTypes'; """
    df_init = get_raw_data(curr, query)

    df_init["currentSearchValue"] = df_init.apply(lambda x: x["value"]["currentSearchValue"], axis=1)

    def return_json(x):
        # foo = x['value']["currentSuggestions"][0]['terms']
        try:
            tmp = x["value"]["currentSuggestions"][0]["terms"]
            return tmp
        except IndexError:
            return None
        except KeyError:
            return None

    return explode_json(
        df_init,
        return_json,
        ["db_id", "executionId", "userId", "orgId", "createdAt", "currentSearchValue", "sessionId"],
    )


def get_matches_misses_deletes(curr) -> pd.DataFrame:
    lazy_sql = """
    select matches.*, misses.misses, deletes.deletes
    from (
	select 
		cs."id",
		cs."userId",
		cs."orgId",
		cs.termdatasourceids,
		cs.canonicalname,
		cs.matchedtext,
		count(*) as matches
	from "auth"."public"."ChoosenSuggestion" cs
	inner join "auth"."public"."PossibleSuggestions" ps
	on cs.dbid  = ps.dbid 
	and cs.id = ps.id
	group by cs."id",
		cs."userId",
		cs."orgId",
		cs.termdatasourceids,
		cs.canonicalname,
		cs.matchedtext
    ) matches	
    left join (  -- should be a full outer join, but don't want to hack in the plumbing
	select 
		cs."id",
		count(*) as misses
	from "auth"."public"."ChoosenSuggestion" cs
	inner join "auth"."public"."PossibleSuggestions" ps
	on cs.dbid  = ps.dbid 
	and cs.id != ps.id
	group by cs."id",
		cs."userId",
		cs."orgId",
		cs.termdatasourceids,
		cs.canonicalname,
		cs.matchedtext
    ) misses
    on matches."id" = misses."id"
    left join (    -- should be a full outer join, but don't have all the data we need
        select 
            d."id",
            count(*) as deletes
        from "auth"."public"."deletes" d
        group by d."id"
    ) deletes
    on matches."id" = deletes."id" """

    return get_raw_data(curr, lazy_sql)


if __name__ == "__main__":
    database = "auth"
    user = "postgres"
    password = "Jtu6Q6KeA37oQnJvNVXN"
    host = "bjb-prototyping.ch8k24g0z0fb.us-east-1.rds.amazonaws.com"
    port = "5432"

    conn = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cursor = conn.cursor()

    for table in ["UserTypes", "ExecuteResult", "ChoosenSuggestion", "PossibleSuggestions", "Weighting", "deletes"]:
        cursor.execute(f"""drop table if exists "public"."{table}" """)

    conn.commit()

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    try:
        deletes: pd.DataFrame = get_deletes(cursor)
        # print(deletes.to_string())
        deletes.to_sql("deletes", engine, schema="public")

        possible_suggestions: pd.DataFrame = get_possible_selections(cursor)
        # print(possible_suggestions.to_string())
        possible_suggestions.to_sql("PossibleSuggestions", engine, schema="public")

        choose_selection: pd.DataFrame = get_chosen_selection(cursor)
        # print(choose_selection.to_string())
        choose_selection.to_sql("ChoosenSuggestion", engine, schema="public")

        user_types_df: pd.DataFrame = get_user_types(cursor)
        # print(user_types_df.to_string())
        user_types_df.to_sql("UserTypes", engine, schema="public")

        exec_results_df: pd.DataFrame = get_execute_result(cursor)
        # print(exec_results_df.to_string())
        exec_results_df.to_sql("ExecuteResult", engine, schema="public")

        m_m_d: pd.DataFrame = get_matches_misses_deletes(cursor)
        # print(m_m_d.to_string())
        m_m_d.to_sql("Weighting", engine, schema="public", index=False)
        conn.close()
        engine.dispose()
    except:
        conn.close()
        engine.dispose()
        raise
