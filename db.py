from constant import DBNAME, HOST, USER, PASSWORD
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from constant import DBNAME, HOST, USER, PASSWORD

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

cached_connection = None


def enqurious_db_connection() -> psycopg2.connect:
    """
    Establishes and returns a connection to the Enqurious database.

    If a cached connection is available, it reuses it. Otherwise, it creates a new connection
    and caches it for future use.

    Returns:
        psycopg2.connection: Connection object to the Enqurious database.
    Raises:
        psycopg2.errors.Error: If there is an error establishing the connection.
    """
    global cached_connection
    if cached_connection:
        # If connection is already cached, return it
        logging.info("Enqurious Connection is fetched!!")
        return cached_connection

    try:
        logging.info("New Enqurious DB Connection is established!!")
        cached_connection = psycopg2.connect(
            database=DBNAME,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=5432,
            cursor_factory=RealDictCursor,
        )

        return cached_connection
    except psycopg2.errors.Error as error:
        logging.error("Error establishing connection:", error)
        raise error


def get_contents_data() -> list:
    """
    Fetches content data for all contents in the database.

    Returns:
        list: A list of tuples, each containing content details such as:
            - id: The unique identifier for the content.
            - name: The name of the content.
            - type: The type/category of the content.
            - level: The difficulty level of the content.
            - created_by: The creator of the content.
            - created_at: The timestamp when the content was created.
            - updated_at: The timestamp when the content was last updated.
            - state: The current state of the content.
            - poster_flag: 'Yes' if a poster file is attached, otherwise 'No'.
            - is_lab_attached: 'Yes' if a cloud lab is attached, otherwise 'No'.

    Raises:
        Exception: If there is an error during the query execution or database connection issues.
    """
    # fetch all deployments based on a particular program_id
    try:
        logging.info(f"{get_contents_data.__name__} called!")
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT c.id, 
            c.name, 
            c.type, 
            c.level, 
            c.created_by,
            c.created_at,
            c.updated_at,
            c.state,
            c.is_standalone,
            c.poster_file_id,
            "level",
            c.cloud_lab_id
        FROM contents c
        WHERE is_standalone=True;"""
        cursor.execute(query)

        result = cursor.fetchall()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error


def get_creator_info_by_creator_id(creator_id: str):
    # fetch all deployments based on a particular program_id
    try:
        logging.info(f"{get_creator_info_by_creator_id.__name__} called!")
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
            u.id,
            u.email,
            p.first_name || ' ' || p.last_name as full_name,
            p.linkedin 
            FROM users u
            JOIN profiles p
            ON p.user_id=u.id
            WHERE u.id=%s;"""
        cursor.execute(query, (creator_id,))

        result = cursor.fetchone()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error


def get_content_tags_by_content_id(content_id: str):
    try:
        logging.info(f"{get_content_tags_by_content_id.__name__} called!")
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
            content_id,
            t."name" as tag_name,
            tc."name" as tag_category_name
            FROM content_tags ct
            JOIN tags t
            ON ct.tag_id=t.id
            JOIN tag_categories tc
            ON tc.id=t.tag_category_id 
            WHERE ct.content_id=%s;"""
        cursor.execute(query, (content_id,))

        result = cursor.fetchall()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error


def get_content_domain_by_content_id(content_id: str):
    try:
        logging.info(f"{get_content_domain_by_content_id.__name__} called!")
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
            ci.content_id,
            i.id as industry_id,
            i."name" as domain
            FROM
            content_industries ci
            JOIN industries i
            ON ci.industry_id = i.id
            WHERE content_id=%s;"""
        cursor.execute(query, (content_id,))

        result = cursor.fetchall()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error


def get_content_standalone_deployments_by_content_id(content_id: str):
    try:
        logging.info(
            f"{get_content_standalone_deployments_by_content_id.__name__} called!"
        )
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
            COUNT(d.id) as total_deployments,
            MAX(d.created_at) as last_consumed
            FROM deployments d
            WHERE d.item_id = %s
            AND is_active=true;"""
        cursor.execute(query, (content_id,))

        result = cursor.fetchone()
        return (
            (result["total_deployments"], result["last_consumed"])
            if result
            else (0, None)
        )
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error


def get_content_deployments_via_skill_path_by_content_id(content_id: str):
    try:
        logging.info(
            f"{get_content_deployments_via_skill_path_by_content_id.__name__} called!"
        )
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
            COUNT(d.id) as total_deployments,
            MAX(d.created_at) as last_consumed
            FROM deployments d
            JOIN
            skill_path_modules spm
            ON d.item_id=spm.skill_path_id
            WHERE spm.module_id = %s
            AND is_active=True;"""
        cursor.execute(query, (content_id,))

        result = cursor.fetchone()
        return (
            (result["total_deployments"], result["last_consumed"])
            if result
            else (0, None)
        )
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error


# def get_content_deployments_of_scenario_bundles_by_content_id():
#     try:
#         logging.info(
#             f"{get_content_deployments_of_scenario_bundles_by_content_id.__name__} called!"
#         )
#         connection = enqurious_db_connection()
#         cursor = connection.cursor()
#         query = """SELECT 
#             COUNT(d.id) as total_deployments,
#             MAX(d.created_at) as last_consumed
#             FROM
#             deployments d
#             JOIN
#             skill_path_modules spm
#             ON d.item_id=spm.skill_path_id
#             JOIN
#             scenario_bundle_scenarios sbs
#             ON spm.module_id = sbs.scenario_bundle_id
#             JOIN
#             contents c
#             ON c.id=sbs.scenario_id
#             WHERE c.id=%s
#             AND is_active=True;"""
#         cursor.execute(query, (content_id,))

#         result = cursor.fetchone()
#         return (
#             (result["total_deployments"], result["last_consumed"])
#             if result
#             else (0, None)
#         )
#     except Exception as error:
#         close_enqurious_db_connection()  # Ensure the connection is closed on error
#         raise error


def get_scenarios_by_content_id(content_id: str):
    try:
        logging.info(
            f"{get_scenarios_by_content_id.__name__} called!"
        )
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
            cs.scenario_id
            FROM content_scenarios cs
            WHERE cs.content_id=%s;"""
        cursor.execute(query, (content_id,))

        result = cursor.fetchall()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error
    

def get_scenario_solution_by_scenario_id(scenario_id: str):
    try:
        logging.info(
            f"{get_scenario_solution_by_scenario_id.__name__} called!"
        )
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
            ss.scenario_id
            FROM scenario_solutions ss
            WHERE ss.scenario_id=%s;"""
        cursor.execute(query, (scenario_id,))

        result = cursor.fetchone()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error
    

def get_all_inputs_by_scenario_id(scenario_id: str):
    try:
        logging.info(
            f"{get_all_inputs_by_scenario_id.__name__} called!"
        )
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """select 
            id,
            input_type
            from 
            scenario_inputs si 
            where si.scenario_id=%s;"""
        cursor.execute(query, (scenario_id,))

        result = cursor.fetchall()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error
    

def get_input_hints_by_scenario_id(scenario_id: str):
    try:
        logging.info(
            f"{get_input_hints_by_scenario_id.__name__} called!"
        )
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
                ih.id
                FROM scenario_inputs si
                JOIN
                input_hints ih
                ON si.id=ih.scenario_input_id
                WHERE si.scenario_id=%s;"""
        cursor.execute(query, (scenario_id,))

        result = cursor.fetchall()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error


def get_input_solutions_by_scenario_id(scenario_id: str):
    try:
        logging.info(
            f"{get_input_solutions_by_scenario_id.__name__} called!"
        )
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT
            is2.id
            FROM scenario_inputs si
            JOIN
            input_solutions is2
            ON si.id=is2.scenario_input_id
            WHERE si.scenario_id=%s;"""
        cursor.execute(query, (scenario_id,))

        result = cursor.fetchall()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error
    

def get_tagged_inputs_by_scenario_id(scenario_id: str):
    try:
        logging.info(
            f"{get_input_solutions_by_scenario_id.__name__} called!"
        )
        connection = enqurious_db_connection()
        cursor = connection.cursor()
        query = """SELECT 
            DISTINCT si.id
            FROM
            scenario_inputs si
            JOIN
            input_tags it
            ON si.id=it.scenario_input_id
            WHERE si.scenario_id=%s;"""
        cursor.execute(query, (scenario_id,))

        result = cursor.fetchall()
        return result
    except Exception as error:
        close_enqurious_db_connection()  # Ensure the connection is closed on error
        raise error    


def close_enqurious_db_connection() -> None:
    """
    Closes the database connection if it is currently open.

    This function ensures that the global cached database connection is closed
    and set to None to prevent further use.

    Returns:
        None
    """
    global cached_connection
    if cached_connection:
        cached_connection.close()
        cached_connection = None


if __name__ == "__main__":
    # contents_data = get_contents_data()
    # print(contents_data)

    # creator_id = "b29e06df-5cf2-4a27-bf44-62edc39eb396"
    # creator_info = get_creator_info_by_creator_id(creator_id)
    # print(creator_info)

    # content_id = "ec04f791-ce49-4b45-a768-ebc2523a62a0"
    # content_tags = get_content_tags_by_content_id(content_id)
    # skills, tools = [], []
    # for tag_record in content_tags:
    #     if tag_record["tag_category_name"] == "skill":
    #         skills.append(tag_record["tag_name"])
    #     elif tag_record["tag_category_name"] == "tool":
    #         tools.append(tag_record["tag_name"])

    # print(skills)
    # print(tools)

    # content_id = "24ba865e-14a0-4615-b7fb-b3f5bb819fb1"
    # domain_info = get_content_domain_by_content_id(content_id)
    # domains = [single_domain_info["domain"] for single_domain_info in domain_info]
    # print(domains)

    # Fetch standalone deployments and deployments via skill paths
    # content_id = "97e2c56a-db70-462d-91ff-7a6314797221"
    # standalone_deployments, standalone_last_consumed_date = get_content_standalone_deployments_by_content_id(content_id)
    # skill_path_deployments, skill_path_last_consumed_date = get_content_deployments_via_skill_path_by_content_id(content_id)

    # # Get the most recent consumed date
    # content_last_consumed_date = max(filter(None, [standalone_last_consumed_date, skill_path_last_consumed_date]))
    # print(content_last_consumed_date)

    # # Total deployments (standalone + via skill paths)
    # total_deployments = standalone_deployments + skill_path_deployments
    # print(total_deployments)

    # content_type = "SCENARIO"
    # is_standalone = True
    # if content_type == "SCENARIO" and is_standalone:
    #     (
    #         scenario_bundle_deployments,
    #         scenario_bundle_last_consumed_date,
    #     ) = get_content_deployments_of_scenario_bundles_by_content_id(content_id)

    #     if scenario_bundle_last_consumed_date:
    #         content_last_consumed_date = max(filter(None, [content_last_consumed_date, scenario_bundle_last_consumed_date]))
    #         total_deployments += scenario_bundle_deployments

    # print(content_last_consumed_date)
    # print(total_deployments)

    # content_id = "24ba865e-14a0-4615-b7fb-b3f5bb819fb1"
    # scenarios_data = get_scenarios_by_content_id(content_id)

    # scenario_id = "b20f4ca2-9595-4cc3-8fea-1f49418a8a98"
    # scenario_solution = get_scenario_solution_by_scenario_id(scenario_id)
    # print(scenario_solution)

    # inputs_dict = {
    #     "file_upload": 0,
    #     "text": 0,
    #     "choice": 0,
    #     "short_answer": 0,
    #     "video": 0,
    #     "code": 0
    # }
    # total_inputs = get_all_inputs_by_scenario_id(scenario_id)
    # for input in total_inputs:
    #     for input_type in inputs_dict.keys():
    #         if input["input_type"].lower() == input_type:
    #             inputs_dict[input_type] += 1
    #             break

    # print(inputs_dict)

    # scenario_id = "b20f4ca2-9595-4cc3-8fea-1f49418a8a98"
    # input_hints = len(get_input_hints_by_scenario_id(scenario_id))
    # input_solutions = len(get_input_solutions_by_scenario_id(scenario_id))
    # tagged_inputs = len(get_tagged_inputs_by_scenario_id(scenario_id))

    # print(input_hints)
    # print(input_solutions)
    # print(tagged_inputs)

    scenario_id = "bc64c3db-55b0-448e-b099-6d160b69ebe7"
    # scenario_id = "b85900dd-3495-4f32-bf07-3c1d38fe730f"
    scenario_type = "SCENARIO"
    if scenario_type == "SCENARIO":
        scenarios_data = [{"scenario_id": scenario_id}]
    else:
        scenarios_data = get_scenarios_by_content_id(scenario_id)

    print(scenarios_data)




    