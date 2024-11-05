import json
import pandas as pd
from gsheet import reflect_data_to_gsheet
import db


def main():

    total_contents = []

    # Fetch only standalone contents
    contents_data = db.get_contents_data()
    for content in contents_data:
        content_id = content.get("id")
        creator_info = db.get_creator_info_by_creator_id(content.get("created_by"))
        print(creator_info)

        content_tags = db.get_content_tags_by_content_id(content_id)
        skills, tools = [], []
        for tag_record in content_tags:
            if tag_record["tag_category_name"] == "skill":
                skills.append(tag_record["tag_name"])
            elif tag_record["tag_category_name"] == "tool":
                tools.append(tag_record["tag_name"])

        domain_info = db.get_content_domain_by_content_id(content_id)
        domains = [single_domain_info["domain"] for single_domain_info in domain_info]
        print(domains)

        # Fetch standalone deployments and deployments via skill paths
        standalone_deployments, standalone_last_consumed_date = (
            db.get_content_standalone_deployments_by_content_id(content_id)
        )
        skill_path_deployments, skill_path_last_consumed_date = (
            db.get_content_deployments_via_skill_path_by_content_id(content_id)
        )

        # Get the most recent consumed date
        content_last_consumed_date = max(
            filter(
                None, [standalone_last_consumed_date, skill_path_last_consumed_date]
            ),
            default=None,
        )

        print(content_last_consumed_date)

        # Total deployments (standalone + via skill paths)
        total_deployments = standalone_deployments + skill_path_deployments
        print(total_deployments)

        # Get all scenarios of contents
        if content["type"]=="SCENARIO":
            scenarios_data = [{"scenario_id": content["id"]}]
        else:
            scenarios_data = db.get_scenarios_by_content_id(content_id)
        # total_scenarios = len(scenarios_data)

        total_scenario_solution = 0

        # Fetch all the content_inputs
        inputs_dict = {
            "file_upload": 0,
            "text": 0,
            "choice": 0,
            "short_answer": 0,
            "video": 0,
            "code": 0,
        }
        input_hints = 0
        input_solutions = 0
        tagged_inputs = 0
        for scenario in scenarios_data:
            # get scenario solution
            scenario_id = scenario.get("scenario_id")
            scenario_solution = db.get_scenario_solution_by_scenario_id(scenario_id)
            total_scenario_solution += len(scenario_solution) if scenario_solution else 0

            total_inputs = db.get_all_inputs_by_scenario_id(scenario_id)
            for input in total_inputs:
                for input_type in inputs_dict.keys():
                    if input["input_type"].lower() == input_type:
                        inputs_dict[input_type] += 1
                        break

            # Get input hints
            input_hints += len(db.get_input_hints_by_scenario_id(scenario_id))

            # Get input solutions
            input_solutions += len(db.get_input_solutions_by_scenario_id(scenario_id))

            # Get tagged inputs
            tagged_inputs += len(db.get_tagged_inputs_by_scenario_id(scenario_id))

        total_contents.append(
            {
                "Content ID": content.get("id"),
                "Content Name": content.get("name"),
                "Content Type": content.get("type"),
                "Content Created At": content.get("created_at"),
                "Content Updated At": content.get("updated_at"),
                "Content State": content.get("state"),
                "Poster Flag": "Yes" if content.get("poster_flag_id") else "No",
                "Difficulty Level": content.get("level"),
                "Is_lab_attached": "Yes" if content.get("cloud_lab_id") else "No",
                "Content Creator Name": creator_info.get("full_name"),
                "Content Creator Email": creator_info.get("email"),
                "Skills": skills,
                "Tools": tools,
                "Domains": domains,
                "Total Deployments": total_deployments,
                "Last Consumed At": content_last_consumed_date,
                "Total Scenarios": len(scenarios_data),
                "Total Scenario Solutions": total_scenario_solution,
                "Choice": inputs_dict.get("choice"),
                "File Upload": inputs_dict.get("file_upload"),
                "Text": inputs_dict.get("text"),
                "Short Answer": inputs_dict.get("short_answer"),
                "Video": inputs_dict.get("video"),
                "Code": inputs_dict.get("code"),
                "Input Hints": input_hints,
                "Input Solution": input_solutions,
                "Tagged Inputs": tagged_inputs,
            }
        )

    total_contents_df = pd.DataFrame(total_contents)
    total_contents_df.to_csv("Enqurious_Total_Contents.csv", index=False)

    # reflect_data_to_gsheet(df=final_df)


def lambda_handler(event, context):
    # Run the main function
    main()
    return {"message": "Report successfully generated"}


if __name__ == "__main__":
    main()
