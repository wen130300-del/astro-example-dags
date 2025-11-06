"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    # Define tasks
    @task(
        # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Dataset("current_astronauts")]
    )  # Define that this task updates the `current_astronauts` Dataset
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        r = requests.get("http://api.open-notify.org/astros.json")
        number_of_people_in_space = r.json()["number"]
        list_of_people_in_space = r.json()["people"]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task(
        # Define a second dataset outlet for spacecraft summary data
        outlets=[Dataset("spacecraft_summary")]
    )
    def summarize_spacecraft(astronauts: list[dict]) -> dict:
        """
        This task aggregates astronaut data by spacecraft, counting
        how many astronauts are on each craft. The result is saved
        as a Dataset outlet that can trigger downstream DAGs.
        """
        summary = {}
        for person in astronauts:
            craft = person["craft"]
            summary[craft] = summary.get(craft, 0) + 1

        print("Spacecraft Summary:")
        for craft, count in summary.items():
            print(f"  {craft}: {count} astronaut(s)")

        return summary

    @task(
        # Define a third dataset outlet for aggregated statistics
        outlets=[Dataset("astronaut_statistics")]
    )
    def aggregate_astronaut_data(astronauts: list[dict], **context) -> dict:
        """
        This task processes astronaut data and creates comprehensive statistics
        including total count, spacecraft distribution, unique crafts, and
        name analysis. Returns a dictionary with all calculated statistics.
        """
        # Get total number from XCom (pushed by get_astronauts task)
        total_astronauts = context["ti"].xcom_pull(key="number_of_people_in_space")

        # Calculate spacecraft statistics
        spacecraft_distribution = {}
        unique_crafts = set()
        astronaut_names = []

        for person in astronauts:
            craft = person["craft"]
            name = person["name"]
            spacecraft_distribution[craft] = spacecraft_distribution.get(craft, 0) + 1
            unique_crafts.add(craft)
            astronaut_names.append(name)

        # Calculate additional statistics
        stats = {
            "total_astronauts": total_astronauts,
            "unique_spacecraft_count": len(unique_crafts),
            "spacecraft_list": list(unique_crafts),
            "spacecraft_distribution": spacecraft_distribution,
            "astronaut_count_verified": len(astronauts),
            "average_per_craft": round(len(astronauts) / len(unique_crafts), 2)
            if unique_crafts
            else 0,
            "most_populated_craft": max(
                spacecraft_distribution.items(), key=lambda x: x[1]
            )[0]
            if spacecraft_distribution
            else None,
            "astronaut_names": astronaut_names,
        }

        # Print statistics
        print("=" * 50)
        print("ASTRONAUT STATISTICS")
        print("=" * 50)
        print(f"Total Astronauts in Space: {stats['total_astronauts']}")
        print(f"Unique Spacecraft: {stats['unique_spacecraft_count']}")
        print(f"Average Astronauts per Craft: {stats['average_per_craft']}")
        print(f"\nMost Populated Craft: {stats['most_populated_craft']}")
        print(
            f"  ({spacecraft_distribution[stats['most_populated_craft']]} astronauts)"
        )
        print("\nSpacecraft Distribution:")
        for craft, count in spacecraft_distribution.items():
            percentage = (count / total_astronauts) * 100
            print(f"  {craft}: {count} ({percentage:.1f}%)")
        print("=" * 50)

        return stats

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    # Define dependencies and task flow
    astronaut_list = get_astronauts()

    # Summarize spacecraft data (produces spacecraft_summary Dataset)
    summarize_spacecraft(astronaut_list)

    # Aggregate astronaut data and create statistics (produces astronaut_statistics Dataset)
    aggregate_astronaut_data(astronaut_list)

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronaut_list
    )


# Instantiate the DAG
example_astronauts()
