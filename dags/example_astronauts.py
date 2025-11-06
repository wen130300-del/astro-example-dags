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

    @task(
        # Define a dataset outlet for weather data
        outlets=[Dataset("weather_data")]
    )
    def get_weather_data() -> dict:
        """
        This task fetches weather data from Open-Meteo API (free, no API key required).
        Uses coordinates for Houston, TX (NASA Johnson Space Center) to get relevant
        weather data. Returns weather metrics that will be correlated with astronaut data.
        """
        # Houston, TX coordinates (NASA Johnson Space Center)
        latitude = 29.5583
        longitude = -95.0853

        # Fetch current weather data from Open-Meteo API
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,pressure_msl,cloud_cover&timezone=America/Chicago"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            weather_data = {
                "location": "Houston, TX (NASA JSC)",
                "latitude": latitude,
                "longitude": longitude,
                "temperature_celsius": data["current"]["temperature_2m"],
                "humidity_percent": data["current"]["relative_humidity_2m"],
                "wind_speed_kmh": data["current"]["wind_speed_10m"],
                "pressure_hpa": data["current"]["pressure_msl"],
                "cloud_cover_percent": data["current"]["cloud_cover"],
                "timestamp": data["current"]["time"],
            }

            print("=" * 50)
            print("WEATHER DATA (NASA Johnson Space Center)")
            print("=" * 50)
            print(f"Location: {weather_data['location']}")
            print(f"Temperature: {weather_data['temperature_celsius']}°C")
            print(f"Humidity: {weather_data['humidity_percent']}%")
            print(f"Wind Speed: {weather_data['wind_speed_kmh']} km/h")
            print(f"Pressure: {weather_data['pressure_hpa']} hPa")
            print(f"Cloud Cover: {weather_data['cloud_cover_percent']}%")
            print(f"Timestamp: {weather_data['timestamp']}")
            print("=" * 50)

            return weather_data

        except Exception as e:
            print(f"Error fetching weather data: {e}")
            # Return default data if API fails
            return {
                "location": "Houston, TX (NASA JSC)",
                "latitude": latitude,
                "longitude": longitude,
                "temperature_celsius": 0,
                "humidity_percent": 0,
                "wind_speed_kmh": 0,
                "pressure_hpa": 0,
                "cloud_cover_percent": 0,
                "timestamp": "N/A",
                "error": str(e),
            }

    @task(
        # Define a dataset outlet for correlation analysis results
        outlets=[Dataset("correlation_analysis")]
    )
    def analyze_correlation(astronaut_stats: dict, weather_data: dict) -> dict:
        """
        This task analyzes the correlation between astronaut data and weather data.
        While these datasets are inherently independent, this demonstrates how to
        combine multiple data sources and perform comparative analysis.
        """
        # Extract key metrics
        total_astronauts = astronaut_stats.get("total_astronauts", 0)
        unique_spacecraft = astronaut_stats.get("unique_spacecraft_count", 0)
        avg_per_craft = astronaut_stats.get("average_per_craft", 0)

        temperature = weather_data.get("temperature_celsius", 0)
        humidity = weather_data.get("humidity_percent", 0)
        wind_speed = weather_data.get("wind_speed_kmh", 0)
        pressure = weather_data.get("pressure_hpa", 0)

        # Calculate correlation metrics and insights
        # Note: These are observational correlations for demonstration purposes
        analysis = {
            "data_sources": {
                "astronaut_data_timestamp": "current",
                "weather_data_timestamp": weather_data.get("timestamp", "N/A"),
                "weather_location": weather_data.get("location", "N/A"),
            },
            "astronaut_metrics": {
                "total_astronauts_in_space": total_astronauts,
                "unique_spacecraft": unique_spacecraft,
                "average_per_craft": avg_per_craft,
                "spacecraft_distribution": astronaut_stats.get(
                    "spacecraft_distribution", {}
                ),
            },
            "weather_metrics": {
                "temperature_celsius": temperature,
                "humidity_percent": humidity,
                "wind_speed_kmh": wind_speed,
                "pressure_hpa": pressure,
                "cloud_cover_percent": weather_data.get("cloud_cover_percent", 0),
            },
            "observational_insights": [],
            "data_quality_score": 0,
        }

        # Generate insights based on data patterns
        insights = []

        # Temperature insights
        if temperature > 25:
            insights.append(
                f"Ground temperature is high ({temperature}°C) - comfortable conditions at JSC"
            )
        elif temperature < 10:
            insights.append(
                f"Ground temperature is low ({temperature}°C) - cold conditions at JSC"
            )

        # Astronaut count insights
        if total_astronauts > 10:
            insights.append(
                f"High astronaut activity: {total_astronauts} people currently in space"
            )
        elif total_astronauts < 5:
            insights.append(
                f"Low astronaut activity: Only {total_astronauts} people in space"
            )

        # Pressure insights (space missions can be affected by weather at launch sites)
        if pressure > 1013:
            insights.append(
                f"High atmospheric pressure ({pressure} hPa) - stable launch conditions"
            )
        elif pressure < 1000:
            insights.append(
                f"Low atmospheric pressure ({pressure} hPa) - potential weather system"
            )

        # Spacecraft diversity insight
        if unique_spacecraft > 2:
            insights.append(
                f"High spacecraft diversity: {unique_spacecraft} different craft types in use"
            )

        # Calculate data quality score (0-100)
        quality_score = 100
        if weather_data.get("error"):
            quality_score -= 50
        if total_astronauts == 0:
            quality_score -= 30

        analysis["observational_insights"] = insights
        analysis["data_quality_score"] = quality_score

        # Print comprehensive correlation analysis
        print("=" * 70)
        print("CORRELATION ANALYSIS: ASTRONAUT DATA & WEATHER DATA")
        print("=" * 70)
        print("\n📊 ASTRONAUT METRICS:")
        print(f"  • Total Astronauts in Space: {total_astronauts}")
        print(f"  • Unique Spacecraft: {unique_spacecraft}")
        print(f"  • Average per Craft: {avg_per_craft}")

        print("\n🌦️  WEATHER METRICS (NASA JSC):")
        print(f"  • Temperature: {temperature}°C")
        print(f"  • Humidity: {humidity}%")
        print(f"  • Wind Speed: {wind_speed} km/h")
        print(f"  • Pressure: {pressure} hPa")

        print("\n💡 OBSERVATIONAL INSIGHTS:")
        for i, insight in enumerate(insights, 1):
            print(f"  {i}. {insight}")

        print(f"\n✅ Data Quality Score: {quality_score}/100")

        print("\n📝 ANALYSIS NOTES:")
        print(
            "  • Astronaut count and weather are independent but both monitored by NASA"
        )
        print(
            "  • Weather conditions affect launch windows and mission control operations"
        )
        print("  • This analysis demonstrates multi-source data integration in Airflow")

        print("=" * 70)

        return analysis

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

    # Parallel data processing tasks
    # Summarize spacecraft data (produces spacecraft_summary Dataset)
    summarize_spacecraft(astronaut_list)

    # Aggregate astronaut data and create statistics (produces astronaut_statistics Dataset)
    astronaut_statistics = aggregate_astronaut_data(astronaut_list)

    # Fetch weather data independently (produces weather_data Dataset)
    weather_info = get_weather_data()

    # Analyze correlation between astronaut and weather data (produces correlation_analysis Dataset)
    analyze_correlation(astronaut_statistics, weather_info)

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronaut_list
    )


# Instantiate the DAG
example_astronauts()
