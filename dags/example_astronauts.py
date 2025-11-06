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

# Define datasets for data-driven scheduling
astronauts_dataset = Dataset("current_astronauts")
astronaut_stats_dataset = Dataset("astronaut_statistics")
weather_dataset = Dataset("iss_weather_data")
aggregated_data_dataset = Dataset("aggregated_astronaut_weather_data")


# Helper functions for weather data fetching
def _try_weather_apis(latitude: float, longitude: float, timeout: int) -> dict:
    """
    Tries multiple weather APIs in sequence until one succeeds.
    Returns weather data dict or None if all fail.
    """
    # API 1: Open-Meteo (Free, no API key required)
    try:
        print("Attempting weather fetch from Open-Meteo API...")
        weather_url = "https://api.open-meteo.com/v1/forecast"
        weather_params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,cloud_cover",
            "temperature_unit": "fahrenheit",
        }
        weather_response = requests.get(
            weather_url, params=weather_params, timeout=timeout
        )
        weather_response.raise_for_status()
        weather_data = weather_response.json()
        if "current" in weather_data:
            current = weather_data["current"]
            result = {
                "iss_latitude": latitude,
                "iss_longitude": longitude,
                "temperature_fahrenheit": current.get("temperature_2m", 0.0),
                "humidity_percent": current.get("relative_humidity_2m", 0.0),
                "wind_speed_kmh": current.get("wind_speed_10m", 0.0),
                "cloud_cover_percent": current.get("cloud_cover", 0.0),
                "timestamp": current.get("time", str(dt.now())),
                "data_source": "open-meteo",
            }
            print(f"✅ Open-Meteo API succeeded: {result}")
            return result
    except Exception as e:
        print(f"❌ Open-Meteo API failed: {str(e)}")
    # API 2: WeatherAPI.com (Free tier, no key needed for basic requests)
    try:
        print("Attempting weather fetch from WeatherAPI.com...")
        weather_url = "https://api.weatherapi.com/v1/current.json"
        weather_params = {
            "q": f"{latitude},{longitude}",
            "key": "demo",  # Demo key for testing, replace with real key in production
        }
        weather_response = requests.get(
            weather_url, params=weather_params, timeout=timeout
        )
        # WeatherAPI may return 403 without valid key, skip to next
        if weather_response.status_code == 403:
            print("❌ WeatherAPI.com requires API key")
        else:
            weather_response.raise_for_status()
            weather_data = weather_response.json()
            if "current" in weather_data:
                current = weather_data["current"]
                result = {
                    "iss_latitude": latitude,
                    "iss_longitude": longitude,
                    "temperature_fahrenheit": current.get("temp_f", 0.0),
                    "humidity_percent": current.get("humidity", 0.0),
                    "wind_speed_kmh": current.get("wind_kph", 0.0),
                    "cloud_cover_percent": current.get("cloud", 0.0),
                    "timestamp": current.get("last_updated", str(dt.now())),
                    "data_source": "weatherapi.com",
                }
                print(f"✅ WeatherAPI.com succeeded: {result}")
                return result
    except Exception as e:
        print(f"❌ WeatherAPI.com failed: {str(e)}")
    # API 3: wttr.in (Free, simple API)
    try:
        print("Attempting weather fetch from wttr.in...")
        weather_url = f"https://wttr.in/{latitude},{longitude}"
        weather_params = {"format": "j1"}
        weather_response = requests.get(
            weather_url, params=weather_params, timeout=timeout
        )
        weather_response.raise_for_status()
        weather_data = weather_response.json()
        if (
            "current_condition" in weather_data
            and len(weather_data["current_condition"]) > 0
        ):
            current = weather_data["current_condition"][0]
            result = {
                "iss_latitude": latitude,
                "iss_longitude": longitude,
                "temperature_fahrenheit": float(current.get("temp_F", 0.0)),
                "humidity_percent": float(current.get("humidity", 0.0)),
                "wind_speed_kmh": float(current.get("windspeedKmph", 0.0)),
                "cloud_cover_percent": float(current.get("cloudcover", 0.0)),
                "timestamp": str(dt.now()),
                "data_source": "wttr.in",
            }
            print(f"✅ wttr.in API succeeded: {result}")
            return result
    except Exception as e:
        print(f"❌ wttr.in API failed: {str(e)}")
    # All APIs failed
    print("❌ All weather APIs failed")
    return None


def _get_fallback_weather_data() -> dict:
    """
    Returns fallback weather data for Houston, TX (NASA Johnson Space Center)
    when ISS location data is unavailable or over oceans/poles.
    """
    API_TIMEOUT = 10
    try:
        # Houston, TX coordinates (NASA Johnson Space Center)
        latitude, longitude = 29.5583, -95.0853
        print(f"Fetching fallback weather for Houston, TX: {latitude}°N, {longitude}°W")
        weather_url = "https://api.open-meteo.com/v1/forecast"
        weather_params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,cloud_cover",
            "temperature_unit": "fahrenheit",
        }
        weather_response = requests.get(
            weather_url, params=weather_params, timeout=API_TIMEOUT
        )
        weather_response.raise_for_status()
        weather_data = weather_response.json()
        current = weather_data["current"]
        result = {
            "iss_latitude": latitude,
            "iss_longitude": longitude,
            "temperature_fahrenheit": current.get("temperature_2m", 72.0),
            "humidity_percent": current.get("relative_humidity_2m", 60.0),
            "wind_speed_kmh": current.get("wind_speed_10m", 10.0),
            "cloud_cover_percent": current.get("cloud_cover", 30.0),
            "timestamp": current.get("time", str(dt.now())),
            "data_source": "fallback_houston_tx",
        }
        print(f"Fallback weather data retrieved: {result}")
        return result
    except Exception as e:
        print(f"ERROR: Even fallback data failed: {str(e)}")
        # Return static default data as last resort
        return {
            "iss_latitude": 29.5583,
            "iss_longitude": -95.0853,
            "temperature_fahrenheit": 72.0,
            "humidity_percent": 60.0,
            "wind_speed_kmh": 10.0,
            "cloud_cover_percent": 30.0,
            "timestamp": str(dt.now()),
            "data_source": "static_default",
        }


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
        r.raise_for_status()
        number_of_people_in_space = r.json()["number"]
        list_of_people_in_space = r.json()["people"]
        print(f"Successfully retrieved data for {number_of_people_in_space} astronauts")

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
        # Define a dataset outlet for calculated statistics
        outlets=[Dataset("calculated_statistics")]
    )
    def calculate_astronauts_stats(astronauts: list[dict], **context) -> dict:
        """
        This task performs advanced statistical calculations on astronaut data
        including name length analysis, spacecraft capacity metrics, distribution
        patterns, and statistical measures (mean, median, variance, etc.).
        """
        # Get base data
        total_astronauts = context["ti"].xcom_pull(key="number_of_people_in_space")

        # Initialize collections
        spacecraft_counts = {}
        name_lengths = []
        names_by_craft = {}

        # Collect data for calculations
        for person in astronauts:
            craft = person["craft"]
            name = person["name"]

            # Count by spacecraft
            spacecraft_counts[craft] = spacecraft_counts.get(craft, 0) + 1

            # Track name lengths
            name_lengths.append(len(name))

            # Group names by craft
            if craft not in names_by_craft:
                names_by_craft[craft] = []
            names_by_craft[craft].append(name)

        # Calculate statistical measures
        num_spacecraft = len(spacecraft_counts)
        counts_list = list(spacecraft_counts.values())

        # Mean calculations
        mean_per_craft = sum(counts_list) / num_spacecraft if num_spacecraft > 0 else 0
        mean_name_length = sum(name_lengths) / len(name_lengths) if name_lengths else 0

        # Median calculation
        sorted_counts = sorted(counts_list)
        n = len(sorted_counts)
        if n == 0:
            median_per_craft = 0
        elif n % 2 == 0:
            median_per_craft = (sorted_counts[n // 2 - 1] + sorted_counts[n // 2]) / 2
        else:
            median_per_craft = sorted_counts[n // 2]

        # Variance and Standard Deviation
        if num_spacecraft > 0:
            variance = (
                sum((x - mean_per_craft) ** 2 for x in counts_list) / num_spacecraft
            )
            std_deviation = variance**0.5
        else:
            variance = 0
            std_deviation = 0

        # Range
        min_per_craft = min(counts_list) if counts_list else 0
        max_per_craft = max(counts_list) if counts_list else 0
        range_per_craft = max_per_craft - min_per_craft

        # Coefficient of Variation (relative variability)
        coefficient_variation = (
            (std_deviation / mean_per_craft * 100) if mean_per_craft > 0 else 0
        )

        # Distribution analysis
        distribution_pattern = "uniform"
        if std_deviation > mean_per_craft * 0.5:
            distribution_pattern = "highly_variable"
        elif std_deviation > mean_per_craft * 0.3:
            distribution_pattern = "moderately_variable"

        # Name analysis
        shortest_name_length = min(name_lengths) if name_lengths else 0
        longest_name_length = max(name_lengths) if name_lengths else 0
        name_length_range = longest_name_length - shortest_name_length

        # Capacity utilization (assuming theoretical max capacity)
        theoretical_max_capacity = num_spacecraft * 10  # Assume max 10 per spacecraft
        capacity_utilization = (
            (total_astronauts / theoretical_max_capacity * 100)
            if theoretical_max_capacity > 0
            else 0
        )

        # Compile all statistics
        calculated_stats = {
            "basic_metrics": {
                "total_astronauts": total_astronauts,
                "number_of_spacecraft": num_spacecraft,
                "spacecraft_names": list(spacecraft_counts.keys()),
            },
            "central_tendency": {
                "mean_astronauts_per_craft": round(mean_per_craft, 2),
                "median_astronauts_per_craft": round(median_per_craft, 2),
                "mean_name_length": round(mean_name_length, 2),
            },
            "variability_measures": {
                "variance": round(variance, 2),
                "standard_deviation": round(std_deviation, 2),
                "coefficient_of_variation_percent": round(coefficient_variation, 2),
                "range": range_per_craft,
                "min_per_craft": min_per_craft,
                "max_per_craft": max_per_craft,
            },
            "distribution_analysis": {
                "pattern": distribution_pattern,
                "spacecraft_distribution": spacecraft_counts,
                "distribution_evenness_score": round(
                    100 - coefficient_variation, 2
                ),  # Higher = more even
            },
            "name_statistics": {
                "mean_name_length": round(mean_name_length, 2),
                "shortest_name_length": shortest_name_length,
                "longest_name_length": longest_name_length,
                "name_length_range": name_length_range,
            },
            "capacity_metrics": {
                "theoretical_max_capacity": theoretical_max_capacity,
                "current_utilization_percent": round(capacity_utilization, 2),
                "available_capacity": theoretical_max_capacity - total_astronauts,
            },
        }

        # Print comprehensive statistics
        print("=" * 70)
        print("CALCULATED ASTRONAUT STATISTICS")
        print("=" * 70)

        print("\n📊 BASIC METRICS:")
        print(f"  • Total Astronauts: {total_astronauts}")
        print(f"  • Number of Spacecraft: {num_spacecraft}")

        print("\n📈 CENTRAL TENDENCY:")
        print(
            f"  • Mean Astronauts per Craft: {calculated_stats['central_tendency']['mean_astronauts_per_craft']}"
        )
        print(
            f"  • Median Astronauts per Craft: {calculated_stats['central_tendency']['median_astronauts_per_craft']}"
        )

        print("\n📉 VARIABILITY MEASURES:")
        print(f"  • Standard Deviation: {std_deviation:.2f}")
        print(f"  • Variance: {variance:.2f}")
        print(f"  • Coefficient of Variation: {coefficient_variation:.2f}%")
        print(
            f"  • Range: {range_per_craft} (Min: {min_per_craft}, Max: {max_per_craft})"
        )

        print("\n🎯 DISTRIBUTION ANALYSIS:")
        print(f"  • Pattern: {distribution_pattern.replace('_', ' ').title()}")
        print(
            f"  • Evenness Score: {calculated_stats['distribution_analysis']['distribution_evenness_score']}/100"
        )
        print("  • Per Spacecraft:")
        for craft, count in spacecraft_counts.items():
            deviation = count - mean_per_craft
            print(f"    - {craft}: {count} (deviation: {deviation:+.2f})")

        print("\n✍️  NAME STATISTICS:")
        print(f"  • Mean Name Length: {mean_name_length:.2f} characters")
        print(
            f"  • Range: {shortest_name_length}-{longest_name_length} characters (span: {name_length_range})"
        )

        print("\n🚀 CAPACITY METRICS:")
        print(f"  • Theoretical Max Capacity: {theoretical_max_capacity}")
        print(f"  • Current Utilization: {capacity_utilization:.2f}%")
        print(
            f"  • Available Capacity: {calculated_stats['capacity_metrics']['available_capacity']}"
        )

        print("\n💡 STATISTICAL INSIGHTS:")
        if distribution_pattern == "uniform":
            print("  • Spacecraft have roughly equal crew distribution")
        elif distribution_pattern == "moderately_variable":
            print("  • Spacecraft show moderate variation in crew sizes")
        else:
            print("  • Spacecraft show high variation in crew sizes")

        if coefficient_variation < 20:
            print("  • Low variability indicates consistent crew allocation")
        elif coefficient_variation < 50:
            print("  • Moderate variability in crew distribution")
        else:
            print("  • High variability suggests diverse spacecraft missions")

        print("=" * 70)

        return calculated_stats

    @task(
        # Define a dataset outlet for statistical methods results
        outlets=[Dataset("statistical_methods")]
    )
    def perform_statistical_methods(
        astronauts: list[dict], calculated_stats: dict, **context
    ) -> dict:
        """
        This task performs advanced statistical methods including percentile analysis,
        z-scores, quartile calculations, interquartile range, skewness detection,
        and distribution testing on astronaut data.
        """
        import math

        # Get astronaut count
        context["ti"].xcom_pull(key="number_of_people_in_space")

        # Collect spacecraft crew counts
        spacecraft_counts = {}
        for person in astronauts:
            craft = person["craft"]
            spacecraft_counts[craft] = spacecraft_counts.get(craft, 0) + 1

        counts_list = list(spacecraft_counts.values())
        sorted(counts_list)
        n = len(counts_list)

        if n == 0:
            # Return empty results if no data
            return {"error": "No data available for statistical analysis"}

        # Extract basic stats from calculated_stats
        mean = calculated_stats.get("central_tendency", {}).get(
            "mean_astronauts_per_craft", 0
        )
        std_dev = calculated_stats.get("variability_measures", {}).get(
            "standard_deviation", 0
        )
        calculated_stats.get("variability_measures", {}).get("variance", 0)

        # ========== PERCENTILE ANALYSIS ==========
        def calculate_percentile(data, percentile):
            """Calculate percentile using linear interpolation"""
            if not data:
                return 0
            sorted_data = sorted(data)
            k = (len(sorted_data) - 1) * percentile / 100
            f = math.floor(k)
            c = math.ceil(k)
            if f == c:
                return sorted_data[int(k)]
            d0 = sorted_data[int(f)] * (c - k)
            d1 = sorted_data[int(c)] * (k - f)
            return d0 + d1

        p25 = calculate_percentile(counts_list, 25)
        p50 = calculate_percentile(counts_list, 50)  # Median
        p75 = calculate_percentile(counts_list, 75)
        p90 = calculate_percentile(counts_list, 90)
        p95 = calculate_percentile(counts_list, 95)

        # ========== QUARTILE ANALYSIS ==========
        q1 = p25
        q2 = p50
        q3 = p75
        iqr = q3 - q1  # Interquartile Range

        # Outlier detection using IQR method
        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr
        outliers = [x for x in counts_list if x < lower_fence or x > upper_fence]

        # ========== Z-SCORE ANALYSIS ==========
        z_scores = {}
        for craft, count in spacecraft_counts.items():
            if std_dev > 0:
                z_score = (count - mean) / std_dev
                z_scores[craft] = round(z_score, 3)
            else:
                z_scores[craft] = 0

        # ========== SKEWNESS CALCULATION ==========
        # Pearson's moment coefficient of skewness
        if std_dev > 0 and n > 0:
            skewness = sum(((x - mean) / std_dev) ** 3 for x in counts_list) / n
        else:
            skewness = 0

        # Interpret skewness
        if abs(skewness) < 0.5:
            skewness_interpretation = "approximately symmetric"
        elif skewness > 0:
            skewness_interpretation = "positively skewed (right tail)"
        else:
            skewness_interpretation = "negatively skewed (left tail)"

        # ========== KURTOSIS CALCULATION ==========
        # Excess kurtosis (normal distribution has kurtosis of 0)
        if std_dev > 0 and n > 0:
            kurtosis = (sum(((x - mean) / std_dev) ** 4 for x in counts_list) / n) - 3
        else:
            kurtosis = 0

        # Interpret kurtosis
        if abs(kurtosis) < 0.5:
            kurtosis_interpretation = "mesokurtic (normal-like)"
        elif kurtosis > 0:
            kurtosis_interpretation = "leptokurtic (heavy tails)"
        else:
            kurtosis_interpretation = "platykurtic (light tails)"

        # ========== RANGE AND SPREAD ANALYSIS ==========
        data_range = max(counts_list) - min(counts_list) if counts_list else 0
        mid_range = (max(counts_list) + min(counts_list)) / 2 if counts_list else 0

        # Coefficient of quartile deviation
        coeff_quartile_dev = (q3 - q1) / (q3 + q1) if q1 + q3 > 0 else 0

        # ========== DISTRIBUTION TESTING ==========
        # Check for uniform distribution (all values are equal)
        is_uniform = len(set(counts_list)) == 1 if counts_list else False

        # Check for normal distribution indicators
        normal_indicators = {
            "mean_median_close": abs(mean - p50) < std_dev * 0.1
            if std_dev > 0
            else True,
            "skewness_near_zero": abs(skewness) < 0.5,
            "kurtosis_near_zero": abs(kurtosis) < 0.5,
        }
        likely_normal = all(normal_indicators.values())

        # ========== COMPILE RESULTS ==========
        statistical_methods_results = {
            "percentile_analysis": {
                "p25": round(p25, 2),
                "p50_median": round(p50, 2),
                "p75": round(p75, 2),
                "p90": round(p90, 2),
                "p95": round(p95, 2),
            },
            "quartile_analysis": {
                "q1_25th": round(q1, 2),
                "q2_median": round(q2, 2),
                "q3_75th": round(q3, 2),
                "iqr": round(iqr, 2),
                "lower_fence": round(lower_fence, 2),
                "upper_fence": round(upper_fence, 2),
                "outliers": outliers,
                "outlier_count": len(outliers),
            },
            "z_score_analysis": {
                "by_spacecraft": z_scores,
                "max_z_score": round(max(z_scores.values()), 3) if z_scores else 0,
                "min_z_score": round(min(z_scores.values()), 3) if z_scores else 0,
            },
            "shape_measures": {
                "skewness": round(skewness, 4),
                "skewness_interpretation": skewness_interpretation,
                "kurtosis": round(kurtosis, 4),
                "kurtosis_interpretation": kurtosis_interpretation,
            },
            "spread_measures": {
                "range": data_range,
                "mid_range": round(mid_range, 2),
                "iqr": round(iqr, 2),
                "coefficient_of_quartile_deviation": round(coeff_quartile_dev, 4),
            },
            "distribution_tests": {
                "is_uniform": is_uniform,
                "likely_normal_distribution": likely_normal,
                "normal_indicators": normal_indicators,
            },
        }

        # ========== PRINT DETAILED REPORT ==========
        print("=" * 80)
        print("ADVANCED STATISTICAL METHODS ANALYSIS")
        print("=" * 80)

        print("\n📊 PERCENTILE ANALYSIS:")
        print(f"  • 25th Percentile (P25): {p25:.2f}")
        print(f"  • 50th Percentile (Median): {p50:.2f}")
        print(f"  • 75th Percentile (P75): {p75:.2f}")
        print(f"  • 90th Percentile (P90): {p90:.2f}")
        print(f"  • 95th Percentile (P95): {p95:.2f}")

        print("\n📦 QUARTILE & OUTLIER ANALYSIS:")
        print(f"  • Q1 (25th): {q1:.2f}")
        print(f"  • Q2 (Median): {q2:.2f}")
        print(f"  • Q3 (75th): {q3:.2f}")
        print(f"  • IQR (Interquartile Range): {iqr:.2f}")
        print(f"  • Lower Fence: {lower_fence:.2f}")
        print(f"  • Upper Fence: {upper_fence:.2f}")
        print(f"  • Outliers Detected: {len(outliers)}")
        if outliers:
            print(f"    Values: {outliers}")

        print("\n📈 Z-SCORE ANALYSIS (Standard Deviations from Mean):")
        for craft, z in sorted(z_scores.items(), key=lambda x: x[1], reverse=True):
            interpretation = ""
            if abs(z) > 2:
                interpretation = " (unusual)"
            elif abs(z) > 1:
                interpretation = " (moderate)"
            print(f"  • {craft}: {z:+.3f}{interpretation}")

        print("\n🎭 DISTRIBUTION SHAPE:")
        print(f"  • Skewness: {skewness:.4f} ({skewness_interpretation})")
        print(f"  • Kurtosis: {kurtosis:.4f} ({kurtosis_interpretation})")

        print("\n📏 SPREAD MEASURES:")
        print(f"  • Range: {data_range}")
        print(f"  • Mid-Range: {mid_range:.2f}")
        print(f"  • IQR: {iqr:.2f}")
        print(f"  • Coefficient of Quartile Deviation: {coeff_quartile_dev:.4f}")

        print("\n🧪 DISTRIBUTION TESTS:")
        print(f"  • Uniform Distribution: {'Yes' if is_uniform else 'No'}")
        print(f"  • Likely Normal Distribution: {'Yes' if likely_normal else 'No'}")
        if not likely_normal:
            print("    Indicators:")
            for indicator, value in normal_indicators.items():
                status = "✓" if value else "✗"
                print(f"      {status} {indicator.replace('_', ' ').title()}")

        print("\n💡 STATISTICAL INSIGHTS:")
        if len(outliers) > 0:
            print(
                f"  • {len(outliers)} outlier(s) detected - some spacecraft have unusual crew sizes"
            )
        else:
            print("  • No outliers detected - crew distribution is consistent")

        if likely_normal:
            print(
                "  • Data follows normal distribution - crew allocation is well-balanced"
            )
        else:
            print(
                "  • Data deviates from normal distribution - varied mission requirements"
            )

        if abs(skewness) > 0.5:
            print(
                f"  • Distribution is skewed - crew sizes tend toward {'higher' if skewness > 0 else 'lower'} values"
            )

        print("=" * 80)

        return statistical_methods_results

    @task(
        # Define a dataset outlet for comprehensive analysis
        outlets=[Dataset("comprehensive_analysis")]
    )
    def perform_comprehensive_analysis(
        astronaut_list: list[dict],
        astronaut_stats: dict,
        calculated_stats: dict,
        statistical_methods: dict,
        **context,
    ) -> dict:
        """
        This task performs comprehensive multi-dimensional analysis combining
        data from multiple sources. It provides trend analysis, anomaly detection,
        risk assessment, efficiency metrics, and strategic recommendations.
        """
        from datetime import datetime as dt

        # Extract execution context
        execution_date = context.get("execution_date", dt.now())
        total_astronauts = context["ti"].xcom_pull(key="number_of_people_in_space")

        # Extract data from all sources
        spacecraft_distribution = astronaut_stats.get("spacecraft_distribution", {})
        mean_per_craft = calculated_stats.get("central_tendency", {}).get(
            "mean_astronauts_per_craft", 0
        )
        std_dev = calculated_stats.get("variability_measures", {}).get(
            "standard_deviation", 0
        )
        distribution_pattern = calculated_stats.get("distribution_analysis", {}).get(
            "pattern", "unknown"
        )
        capacity_utilization = calculated_stats.get("capacity_metrics", {}).get(
            "current_utilization_percent", 0
        )

        # Statistical methods data
        outliers = statistical_methods.get("quartile_analysis", {}).get("outliers", [])
        z_scores = statistical_methods.get("z_score_analysis", {}).get(
            "by_spacecraft", {}
        )
        skewness = statistical_methods.get("shape_measures", {}).get("skewness", 0)
        is_normal = statistical_methods.get("distribution_tests", {}).get(
            "likely_normal_distribution", False
        )

        # ========== TREND ANALYSIS ==========
        # Analyze crew size trends
        crew_sizes = list(spacecraft_distribution.values())
        trend_direction = "stable"
        if crew_sizes:
            sorted_sizes = sorted(crew_sizes, reverse=True)
            if len(sorted_sizes) >= 2:
                top_diff = sorted_sizes[0] - sorted_sizes[-1]
                if top_diff > mean_per_craft:
                    trend_direction = "concentrated"  # Few spacecraft with many crew
                elif std_dev < mean_per_craft * 0.3:
                    trend_direction = "distributed"  # Even distribution

        # Capacity trend
        capacity_trend = "nominal"
        if capacity_utilization < 30:
            capacity_trend = "underutilized"
        elif capacity_utilization > 70:
            capacity_trend = "high_utilization"
        elif 40 <= capacity_utilization <= 60:
            capacity_trend = "optimal"

        # ========== ANOMALY DETECTION ==========
        anomalies = []

        # Detect outlier spacecraft
        if outliers:
            anomalies.append(
                {
                    "type": "statistical_outlier",
                    "severity": "medium",
                    "description": f"{len(outliers)} spacecraft with unusual crew sizes detected",
                    "values": outliers,
                }
            )

        # Detect extreme z-scores
        extreme_z_scores = {craft: z for craft, z in z_scores.items() if abs(z) > 2}
        if extreme_z_scores:
            anomalies.append(
                {
                    "type": "extreme_deviation",
                    "severity": "high",
                    "description": f"{len(extreme_z_scores)} spacecraft significantly deviate from mean",
                    "details": extreme_z_scores,
                }
            )

        # Detect imbalanced distribution
        if abs(skewness) > 1.0:
            anomalies.append(
                {
                    "type": "distribution_skew",
                    "severity": "low",
                    "description": f"Highly skewed distribution (skewness: {skewness:.2f})",
                    "impact": "Crew allocation may be imbalanced",
                }
            )

        # Detect low astronaut count
        if total_astronauts < 5:
            anomalies.append(
                {
                    "type": "low_crew_count",
                    "severity": "medium",
                    "description": f"Low total astronaut count: {total_astronauts}",
                    "impact": "Reduced operational capacity",
                }
            )

        # ========== RISK ASSESSMENT ==========
        risk_factors = []
        risk_score = 0

        # Capacity risk
        if capacity_utilization > 80:
            risk_factors.append(
                {
                    "risk": "overcapacity",
                    "level": "high",
                    "impact": "Limited flexibility for mission expansion",
                    "mitigation": "Plan crew rotation or additional spacecraft",
                }
            )
            risk_score += 30
        elif capacity_utilization < 20:
            risk_factors.append(
                {
                    "risk": "underutilization",
                    "level": "medium",
                    "impact": "Inefficient resource allocation",
                    "mitigation": "Increase mission frequency or consolidate resources",
                }
            )
            risk_score += 15

        # Distribution risk
        if not is_normal and len(outliers) > 0:
            risk_factors.append(
                {
                    "risk": "uneven_distribution",
                    "level": "medium",
                    "impact": "Potential operational inefficiencies",
                    "mitigation": "Review crew allocation strategy",
                }
            )
            risk_score += 20

        # Concentration risk
        if spacecraft_distribution:
            max_crew_pct = (
                max(spacecraft_distribution.values()) / total_astronauts * 100
            )
            if max_crew_pct > 70:
                risk_factors.append(
                    {
                        "risk": "crew_concentration",
                        "level": "high",
                        "impact": f"{max_crew_pct:.0f}% of crew on single spacecraft",
                        "mitigation": "Diversify crew across multiple spacecraft",
                    }
                )
                risk_score += 35

        # Calculate overall risk level
        if risk_score >= 50:
            overall_risk = "HIGH"
        elif risk_score >= 30:
            overall_risk = "MEDIUM"
        else:
            overall_risk = "LOW"

        # ========== EFFICIENCY METRICS ==========
        efficiency_metrics = {
            "capacity_efficiency": capacity_utilization,
            "distribution_efficiency": 100
            - (std_dev / mean_per_craft * 100 if mean_per_craft > 0 else 0),
            "spacecraft_utilization_rate": (
                len([v for v in crew_sizes if v > 0])
                / len(spacecraft_distribution)
                * 100
                if spacecraft_distribution
                else 0
            ),
        }

        # Overall efficiency score (weighted average)
        overall_efficiency = (
            efficiency_metrics["capacity_efficiency"] * 0.4
            + efficiency_metrics["distribution_efficiency"] * 0.4
            + efficiency_metrics["spacecraft_utilization_rate"] * 0.2
        )

        # ========== PREDICTIVE INSIGHTS ==========
        predictions = []

        if capacity_trend == "high_utilization":
            predictions.append(
                {
                    "insight": "Approaching capacity limits",
                    "timeframe": "near_term",
                    "confidence": "high",
                    "recommendation": "Prepare for crew rotation or additional capacity",
                }
            )

        if trend_direction == "concentrated":
            predictions.append(
                {
                    "insight": "Crew concentration increasing",
                    "timeframe": "current",
                    "confidence": "medium",
                    "recommendation": "Consider load balancing across spacecraft",
                }
            )

        if overall_efficiency < 60:
            predictions.append(
                {
                    "insight": "Efficiency below optimal levels",
                    "timeframe": "current",
                    "confidence": "high",
                    "recommendation": "Review resource allocation and mission planning",
                }
            )

        # ========== STRATEGIC RECOMMENDATIONS ==========
        recommendations = []

        # Capacity recommendations
        if capacity_utilization < 30:
            recommendations.append(
                {
                    "priority": "medium",
                    "category": "capacity_planning",
                    "action": "Increase mission frequency to improve resource utilization",
                    "expected_impact": "15-25% efficiency gain",
                }
            )
        elif capacity_utilization > 80:
            recommendations.append(
                {
                    "priority": "high",
                    "category": "capacity_planning",
                    "action": "Expand capacity or implement strict crew rotation schedule",
                    "expected_impact": "Prevent operational constraints",
                }
            )

        # Distribution recommendations
        if not is_normal:
            recommendations.append(
                {
                    "priority": "medium",
                    "category": "crew_allocation",
                    "action": "Rebalance crew distribution to achieve normal distribution",
                    "expected_impact": "Improved operational efficiency",
                }
            )

        # Risk mitigation recommendations
        if overall_risk == "HIGH":
            recommendations.append(
                {
                    "priority": "high",
                    "category": "risk_mitigation",
                    "action": "Address identified high-risk factors immediately",
                    "expected_impact": "Reduce operational risk by 30-40%",
                }
            )

        # Efficiency recommendations
        if overall_efficiency < 70:
            recommendations.append(
                {
                    "priority": "high",
                    "category": "efficiency_improvement",
                    "action": "Implement efficiency optimization program",
                    "expected_impact": f"Potential to reach 85%+ efficiency from current {overall_efficiency:.1f}%",
                }
            )

        # ========== COMPILE COMPREHENSIVE ANALYSIS ==========
        comprehensive_analysis = {
            "analysis_metadata": {
                "analysis_date": str(execution_date),
                "data_sources": 4,
                "astronaut_count": total_astronauts,
            },
            "trend_analysis": {
                "crew_distribution_trend": trend_direction,
                "capacity_trend": capacity_trend,
                "distribution_pattern": distribution_pattern,
            },
            "anomaly_detection": {
                "anomalies_detected": len(anomalies),
                "anomalies": anomalies,
            },
            "risk_assessment": {
                "overall_risk_level": overall_risk,
                "risk_score": risk_score,
                "risk_factors": risk_factors,
            },
            "efficiency_metrics": {
                "overall_efficiency_score": round(overall_efficiency, 2),
                "capacity_efficiency": round(
                    efficiency_metrics["capacity_efficiency"], 2
                ),
                "distribution_efficiency": round(
                    efficiency_metrics["distribution_efficiency"], 2
                ),
                "spacecraft_utilization": round(
                    efficiency_metrics["spacecraft_utilization_rate"], 2
                ),
            },
            "predictive_insights": predictions,
            "strategic_recommendations": recommendations,
        }

        # ========== PRINT COMPREHENSIVE ANALYSIS REPORT ==========
        print("\n")
        print("=" * 90)
        print("╔" + "═" * 88 + "╗")
        print("║" + " " * 25 + "COMPREHENSIVE ANALYSIS REPORT" + " " * 34 + "║")
        print("╚" + "═" * 88 + "╝")
        print("=" * 90)

        print("\n📅 ANALYSIS METADATA:")
        print(f"  • Analysis Date: {execution_date}")
        print(f"  • Total Astronauts: {total_astronauts}")
        print(
            f"  • Data Sources Analyzed: {comprehensive_analysis['analysis_metadata']['data_sources']}"
        )

        print("\n" + "=" * 90)
        print("SECTION 1: TREND ANALYSIS")
        print("=" * 90)
        print(f"  • Crew Distribution Trend: {trend_direction.upper()}")
        print(f"  • Capacity Trend: {capacity_trend.upper()}")
        print(
            f"  • Distribution Pattern: {distribution_pattern.replace('_', ' ').title()}"
        )

        print("\n" + "=" * 90)
        print("SECTION 2: ANOMALY DETECTION")
        print("=" * 90)
        print(f"  • Anomalies Detected: {len(anomalies)}")
        if anomalies:
            for i, anomaly in enumerate(anomalies, 1):
                severity_icon = (
                    "🔴"
                    if anomaly["severity"] == "high"
                    else "🟡"
                    if anomaly["severity"] == "medium"
                    else "🟢"
                )
                print(f"\n  {severity_icon} Anomaly {i}: {anomaly['type'].upper()}")
                print(f"     Severity: {anomaly['severity'].upper()}")
                print(f"     Description: {anomaly['description']}")
        else:
            print("  ✅ No anomalies detected - all metrics within normal ranges")

        print("\n" + "=" * 90)
        print("SECTION 3: RISK ASSESSMENT")
        print("=" * 90)
        risk_icon = (
            "🔴"
            if overall_risk == "HIGH"
            else "🟡"
            if overall_risk == "MEDIUM"
            else "🟢"
        )
        print(f"  {risk_icon} Overall Risk Level: {overall_risk}")
        print(f"  • Risk Score: {risk_score}/100")
        print(f"  • Risk Factors Identified: {len(risk_factors)}")
        if risk_factors:
            for i, risk in enumerate(risk_factors, 1):
                print(f"\n  Risk Factor {i}:")
                print(f"     Type: {risk['risk'].replace('_', ' ').title()}")
                print(f"     Level: {risk['level'].upper()}")
                print(f"     Impact: {risk['impact']}")
                print(f"     Mitigation: {risk['mitigation']}")

        print("\n" + "=" * 90)
        print("SECTION 4: EFFICIENCY METRICS")
        print("=" * 90)
        eff_icon = (
            "🟢"
            if overall_efficiency >= 80
            else "🟡"
            if overall_efficiency >= 60
            else "🔴"
        )
        print(f"  {eff_icon} Overall Efficiency Score: {overall_efficiency:.2f}%")
        print(
            f"  • Capacity Efficiency: {efficiency_metrics['capacity_efficiency']:.2f}%"
        )
        print(
            f"  • Distribution Efficiency: {efficiency_metrics['distribution_efficiency']:.2f}%"
        )
        print(
            f"  • Spacecraft Utilization: {efficiency_metrics['spacecraft_utilization_rate']:.2f}%"
        )

        print("\n" + "=" * 90)
        print("SECTION 5: PREDICTIVE INSIGHTS")
        print("=" * 90)
        if predictions:
            for i, pred in enumerate(predictions, 1):
                print(f"\n  🔮 Insight {i}:")
                print(f"     Prediction: {pred['insight']}")
                print(f"     Timeframe: {pred['timeframe'].replace('_', ' ').title()}")
                print(f"     Confidence: {pred['confidence'].upper()}")
                print(f"     Recommendation: {pred['recommendation']}")
        else:
            print("  • No predictive insights generated - current state is stable")

        print("\n" + "=" * 90)
        print("SECTION 6: STRATEGIC RECOMMENDATIONS")
        print("=" * 90)
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                priority_icon = "🔴" if rec["priority"] == "high" else "🟡"
                print(
                    f"\n  {priority_icon} Recommendation {i} (Priority: {rec['priority'].upper()}):"
                )
                print(f"     Category: {rec['category'].replace('_', ' ').title()}")
                print(f"     Action: {rec['action']}")
                print(f"     Expected Impact: {rec['expected_impact']}")
        else:
            print("  ✅ No specific recommendations - operations are optimal")

        print("\n" + "=" * 90)
        print("END OF COMPREHENSIVE ANALYSIS")
        print("=" * 90)
        print("\n")

        return comprehensive_analysis

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
    def analyze_correlation(
        astronaut_stats: dict, weather_data: dict, calculated_stats: dict
    ) -> dict:
        """
        This task analyzes the correlation between astronaut data, weather data,
        and calculated statistics. While these datasets are inherently independent,
        this demonstrates how to combine multiple data sources and perform
        comprehensive comparative analysis with statistical measures.
        """
        # Extract key metrics from astronaut_stats
        total_astronauts = astronaut_stats.get("total_astronauts", 0)
        unique_spacecraft = astronaut_stats.get("unique_spacecraft_count", 0)
        avg_per_craft = astronaut_stats.get("average_per_craft", 0)

        # Extract calculated statistics
        std_deviation = calculated_stats.get("variability_measures", {}).get(
            "standard_deviation", 0
        )
        coefficient_variation = calculated_stats.get("variability_measures", {}).get(
            "coefficient_of_variation_percent", 0
        )
        distribution_pattern = calculated_stats.get("distribution_analysis", {}).get(
            "pattern", "unknown"
        )
        evenness_score = calculated_stats.get("distribution_analysis", {}).get(
            "distribution_evenness_score", 0
        )
        capacity_utilization = calculated_stats.get("capacity_metrics", {}).get(
            "current_utilization_percent", 0
        )

        # Extract weather metrics
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
            "calculated_statistics": {
                "standard_deviation": std_deviation,
                "coefficient_of_variation": coefficient_variation,
                "distribution_pattern": distribution_pattern,
                "evenness_score": evenness_score,
                "capacity_utilization": capacity_utilization,
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

        # Statistical distribution insights
        if distribution_pattern == "uniform":
            insights.append(
                f"Crew distribution is uniform (CV: {coefficient_variation:.1f}%) - balanced allocation"
            )
        elif distribution_pattern == "highly_variable":
            insights.append(
                f"Crew distribution is highly variable (CV: {coefficient_variation:.1f}%) - diverse missions"
            )

        # Capacity insights
        if capacity_utilization < 30:
            insights.append(
                f"Low capacity utilization ({capacity_utilization:.1f}%) - room for expansion"
            )
        elif capacity_utilization > 70:
            insights.append(
                f"High capacity utilization ({capacity_utilization:.1f}%) - near maximum"
            )

        # Cross-correlation insights (statistical patterns with environmental conditions)
        if coefficient_variation < 20 and pressure > 1013:
            insights.append(
                "Stable crew distribution paired with stable atmospheric conditions"
            )

        if evenness_score > 80 and temperature > 20:
            insights.append(
                f"Optimal conditions: even crew distribution ({evenness_score:.0f}/100) and comfortable ground temp"
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
        print("=" * 80)
        print("CORRELATION ANALYSIS: ASTRONAUT, WEATHER & STATISTICAL DATA")
        print("=" * 80)
        print("\n📊 ASTRONAUT METRICS:")
        print(f"  • Total Astronauts in Space: {total_astronauts}")
        print(f"  • Unique Spacecraft: {unique_spacecraft}")
        print(f"  • Average per Craft: {avg_per_craft}")

        print("\n📈 CALCULATED STATISTICS:")
        print(f"  • Standard Deviation: {std_deviation:.2f}")
        print(f"  • Coefficient of Variation: {coefficient_variation:.2f}%")
        print(
            f"  • Distribution Pattern: {distribution_pattern.replace('_', ' ').title()}"
        )
        print(f"  • Evenness Score: {evenness_score:.0f}/100")
        print(f"  • Capacity Utilization: {capacity_utilization:.2f}%")

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

    @task(
        # Define a dataset outlet for the comprehensive summary report
        outlets=[Dataset("summary_report")]
    )
    def generate_summary_report(
        astronaut_list: list[dict],
        weather_data: dict,
        correlation_analysis: dict,
        calculated_stats: dict,
        **context,
    ) -> dict:
        """
        This task generates a comprehensive summary report by consolidating data
        from all major pipeline tasks: astronaut data, weather data, correlation
        analysis, and calculated statistics. It provides an executive summary
        with key findings, metrics, and actionable insights.
        """
        from datetime import datetime

        # Extract execution timestamp
        execution_date = context.get("execution_date", datetime.now())

        # Get astronaut count from XCom
        total_astronauts = context["ti"].xcom_pull(key="number_of_people_in_space")

        # Extract key data from each source
        # From astronaut_list
        spacecraft_names = list(set(person["craft"] for person in astronaut_list))
        astronaut_names = [person["name"] for person in astronaut_list]

        # From weather_data
        temperature = weather_data.get("temperature_celsius", 0)
        pressure = weather_data.get("pressure_hpa", 0)
        weather_location = weather_data.get("location", "N/A")
        weather_timestamp = weather_data.get("timestamp", "N/A")

        # From calculated_stats
        std_deviation = calculated_stats.get("variability_measures", {}).get(
            "standard_deviation", 0
        )
        distribution_pattern = calculated_stats.get("distribution_analysis", {}).get(
            "pattern", "unknown"
        )
        capacity_utilization = calculated_stats.get("capacity_metrics", {}).get(
            "current_utilization_percent", 0
        )
        evenness_score = calculated_stats.get("distribution_analysis", {}).get(
            "distribution_evenness_score", 0
        )

        # From correlation_analysis
        quality_score = correlation_analysis.get("data_quality_score", 0)
        insights = correlation_analysis.get("observational_insights", [])

        # Build comprehensive summary report
        summary_report = {
            "report_metadata": {
                "report_title": "Astronaut Mission Summary Report",
                "generated_at": str(execution_date),
                "report_version": "1.0",
                "data_sources": [
                    "Open Notify Astronaut API",
                    "Open-Meteo Weather API",
                    "Internal Statistical Calculations",
                ],
            },
            "executive_summary": {
                "total_astronauts_in_space": total_astronauts,
                "number_of_spacecraft": len(spacecraft_names),
                "data_quality_score": quality_score,
                "overall_status": "OPERATIONAL",
            },
            "astronaut_data": {
                "spacecraft_list": spacecraft_names,
                "astronaut_count": total_astronauts,
                "crew_members": astronaut_names,
            },
            "environmental_data": {
                "ground_control_location": weather_location,
                "temperature_celsius": temperature,
                "atmospheric_pressure_hpa": pressure,
                "observation_time": weather_timestamp,
            },
            "statistical_analysis": {
                "distribution_pattern": distribution_pattern,
                "standard_deviation": std_deviation,
                "capacity_utilization_percent": capacity_utilization,
                "distribution_evenness_score": evenness_score,
            },
            "key_insights": insights,
            "recommendations": [],
        }

        # Generate recommendations based on analysis
        recommendations = []

        if capacity_utilization < 30:
            recommendations.append(
                "Low capacity utilization detected - consider planning additional missions"
            )
        elif capacity_utilization > 80:
            recommendations.append(
                "High capacity utilization - monitor crew rotation schedules closely"
            )

        if distribution_pattern == "highly_variable":
            recommendations.append(
                "Uneven crew distribution - review mission requirements and resource allocation"
            )

        if evenness_score > 80:
            recommendations.append(
                "Excellent crew distribution balance - maintain current allocation strategy"
            )

        if quality_score < 70:
            recommendations.append(
                "Data quality issues detected - verify data sources and connections"
            )

        if temperature < 5 or temperature > 35:
            recommendations.append(
                f"Extreme ground temperature ({temperature}°C) - monitor mission control environment"
            )

        if not recommendations:
            recommendations.append("All systems nominal - continue standard operations")

        summary_report["recommendations"] = recommendations

        # Generate comprehensive formatted report
        print("\n")
        print("=" * 90)
        print("╔" + "═" * 88 + "╗")
        print("║" + " " * 20 + "ASTRONAUT MISSION SUMMARY REPORT" + " " * 36 + "║")
        print("╚" + "═" * 88 + "╝")
        print("=" * 90)

        print("\n📋 REPORT METADATA:")
        print(f"  • Generated: {execution_date}")
        print(f"  • Version: {summary_report['report_metadata']['report_version']}")
        print(
            f"  • Data Sources: {len(summary_report['report_metadata']['data_sources'])}"
        )

        print("\n" + "=" * 90)
        print("EXECUTIVE SUMMARY")
        print("=" * 90)
        print(f"  🚀 Total Astronauts in Space: {total_astronauts}")
        print(f"  🛸 Active Spacecraft: {len(spacecraft_names)}")
        print(f"  ✅ Data Quality Score: {quality_score}/100")
        print(
            f"  📊 Overall Status: {summary_report['executive_summary']['overall_status']}"
        )

        print("\n" + "=" * 90)
        print("SECTION 1: ASTRONAUT DATA")
        print("=" * 90)
        print(f"  • Total Crew Members: {total_astronauts}")
        print("  • Spacecraft in Operation:")
        for i, craft in enumerate(spacecraft_names, 1):
            print(f"    {i}. {craft}")
        print("\n  • Crew Members:")
        for i, name in enumerate(astronaut_names, 1):
            print(f"    {i}. {name}")

        print("\n" + "=" * 90)
        print("SECTION 2: ENVIRONMENTAL DATA (Ground Control)")
        print("=" * 90)
        print(f"  • Location: {weather_location}")
        print(f"  • Temperature: {temperature}°C")
        print(f"  • Atmospheric Pressure: {pressure} hPa")
        print(f"  • Observation Time: {weather_timestamp}")

        print("\n" + "=" * 90)
        print("SECTION 3: STATISTICAL ANALYSIS")
        print("=" * 90)
        print(
            f"  • Distribution Pattern: {distribution_pattern.replace('_', ' ').title()}"
        )
        print(f"  • Standard Deviation: {std_deviation:.2f}")
        print(f"  • Capacity Utilization: {capacity_utilization:.2f}%")
        print(f"  • Distribution Evenness: {evenness_score:.0f}/100")

        print("\n" + "=" * 90)
        print("SECTION 4: KEY INSIGHTS")
        print("=" * 90)
        if insights:
            for i, insight in enumerate(insights, 1):
                print(f"  {i}. {insight}")
        else:
            print("  • No significant insights detected")

        print("\n" + "=" * 90)
        print("SECTION 5: RECOMMENDATIONS")
        print("=" * 90)
        for i, recommendation in enumerate(recommendations, 1):
            print(f"  {i}. {recommendation}")

        print("\n" + "=" * 90)
        print("END OF REPORT")
        print("=" * 90)
        print("\n")

        return summary_report

    @task(
        # Define a dataset outlet for data quality validation results
        outlets=[Dataset("data_quality_validation")]
    )
    def validate_data_quality(
        astronaut_list: list[dict],
        weather_data: dict,
        calculated_stats: dict,
        correlation_analysis: dict,
        astronaut_stats: dict,
    ) -> dict:
        """
        This task performs comprehensive data quality validation across all
        data sources. It checks for completeness, accuracy, consistency, and
        timeliness of data, providing a detailed validation report with
        pass/fail status for each check.
        """
        from datetime import datetime as dt

        validation_results = {
            "validation_timestamp": str(dt.now()),
            "overall_status": "PASS",
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "warnings": 0,
            "checks": [],
        }

        def add_check(
            category: str,
            check_name: str,
            status: str,
            details: str,
            severity: str = "ERROR",
        ):
            """Helper function to add validation checks"""
            validation_results["checks"].append(
                {
                    "category": category,
                    "check_name": check_name,
                    "status": status,
                    "details": details,
                    "severity": severity,
                }
            )
            validation_results["total_checks"] += 1
            if status == "PASS":
                validation_results["passed_checks"] += 1
            elif status == "FAIL":
                validation_results["failed_checks"] += 1
                if severity == "ERROR":
                    validation_results["overall_status"] = "FAIL"
            elif status == "WARN":
                validation_results["warnings"] += 1

        # ========== ASTRONAUT DATA VALIDATION ==========
        # Check 1: Astronaut list is not empty
        if astronaut_list and len(astronaut_list) > 0:
            add_check(
                "Astronaut Data",
                "Non-empty astronaut list",
                "PASS",
                f"Found {len(astronaut_list)} astronauts",
            )
        else:
            add_check(
                "Astronaut Data",
                "Non-empty astronaut list",
                "FAIL",
                "No astronaut data available",
            )

        # Check 2: All astronauts have required fields
        required_fields = ["name", "craft"]
        all_fields_present = all(
            all(field in person for field in required_fields)
            for person in astronaut_list
        )
        if all_fields_present:
            add_check(
                "Astronaut Data",
                "Required fields present",
                "PASS",
                "All astronauts have name and craft fields",
            )
        else:
            add_check(
                "Astronaut Data",
                "Required fields present",
                "FAIL",
                "Some astronauts missing required fields",
            )

        # Check 3: Validate astronaut count consistency
        total_from_stats = astronaut_stats.get("total_astronauts", 0)
        if len(astronaut_list) == total_from_stats:
            add_check(
                "Astronaut Data",
                "Count consistency",
                "PASS",
                f"Astronaut count matches across sources: {len(astronaut_list)}",
            )
        else:
            add_check(
                "Astronaut Data",
                "Count consistency",
                "WARN",
                f"Count mismatch: list={len(astronaut_list)}, stats={total_from_stats}",
                "WARNING",
            )

        # ========== WEATHER DATA VALIDATION ==========
        # Check 4: Weather data is not empty
        if weather_data:
            add_check(
                "Weather Data", "Data availability", "PASS", "Weather data retrieved"
            )
        else:
            add_check(
                "Weather Data",
                "Data availability",
                "FAIL",
                "Weather data is missing",
            )

        # Check 5: Weather data has no errors
        if not weather_data.get("error"):
            add_check(
                "Weather Data",
                "API error check",
                "PASS",
                "No errors in weather API response",
            )
        else:
            add_check(
                "Weather Data",
                "API error check",
                "WARN",
                f"Weather API error: {weather_data.get('error')}",
                "WARNING",
            )

        # Check 6: Temperature is within reasonable range
        temperature = weather_data.get("temperature_celsius", 0)
        if -50 <= temperature <= 60:
            add_check(
                "Weather Data",
                "Temperature range",
                "PASS",
                f"Temperature within normal range: {temperature}°C",
            )
        else:
            add_check(
                "Weather Data",
                "Temperature range",
                "WARN",
                f"Temperature outside expected range: {temperature}°C",
                "WARNING",
            )

        # Check 7: Pressure is within reasonable range
        pressure = weather_data.get("pressure_hpa", 0)
        if 950 <= pressure <= 1050:
            add_check(
                "Weather Data",
                "Pressure range",
                "PASS",
                f"Pressure within normal range: {pressure} hPa",
            )
        else:
            add_check(
                "Weather Data",
                "Pressure range",
                "WARN",
                f"Pressure outside expected range: {pressure} hPa",
                "WARNING",
            )

        # ========== STATISTICAL DATA VALIDATION ==========
        # Check 8: Calculated statistics are complete
        required_stat_keys = [
            "basic_metrics",
            "central_tendency",
            "variability_measures",
            "distribution_analysis",
        ]
        stats_complete = all(key in calculated_stats for key in required_stat_keys)
        if stats_complete:
            add_check(
                "Statistical Data",
                "Completeness",
                "PASS",
                "All statistical sections present",
            )
        else:
            add_check(
                "Statistical Data",
                "Completeness",
                "FAIL",
                "Some statistical sections missing",
            )

        # Check 9: Variance and standard deviation are non-negative
        variance = calculated_stats.get("variability_measures", {}).get("variance", -1)
        std_dev = calculated_stats.get("variability_measures", {}).get(
            "standard_deviation", -1
        )
        if variance >= 0 and std_dev >= 0:
            add_check(
                "Statistical Data",
                "Valid variance/std dev",
                "PASS",
                f"Variance={variance:.2f}, StdDev={std_dev:.2f}",
            )
        else:
            add_check(
                "Statistical Data",
                "Valid variance/std dev",
                "FAIL",
                "Negative variance or standard deviation detected",
            )

        # Check 10: Capacity utilization is within 0-100%
        capacity = calculated_stats.get("capacity_metrics", {}).get(
            "current_utilization_percent", -1
        )
        if 0 <= capacity <= 100:
            add_check(
                "Statistical Data",
                "Valid capacity utilization",
                "PASS",
                f"Capacity utilization: {capacity:.2f}%",
            )
        else:
            add_check(
                "Statistical Data",
                "Valid capacity utilization",
                "FAIL",
                f"Invalid capacity utilization: {capacity:.2f}%",
            )

        # ========== CORRELATION ANALYSIS VALIDATION ==========
        # Check 11: Correlation analysis has insights
        insights = correlation_analysis.get("observational_insights", [])
        if insights and len(insights) > 0:
            add_check(
                "Correlation Analysis",
                "Insights generated",
                "PASS",
                f"Generated {len(insights)} insights",
            )
        else:
            add_check(
                "Correlation Analysis",
                "Insights generated",
                "WARN",
                "No insights generated",
                "WARNING",
            )

        # Check 12: Data quality score is valid
        quality_score = correlation_analysis.get("data_quality_score", -1)
        if 0 <= quality_score <= 100:
            if quality_score >= 80:
                status = "PASS"
                details = f"Excellent data quality: {quality_score}/100"
            elif quality_score >= 60:
                status = "WARN"
                details = f"Acceptable data quality: {quality_score}/100"
                severity = "WARNING"
            else:
                status = "FAIL"
                details = f"Poor data quality: {quality_score}/100"
                severity = "ERROR"
            add_check(
                "Correlation Analysis",
                "Data quality score",
                status,
                details,
                severity if status != "PASS" else "ERROR",
            )
        else:
            add_check(
                "Correlation Analysis",
                "Data quality score",
                "FAIL",
                f"Invalid quality score: {quality_score}",
            )

        # Calculate pass rate
        pass_rate = (
            (validation_results["passed_checks"] / validation_results["total_checks"])
            * 100
            if validation_results["total_checks"] > 0
            else 0
        )
        validation_results["pass_rate_percent"] = round(pass_rate, 2)

        # Print validation report
        print("\n")
        print("=" * 90)
        print("╔" + "═" * 88 + "╗")
        print("║" + " " * 25 + "DATA QUALITY VALIDATION REPORT" + " " * 33 + "║")
        print("╚" + "═" * 88 + "╝")
        print("=" * 90)

        print("\n🔍 VALIDATION SUMMARY:")
        print(f"  • Overall Status: {validation_results['overall_status']}")
        print(f"  • Total Checks: {validation_results['total_checks']}")
        print(f"  • Passed: {validation_results['passed_checks']} ✅")
        print(f"  • Failed: {validation_results['failed_checks']} ❌")
        print(f"  • Warnings: {validation_results['warnings']} ⚠️")
        print(f"  • Pass Rate: {validation_results['pass_rate_percent']}%")

        print("\n" + "=" * 90)
        print("VALIDATION CHECKS BY CATEGORY")
        print("=" * 90)

        # Group checks by category
        categories = {}
        for check in validation_results["checks"]:
            cat = check["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(check)

        for category, checks in categories.items():
            print(f"\n📋 {category.upper()}")
            print("-" * 90)
            for check in checks:
                status_icon = (
                    "✅"
                    if check["status"] == "PASS"
                    else "❌"
                    if check["status"] == "FAIL"
                    else "⚠️"
                )
                print(f"  {status_icon} {check['check_name']}: {check['status']}")
                print(f"     └─ {check['details']}")

        print("\n" + "=" * 90)
        if validation_results["overall_status"] == "PASS":
            print("✅ ALL CRITICAL VALIDATIONS PASSED - DATA QUALITY CONFIRMED")
        else:
            print("❌ VALIDATION FAILED - REVIEW ERRORS ABOVE")
        print("=" * 90)
        print("\n")

        return validation_results

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

    # Calculate advanced statistics (produces calculated_statistics Dataset)
    calculated_statistics = calculate_astronauts_stats(astronaut_list)

    # Perform advanced statistical methods analysis (produces statistical_methods Dataset)
    statistical_methods_result = perform_statistical_methods(
        astronaut_list, calculated_statistics
    )

    # Perform comprehensive analysis (produces comprehensive_analysis Dataset)
    perform_comprehensive_analysis(
        astronaut_list,
        astronaut_statistics,
        calculated_statistics,
        statistical_methods_result,
    )

    # Fetch weather data independently (produces weather_data Dataset)
    weather_info = get_weather_data()

    # Analyze correlation between astronaut, weather, and calculated statistics data
    # (produces correlation_analysis Dataset)
    correlation_result = analyze_correlation(
        astronaut_statistics, weather_info, calculated_statistics
    )

    # Validate data quality across all sources (produces data_quality_validation Dataset)
    validate_data_quality(
        astronaut_list,
        weather_info,
        calculated_statistics,
        correlation_result,
        astronaut_statistics,
    )

    # Generate comprehensive summary report combining all data sources
    # (produces summary_report Dataset)
    generate_summary_report(
        astronaut_list, weather_info, correlation_result, calculated_statistics
    )

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronaut_list
    )


# Instantiate the DAG
example_astronauts()
