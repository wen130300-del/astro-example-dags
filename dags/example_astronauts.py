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
        # Define a dataset outlet for trend calculations
        outlets=[Dataset("trend_calculations")]
    )
    def calculate_trends(
        astronaut_list: list[dict],
        calculated_stats: dict,
        statistical_methods: dict,
        **context,
    ) -> dict:
        """
        This task calculates trends, growth rates, and temporal patterns in astronaut data.
        It provides moving averages, rate of change calculations, trend direction analysis,
        and forecasting indicators based on current data patterns.
        """
        from datetime import datetime as dt

        # Extract execution context
        execution_date = context.get("execution_date", dt.now())
        total_astronauts = context["ti"].xcom_pull(key="number_of_people_in_space")

        # Collect spacecraft data
        spacecraft_counts = {}
        for person in astronaut_list:
            craft = person["craft"]
            spacecraft_counts[craft] = spacecraft_counts.get(craft, 0) + 1

        # Extract statistical data
        mean = calculated_stats.get("central_tendency", {}).get(
            "mean_astronauts_per_craft", 0
        )
        std_dev = calculated_stats.get("variability_measures", {}).get(
            "standard_deviation", 0
        )
        capacity_utilization = calculated_stats.get("capacity_metrics", {}).get(
            "current_utilization_percent", 0
        )

        # Extract quartile data
        q1 = statistical_methods.get("quartile_analysis", {}).get("q1_25th", 0)
        q2 = statistical_methods.get("quartile_analysis", {}).get("q2_median", 0)
        q3 = statistical_methods.get("quartile_analysis", {}).get("q3_75th", 0)

        # ========== TREND BASELINE CALCULATIONS ==========
        # Simulate historical baseline for trend analysis
        # In production, this would query historical data from a database

        # Calculate baseline (simulated as 80% of current for demonstration)
        baseline_astronauts = int(total_astronauts * 0.8)
        baseline_mean = mean * 0.85
        baseline_capacity = capacity_utilization * 0.9

        # ========== GROWTH RATE CALCULATIONS ==========
        # Calculate growth rates (current vs baseline)
        astronaut_growth_rate = (
            ((total_astronauts - baseline_astronauts) / baseline_astronauts * 100)
            if baseline_astronauts > 0
            else 0
        )

        mean_growth_rate = (
            ((mean - baseline_mean) / baseline_mean * 100) if baseline_mean > 0 else 0
        )

        capacity_growth_rate = (
            ((capacity_utilization - baseline_capacity) / baseline_capacity * 100)
            if baseline_capacity > 0
            else 0
        )

        # ========== RATE OF CHANGE (VELOCITY) ==========
        # Calculate velocity metrics
        astronaut_velocity = total_astronauts - baseline_astronauts
        mean_velocity = mean - baseline_mean
        capacity_velocity = capacity_utilization - baseline_capacity

        # ========== MOVING AVERAGE CALCULATIONS ==========
        # Simulate moving averages (in production, use historical data)
        crew_sizes = list(spacecraft_counts.values())

        # Simple moving average (current snapshot)
        sma_crew_size = sum(crew_sizes) / len(crew_sizes) if crew_sizes else 0

        # Exponential moving average (weighted recent data)
        # EMA = Price × (Smoothing / (1 + Days)) + EMA_yesterday × (1 - (Smoothing / (1 + Days)))
        smoothing = 2
        periods = 5
        ema_multiplier = smoothing / (1 + periods)
        ema_crew_size = sma_crew_size * ema_multiplier + baseline_mean * (
            1 - ema_multiplier
        )

        # ========== TREND DIRECTION ANALYSIS ==========
        trend_direction = "neutral"
        trend_strength = "weak"

        if astronaut_growth_rate > 10:
            trend_direction = "strongly_increasing"
            trend_strength = "strong"
        elif astronaut_growth_rate > 5:
            trend_direction = "increasing"
            trend_strength = "moderate"
        elif astronaut_growth_rate > 1:
            trend_direction = "slightly_increasing"
            trend_strength = "weak"
        elif astronaut_growth_rate < -10:
            trend_direction = "strongly_decreasing"
            trend_strength = "strong"
        elif astronaut_growth_rate < -5:
            trend_direction = "decreasing"
            trend_strength = "moderate"
        elif astronaut_growth_rate < -1:
            trend_direction = "slightly_decreasing"
            trend_strength = "weak"

        # ========== MOMENTUM INDICATORS ==========
        # Calculate momentum score (0-100)
        momentum_score = 50  # Neutral baseline

        # Adjust based on growth rates
        momentum_score += astronaut_growth_rate * 2
        momentum_score += capacity_growth_rate * 0.5

        # Clamp to 0-100
        momentum_score = max(0, min(100, momentum_score))

        # Momentum classification
        if momentum_score > 70:
            momentum_classification = "bullish"
        elif momentum_score > 55:
            momentum_classification = "slightly_bullish"
        elif momentum_score < 30:
            momentum_classification = "bearish"
        elif momentum_score < 45:
            momentum_classification = "slightly_bearish"
        else:
            momentum_classification = "neutral"

        # ========== VOLATILITY ANALYSIS ==========
        # Calculate coefficient of variation as volatility measure
        volatility = (std_dev / mean * 100) if mean > 0 else 0

        volatility_level = "low"
        if volatility > 50:
            volatility_level = "high"
        elif volatility > 30:
            volatility_level = "medium"

        # ========== FORECASTING INDICATORS ==========
        # Simple linear projection
        projected_astronauts = total_astronauts + (astronaut_velocity * 0.5)
        projected_capacity = capacity_utilization + (capacity_growth_rate * 0.3)

        # Forecast confidence based on volatility
        if volatility_level == "low":
            forecast_confidence = "high"
        elif volatility_level == "medium":
            forecast_confidence = "medium"
        else:
            forecast_confidence = "low"

        # ========== TREND PATTERNS ==========
        patterns = []

        # Detect growth pattern
        if astronaut_growth_rate > 5 and capacity_growth_rate > 5:
            patterns.append(
                {
                    "pattern": "expansion_phase",
                    "description": "Both crew count and capacity utilization are increasing",
                    "implication": "System is in growth mode",
                }
            )

        # Detect consolidation pattern
        if abs(astronaut_growth_rate) < 3 and volatility_level == "low":
            patterns.append(
                {
                    "pattern": "consolidation",
                    "description": "Stable crew levels with low volatility",
                    "implication": "System is in steady state",
                }
            )

        # Detect divergence pattern
        if (
            astronaut_growth_rate > 5
            and capacity_growth_rate < -5
            or astronaut_growth_rate < -5
            and capacity_growth_rate > 5
        ):
            patterns.append(
                {
                    "pattern": "divergence",
                    "description": "Crew count and capacity moving in opposite directions",
                    "implication": "Potential efficiency imbalance",
                }
            )

        # Detect saturation pattern
        if capacity_utilization > 80 and capacity_growth_rate < 2:
            patterns.append(
                {
                    "pattern": "saturation",
                    "description": "High capacity utilization with slow growth",
                    "implication": "Approaching operational limits",
                }
            )

        # ========== COMPARATIVE METRICS ==========
        # Compare current vs quartiles
        quartile_position = "median"
        if mean > q3:
            quartile_position = "above_q3"
        elif mean > q2:
            quartile_position = "between_q2_q3"
        elif mean > q1:
            quartile_position = "between_q1_q2"
        else:
            quartile_position = "below_q1"

        # ========== COMPILE TREND CALCULATIONS ==========
        trend_calculations = {
            "calculation_metadata": {
                "calculation_date": str(execution_date),
                "baseline_period": "simulated",
                "current_astronauts": total_astronauts,
            },
            "growth_metrics": {
                "astronaut_growth_rate_percent": round(astronaut_growth_rate, 2),
                "mean_growth_rate_percent": round(mean_growth_rate, 2),
                "capacity_growth_rate_percent": round(capacity_growth_rate, 2),
            },
            "velocity_metrics": {
                "astronaut_velocity": round(astronaut_velocity, 2),
                "mean_velocity": round(mean_velocity, 2),
                "capacity_velocity": round(capacity_velocity, 2),
            },
            "moving_averages": {
                "simple_moving_average": round(sma_crew_size, 2),
                "exponential_moving_average": round(ema_crew_size, 2),
                "difference_sma_ema": round(sma_crew_size - ema_crew_size, 2),
            },
            "trend_analysis": {
                "direction": trend_direction,
                "strength": trend_strength,
                "momentum_score": round(momentum_score, 2),
                "momentum_classification": momentum_classification,
            },
            "volatility_analysis": {
                "volatility_percent": round(volatility, 2),
                "volatility_level": volatility_level,
            },
            "forecast_indicators": {
                "projected_astronauts": round(projected_astronauts, 2),
                "projected_capacity_percent": round(projected_capacity, 2),
                "forecast_confidence": forecast_confidence,
            },
            "patterns_detected": patterns,
            "comparative_metrics": {
                "quartile_position": quartile_position,
                "vs_baseline_astronauts": total_astronauts - baseline_astronauts,
                "vs_baseline_capacity": round(
                    capacity_utilization - baseline_capacity, 2
                ),
            },
        }

        # ========== PRINT TREND CALCULATIONS REPORT ==========
        print("\n")
        print("=" * 90)
        print("╔" + "═" * 88 + "╗")
        print("║" + " " * 30 + "TREND CALCULATIONS REPORT" + " " * 33 + "║")
        print("╚" + "═" * 88 + "╝")
        print("=" * 90)

        print("\n📅 CALCULATION METADATA:")
        print(f"  • Calculation Date: {execution_date}")
        print(f"  • Current Astronauts: {total_astronauts}")
        print(f"  • Baseline Astronauts: {baseline_astronauts}")

        print("\n" + "=" * 90)
        print("SECTION 1: GROWTH METRICS")
        print("=" * 90)
        growth_icon = (
            "📈"
            if astronaut_growth_rate > 0
            else "📉"
            if astronaut_growth_rate < 0
            else "➡️"
        )
        print(f"  {growth_icon} Astronaut Growth Rate: {astronaut_growth_rate:+.2f}%")
        print(f"  {growth_icon} Mean Growth Rate: {mean_growth_rate:+.2f}%")
        print(f"  {growth_icon} Capacity Growth Rate: {capacity_growth_rate:+.2f}%")

        print("\n" + "=" * 90)
        print("SECTION 2: VELOCITY METRICS (Rate of Change)")
        print("=" * 90)
        print(f"  • Astronaut Velocity: {astronaut_velocity:+.2f} astronauts/period")
        print(f"  • Mean Velocity: {mean_velocity:+.2f} per spacecraft/period")
        print(f"  • Capacity Velocity: {capacity_velocity:+.2f}%/period")

        print("\n" + "=" * 90)
        print("SECTION 3: MOVING AVERAGES")
        print("=" * 90)
        print(f"  • Simple Moving Average (SMA): {sma_crew_size:.2f}")
        print(f"  • Exponential Moving Average (EMA): {ema_crew_size:.2f}")
        print(f"  • SMA - EMA Difference: {sma_crew_size - ema_crew_size:+.2f}")
        if sma_crew_size > ema_crew_size:
            print("    → Short-term average above long-term (bullish signal)")
        elif sma_crew_size < ema_crew_size:
            print("    → Short-term average below long-term (bearish signal)")
        else:
            print("    → Averages aligned (neutral)")

        print("\n" + "=" * 90)
        print("SECTION 4: TREND ANALYSIS")
        print("=" * 90)
        print(f"  • Trend Direction: {trend_direction.replace('_', ' ').upper()}")
        print(f"  • Trend Strength: {trend_strength.upper()}")
        momentum_icon = (
            "🚀"
            if momentum_classification == "bullish"
            else "📊"
            if momentum_classification in ["slightly_bullish", "neutral"]
            else "⚠️"
        )
        print(f"  {momentum_icon} Momentum Score: {momentum_score:.2f}/100")
        print(
            f"  • Momentum Classification: {momentum_classification.replace('_', ' ').upper()}"
        )

        print("\n" + "=" * 90)
        print("SECTION 5: VOLATILITY ANALYSIS")
        print("=" * 90)
        vol_icon = (
            "🟢"
            if volatility_level == "low"
            else "🟡"
            if volatility_level == "medium"
            else "🔴"
        )
        print(f"  {vol_icon} Volatility: {volatility:.2f}%")
        print(f"  • Volatility Level: {volatility_level.upper()}")

        print("\n" + "=" * 90)
        print("SECTION 6: FORECAST INDICATORS")
        print("=" * 90)
        print(f"  🔮 Projected Astronauts: {projected_astronauts:.0f}")
        print(f"  🔮 Projected Capacity: {projected_capacity:.2f}%")
        print(f"  • Forecast Confidence: {forecast_confidence.upper()}")

        print("\n" + "=" * 90)
        print("SECTION 7: PATTERNS DETECTED")
        print("=" * 90)
        if patterns:
            for i, pattern in enumerate(patterns, 1):
                print(
                    f"\n  📊 Pattern {i}: {pattern['pattern'].replace('_', ' ').upper()}"
                )
                print(f"     Description: {pattern['description']}")
                print(f"     Implication: {pattern['implication']}")
        else:
            print("  • No significant patterns detected")

        print("\n" + "=" * 90)
        print("SECTION 8: COMPARATIVE METRICS")
        print("=" * 90)
        print(f"  • Quartile Position: {quartile_position.replace('_', ' ').upper()}")
        print(
            f"  • vs Baseline (Astronauts): {total_astronauts - baseline_astronauts:+d}"
        )
        print(
            f"  • vs Baseline (Capacity): {capacity_utilization - baseline_capacity:+.2f}%"
        )

        print("\n" + "=" * 90)
        print("END OF TREND CALCULATIONS")
        print("=" * 90)
        print("\n")

        return trend_calculations

    @task(
        # Define a dataset outlet for insights report
        outlets=[Dataset("insights_report")]
    )
    def generate_insights_report(
        comprehensive_analysis: dict,
        trend_calculations: dict,
        validation_results: dict,
        correlation_analysis: dict,
        **context,
    ) -> dict:
        """
        This task generates an executive insights report synthesizing all analysis
        outputs into actionable business intelligence. It provides key findings,
        critical alerts, opportunity identification, and decision support recommendations.
        """
        from datetime import datetime as dt

        execution_date = context.get("execution_date", dt.now())
        total_astronauts = context["ti"].xcom_pull(key="number_of_people_in_space")

        # Extract key metrics from all sources
        risk_level = comprehensive_analysis.get("risk_assessment", {}).get(
            "overall_risk_level", "UNKNOWN"
        )
        efficiency_score = comprehensive_analysis.get("efficiency_metrics", {}).get(
            "overall_efficiency_score", 0
        )
        anomalies = comprehensive_analysis.get("anomaly_detection", {}).get(
            "anomalies", []
        )

        momentum = trend_calculations.get("trend_analysis", {}).get(
            "momentum_classification", "neutral"
        )
        growth_rate = trend_calculations.get("growth_metrics", {}).get(
            "astronaut_growth_rate_percent", 0
        )
        volatility = trend_calculations.get("volatility_analysis", {}).get(
            "volatility_level", "unknown"
        )

        data_quality = validation_results.get("pass_rate_percent", 0)
        validation_results.get("overall_status", "UNKNOWN")

        len(correlation_analysis.get("observational_insights", []))

        # ========== EXECUTIVE SUMMARY ==========
        executive_summary = {
            "overall_health_score": 0,
            "status": "UNKNOWN",
            "critical_flags": [],
            "key_highlights": [],
        }

        # Calculate overall health score (0-100)
        health_score = 0
        health_score += (
            100 - (risk_level == "HIGH") * 30 - (risk_level == "MEDIUM") * 15
        )
        health_score += efficiency_score * 0.3
        health_score += data_quality * 0.2
        health_score += (50 + growth_rate * 2) * 0.2
        health_score = max(0, min(100, health_score))

        executive_summary["overall_health_score"] = round(health_score, 2)

        # Determine overall status
        if health_score >= 80:
            executive_summary["status"] = "EXCELLENT"
        elif health_score >= 65:
            executive_summary["status"] = "GOOD"
        elif health_score >= 50:
            executive_summary["status"] = "FAIR"
        else:
            executive_summary["status"] = "NEEDS_ATTENTION"

        # Critical flags
        if risk_level == "HIGH":
            executive_summary["critical_flags"].append("High operational risk detected")
        if efficiency_score < 60:
            executive_summary["critical_flags"].append("Low efficiency score")
        if data_quality < 70:
            executive_summary["critical_flags"].append("Data quality concerns")
        if len(anomalies) > 2:
            executive_summary["critical_flags"].append(
                f"{len(anomalies)} anomalies detected"
            )

        # Key highlights
        if efficiency_score >= 80:
            executive_summary["key_highlights"].append(
                "Excellent operational efficiency"
            )
        if momentum == "bullish":
            executive_summary["key_highlights"].append("Strong positive momentum")
        if data_quality >= 90:
            executive_summary["key_highlights"].append("High data quality confidence")
        if risk_level == "LOW":
            executive_summary["key_highlights"].append("Low risk profile")

        # ========== KEY FINDINGS ==========
        key_findings = []

        # Finding 1: Risk assessment
        key_findings.append(
            {
                "category": "Risk Management",
                "finding": f"Overall risk level is {risk_level}",
                "impact": "high"
                if risk_level == "HIGH"
                else "medium"
                if risk_level == "MEDIUM"
                else "low",
                "actionable": risk_level in ["HIGH", "MEDIUM"],
            }
        )

        # Finding 2: Efficiency
        key_findings.append(
            {
                "category": "Operational Efficiency",
                "finding": f"Efficiency score at {efficiency_score:.1f}%",
                "impact": "high"
                if efficiency_score < 60
                else "medium"
                if efficiency_score < 80
                else "low",
                "actionable": efficiency_score < 80,
            }
        )

        # Finding 3: Growth trajectory
        key_findings.append(
            {
                "category": "Growth Trajectory",
                "finding": f"Growth rate at {growth_rate:+.1f}% with {momentum} momentum",
                "impact": "high" if abs(growth_rate) > 10 else "medium",
                "actionable": abs(growth_rate) > 15
                or momentum in ["bearish", "bullish"],
            }
        )

        # Finding 4: Data quality
        key_findings.append(
            {
                "category": "Data Quality",
                "finding": f"Data validation pass rate: {data_quality:.1f}%",
                "impact": "high" if data_quality < 70 else "low",
                "actionable": data_quality < 85,
            }
        )

        # ========== CRITICAL ALERTS ==========
        critical_alerts = []

        # Alert 1: High risk
        if risk_level == "HIGH":
            critical_alerts.append(
                {
                    "severity": "CRITICAL",
                    "alert": "High operational risk requires immediate attention",
                    "recommended_action": "Review and address risk factors within 24 hours",
                    "priority": 1,
                }
            )

        # Alert 2: Anomalies
        if len(anomalies) >= 3:
            critical_alerts.append(
                {
                    "severity": "HIGH",
                    "alert": f"{len(anomalies)} anomalies detected in the system",
                    "recommended_action": "Investigate anomalies and determine root causes",
                    "priority": 2,
                }
            )

        # Alert 3: Low efficiency
        if efficiency_score < 50:
            critical_alerts.append(
                {
                    "severity": "HIGH",
                    "alert": "Critically low efficiency score",
                    "recommended_action": "Initiate efficiency improvement program immediately",
                    "priority": 1,
                }
            )

        # Alert 4: Data quality issues
        if data_quality < 70:
            critical_alerts.append(
                {
                    "severity": "MEDIUM",
                    "alert": "Data quality below acceptable threshold",
                    "recommended_action": "Audit data sources and validation processes",
                    "priority": 3,
                }
            )

        # Alert 5: High volatility
        if volatility == "high":
            critical_alerts.append(
                {
                    "severity": "MEDIUM",
                    "alert": "High volatility indicates unstable operations",
                    "recommended_action": "Implement stabilization measures",
                    "priority": 2,
                }
            )

        # Sort alerts by priority
        critical_alerts.sort(key=lambda x: x["priority"])

        # ========== OPPORTUNITIES ==========
        opportunities = []

        # Opportunity 1: Growth potential
        if growth_rate > 5 and efficiency_score > 70:
            opportunities.append(
                {
                    "opportunity": "Expansion Opportunity",
                    "description": "Strong growth with good efficiency enables scaling",
                    "potential_value": "High",
                    "timeframe": "Near-term",
                    "effort": "Medium",
                }
            )

        # Opportunity 2: Efficiency optimization
        if efficiency_score >= 60 and efficiency_score < 85:
            opportunities.append(
                {
                    "opportunity": "Efficiency Optimization",
                    "description": f"Potential to increase efficiency from {efficiency_score:.0f}% to 85%+",
                    "potential_value": "Medium",
                    "timeframe": "Medium-term",
                    "effort": "Medium",
                }
            )

        # Opportunity 3: Risk reduction
        if risk_level == "MEDIUM":
            opportunities.append(
                {
                    "opportunity": "Risk Reduction Initiative",
                    "description": "Moderate risk level can be lowered with targeted actions",
                    "potential_value": "Medium",
                    "timeframe": "Short-term",
                    "effort": "Low",
                }
            )

        # Opportunity 4: Data excellence
        if data_quality >= 85 and data_quality < 95:
            opportunities.append(
                {
                    "opportunity": "Data Excellence Program",
                    "description": "Achieve data quality excellence (95%+) for better insights",
                    "potential_value": "Low",
                    "timeframe": "Long-term",
                    "effort": "Low",
                }
            )

        # ========== DECISION SUPPORT RECOMMENDATIONS ==========
        decisions = []

        # Decision 1: Immediate actions
        immediate_actions = []
        if critical_alerts:
            immediate_actions.append("Address critical alerts based on priority")
        if risk_level == "HIGH":
            immediate_actions.append("Implement risk mitigation strategies")
        if efficiency_score < 60:
            immediate_actions.append("Launch efficiency recovery program")

        if immediate_actions:
            decisions.append(
                {
                    "timeframe": "IMMEDIATE (0-7 days)",
                    "decision_type": "Tactical",
                    "actions": immediate_actions,
                    "expected_impact": "Stabilize operations and address critical issues",
                }
            )

        # Decision 2: Short-term actions
        short_term_actions = []
        if efficiency_score < 80:
            short_term_actions.append("Optimize resource allocation")
        if len(anomalies) > 0:
            short_term_actions.append("Investigate and resolve anomalies")
        if volatility in ["medium", "high"]:
            short_term_actions.append("Reduce operational volatility")

        if short_term_actions:
            decisions.append(
                {
                    "timeframe": "SHORT-TERM (1-4 weeks)",
                    "decision_type": "Operational",
                    "actions": short_term_actions,
                    "expected_impact": "Improve operational metrics and stability",
                }
            )

        # Decision 3: Strategic actions
        strategic_actions = []
        if growth_rate > 5:
            strategic_actions.append("Develop expansion plan to capitalize on growth")
        if opportunities:
            strategic_actions.append("Evaluate and prioritize identified opportunities")
        if efficiency_score >= 80:
            strategic_actions.append("Maintain excellence and explore innovation")

        if strategic_actions:
            decisions.append(
                {
                    "timeframe": "STRATEGIC (1-6 months)",
                    "decision_type": "Strategic",
                    "actions": strategic_actions,
                    "expected_impact": "Position for long-term success and growth",
                }
            )

        # ========== COMPILE INSIGHTS REPORT ==========
        insights_report = {
            "report_metadata": {
                "report_date": str(execution_date),
                "astronaut_count": total_astronauts,
                "data_sources_integrated": 4,
            },
            "executive_summary": executive_summary,
            "key_findings": key_findings,
            "critical_alerts": critical_alerts,
            "opportunities": opportunities,
            "decision_support": decisions,
            "performance_indicators": {
                "health_score": round(health_score, 2),
                "risk_level": risk_level,
                "efficiency_score": round(efficiency_score, 2),
                "data_quality": round(data_quality, 2),
                "growth_rate": round(growth_rate, 2),
                "momentum": momentum,
            },
        }

        # ========== PRINT INSIGHTS REPORT ==========
        print("\n")
        print("=" * 95)
        print("╔" + "═" * 93 + "╗")
        print("║" + " " * 30 + "EXECUTIVE INSIGHTS REPORT" + " " * 38 + "║")
        print("╚" + "═" * 93 + "╝")
        print("=" * 95)

        print("\n📅 REPORT METADATA:")
        print(f"  • Report Date: {execution_date}")
        print(f"  • Astronaut Count: {total_astronauts}")
        print(
            f"  • Data Sources: {insights_report['report_metadata']['data_sources_integrated']}"
        )

        print("\n" + "=" * 95)
        print("EXECUTIVE SUMMARY")
        print("=" * 95)
        status_icon = (
            "🟢"
            if executive_summary["status"] == "EXCELLENT"
            else "🟡"
            if executive_summary["status"] == "GOOD"
            else "🟠"
            if executive_summary["status"] == "FAIR"
            else "🔴"
        )
        print(f"  {status_icon} Overall Status: {executive_summary['status']}")
        print(f"  • Health Score: {executive_summary['overall_health_score']:.1f}/100")

        if executive_summary["critical_flags"]:
            print("\n  🚨 CRITICAL FLAGS:")
            for flag in executive_summary["critical_flags"]:
                print(f"    ⚠️  {flag}")

        if executive_summary["key_highlights"]:
            print("\n  ✨ KEY HIGHLIGHTS:")
            for highlight in executive_summary["key_highlights"]:
                print(f"    ✓ {highlight}")

        print("\n" + "=" * 95)
        print("KEY FINDINGS")
        print("=" * 95)
        for i, finding in enumerate(key_findings, 1):
            impact_icon = (
                "🔴"
                if finding["impact"] == "high"
                else "🟡"
                if finding["impact"] == "medium"
                else "🟢"
            )
            action_flag = " [ACTION REQUIRED]" if finding["actionable"] else ""
            print(f"\n  {i}. {finding['category']}{action_flag}")
            print(f"     {impact_icon} {finding['finding']}")
            print(f"     Impact: {finding['impact'].upper()}")

        if critical_alerts:
            print("\n" + "=" * 95)
            print("CRITICAL ALERTS")
            print("=" * 95)
            for i, alert in enumerate(critical_alerts, 1):
                severity_icon = (
                    "🔴"
                    if alert["severity"] == "CRITICAL"
                    else "🟡"
                    if alert["severity"] == "HIGH"
                    else "🟠"
                )
                print(f"\n  {severity_icon} ALERT {i} ({alert['severity']})")
                print(f"     Alert: {alert['alert']}")
                print(f"     Action: {alert['recommended_action']}")
                print(f"     Priority: {alert['priority']}")

        if opportunities:
            print("\n" + "=" * 95)
            print("OPPORTUNITIES")
            print("=" * 95)
            for i, opp in enumerate(opportunities, 1):
                value_icon = (
                    "💎"
                    if opp["potential_value"] == "High"
                    else "💰"
                    if opp["potential_value"] == "Medium"
                    else "📊"
                )
                print(f"\n  {value_icon} OPPORTUNITY {i}: {opp['opportunity']}")
                print(f"     Description: {opp['description']}")
                print(
                    f"     Value: {opp['potential_value']} | Timeframe: {opp['timeframe']} | Effort: {opp['effort']}"
                )

        print("\n" + "=" * 95)
        print("DECISION SUPPORT RECOMMENDATIONS")
        print("=" * 95)
        for decision in decisions:
            timeframe_icon = (
                "⚡"
                if "IMMEDIATE" in decision["timeframe"]
                else "📅"
                if "SHORT" in decision["timeframe"]
                else "🎯"
            )
            print(f"\n  {timeframe_icon} {decision['timeframe']}")
            print(f"     Type: {decision['decision_type']}")
            print("     Actions:")
            for action in decision["actions"]:
                print(f"       • {action}")
            print(f"     Expected Impact: {decision['expected_impact']}")

        print("\n" + "=" * 95)
        print("PERFORMANCE INDICATORS SUMMARY")
        print("=" * 95)
        print(f"  • Health Score: {health_score:.1f}/100")
        print(f"  • Risk Level: {risk_level}")
        print(f"  • Efficiency: {efficiency_score:.1f}%")
        print(f"  • Data Quality: {data_quality:.1f}%")
        print(f"  • Growth Rate: {growth_rate:+.1f}%")
        print(f"  • Momentum: {momentum.replace('_', ' ').title()}")

        print("\n" + "=" * 95)
        print("END OF EXECUTIVE INSIGHTS REPORT")
        print("=" * 95)
        print("\n")

        return insights_report

    @task(
        # Define a dataset outlet for performance dashboard
        outlets=[Dataset("performance_dashboard")]
    )
    def generate_performance_dashboard(
        astronaut_stats: dict,
        calculated_stats: dict,
        trend_calculations: dict,
        insights_report: dict,
        **context,
    ) -> dict:
        """
        This task generates a performance dashboard with visual KPIs, trend indicators,
        and real-time metrics display. Provides at-a-glance operational status with
        color-coded indicators and simplified metrics for monitoring.
        """
        from datetime import datetime as dt

        execution_date = context.get("execution_date", dt.now())
        total_astronauts = context["ti"].xcom_pull(key="number_of_people_in_space")

        # Extract key metrics
        spacecraft_count = astronaut_stats.get("unique_spacecraft_count", 0)
        avg_per_craft = calculated_stats.get("central_tendency", {}).get(
            "mean_astronauts_per_craft", 0
        )
        capacity_utilization = calculated_stats.get("capacity_metrics", {}).get(
            "current_utilization_percent", 0
        )
        distribution_pattern = calculated_stats.get("distribution_analysis", {}).get(
            "pattern", "unknown"
        )

        growth_rate = trend_calculations.get("growth_metrics", {}).get(
            "astronaut_growth_rate_percent", 0
        )
        momentum = trend_calculations.get("trend_analysis", {}).get(
            "momentum_classification", "neutral"
        )
        volatility = trend_calculations.get("volatility_analysis", {}).get(
            "volatility_level", "unknown"
        )

        health_score = insights_report.get("performance_indicators", {}).get(
            "health_score", 0
        )
        status = insights_report.get("executive_summary", {}).get("status", "UNKNOWN")

        # ========== KPI CALCULATIONS ==========
        kpis = {
            "astronaut_count": {
                "value": total_astronauts,
                "label": "Total Astronauts",
                "unit": "crew",
                "trend": "up"
                if growth_rate > 0
                else "down"
                if growth_rate < 0
                else "stable",
                "status": "good" if total_astronauts >= 5 else "warning",
            },
            "spacecraft_count": {
                "value": spacecraft_count,
                "label": "Active Spacecraft",
                "unit": "craft",
                "trend": "stable",
                "status": "good" if spacecraft_count >= 2 else "warning",
            },
            "capacity_utilization": {
                "value": round(capacity_utilization, 1),
                "label": "Capacity Utilization",
                "unit": "%",
                "trend": "up" if growth_rate > 0 else "stable",
                "status": "good"
                if 40 <= capacity_utilization <= 80
                else "warning"
                if capacity_utilization < 90
                else "critical",
            },
            "health_score": {
                "value": round(health_score, 1),
                "label": "System Health",
                "unit": "/100",
                "trend": "stable",
                "status": "good"
                if health_score >= 80
                else "warning"
                if health_score >= 60
                else "critical",
            },
            "avg_crew_per_craft": {
                "value": round(avg_per_craft, 1),
                "label": "Avg Crew/Craft",
                "unit": "crew",
                "trend": "stable",
                "status": "good",
            },
        }

        # ========== TREND INDICATORS ==========
        trend_indicators = {
            "growth_momentum": {
                "label": "Growth Momentum",
                "value": momentum.replace("_", " ").title(),
                "indicator": "🚀"
                if momentum == "bullish"
                else "⚠️"
                if momentum == "bearish"
                else "📊",
                "color": "green"
                if momentum in ["bullish", "slightly_bullish"]
                else "red"
                if momentum in ["bearish", "slightly_bearish"]
                else "yellow",
            },
            "growth_rate": {
                "label": "Growth Rate",
                "value": f"{growth_rate:+.1f}%",
                "indicator": "📈"
                if growth_rate > 0
                else "📉"
                if growth_rate < 0
                else "➡️",
                "color": "green"
                if growth_rate > 5
                else "red"
                if growth_rate < -5
                else "yellow",
            },
            "volatility": {
                "label": "Volatility",
                "value": volatility.title(),
                "indicator": "🟢"
                if volatility == "low"
                else "🟡"
                if volatility == "medium"
                else "🔴",
                "color": "green"
                if volatility == "low"
                else "yellow"
                if volatility == "medium"
                else "red",
            },
            "distribution": {
                "label": "Distribution Pattern",
                "value": distribution_pattern.replace("_", " ").title(),
                "indicator": "✅" if distribution_pattern == "uniform" else "⚠️",
                "color": "green" if distribution_pattern == "uniform" else "yellow",
            },
        }

        # ========== STATUS INDICATORS ==========
        status_indicators = {
            "overall_status": {
                "label": "Overall Status",
                "value": status,
                "indicator": "🟢"
                if status == "EXCELLENT"
                else "🟡"
                if status == "GOOD"
                else "🟠"
                if status == "FAIR"
                else "🔴",
                "description": "System operational health",
            },
            "operational_efficiency": {
                "label": "Operational Efficiency",
                "value": f"{capacity_utilization:.0f}%",
                "indicator": "🟢"
                if 40 <= capacity_utilization <= 80
                else "🟡"
                if capacity_utilization < 90
                else "🔴",
                "description": "Resource utilization level",
            },
            "crew_allocation": {
                "label": "Crew Allocation",
                "value": distribution_pattern.replace("_", " ").title(),
                "indicator": "🟢" if distribution_pattern == "uniform" else "🟡",
                "description": "Distribution across spacecraft",
            },
        }

        # ========== ALERTS AND NOTIFICATIONS ==========
        alerts = []
        critical_alerts = insights_report.get("critical_alerts", [])
        if critical_alerts:
            for alert in critical_alerts[:3]:  # Top 3 alerts
                alerts.append(
                    {
                        "severity": alert.get("severity", "INFO"),
                        "message": alert.get("alert", ""),
                        "priority": alert.get("priority", 5),
                    }
                )

        # ========== QUICK STATS ==========
        quick_stats = {
            "timestamp": str(execution_date),
            "astronauts_in_space": total_astronauts,
            "spacecraft_active": spacecraft_count,
            "capacity_used": f"{capacity_utilization:.1f}%",
            "health_score": f"{health_score:.1f}/100",
            "growth_rate": f"{growth_rate:+.1f}%",
            "status": status,
        }

        # ========== COMPILE DASHBOARD ==========
        dashboard = {
            "dashboard_metadata": {
                "generated_at": str(execution_date),
                "dashboard_version": "1.0",
                "refresh_interval": "daily",
            },
            "kpis": kpis,
            "trend_indicators": trend_indicators,
            "status_indicators": status_indicators,
            "alerts": alerts,
            "quick_stats": quick_stats,
        }

        # ========== PRINT PERFORMANCE DASHBOARD ==========
        print("\n")
        print("╔" + "═" * 98 + "╗")
        print("║" + " " * 35 + "PERFORMANCE DASHBOARD" + " " * 42 + "║")
        print("╠" + "═" * 98 + "╣")
        print("║" + f" Generated: {execution_date}".ljust(98) + "║")
        print("╚" + "═" * 98 + "╝")

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " KEY PERFORMANCE INDICATORS (KPIs)".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        # Display KPIs in a grid-like format
        kpi_list = list(kpis.items())
        for i in range(0, len(kpi_list), 2):
            left_key, left_kpi = kpi_list[i]

            # Left KPI
            status_icon = (
                "🟢"
                if left_kpi["status"] == "good"
                else "🟡"
                if left_kpi["status"] == "warning"
                else "🔴"
            )
            trend_icon = (
                "↗"
                if left_kpi["trend"] == "up"
                else "↘"
                if left_kpi["trend"] == "down"
                else "→"
            )
            left_text = f"  {status_icon} {left_kpi['label']}: {left_kpi['value']}{left_kpi['unit']} {trend_icon}"

            # Right KPI (if exists)
            if i + 1 < len(kpi_list):
                right_key, right_kpi = kpi_list[i + 1]
                r_status_icon = (
                    "🟢"
                    if right_kpi["status"] == "good"
                    else "🟡"
                    if right_kpi["status"] == "warning"
                    else "🔴"
                )
                r_trend_icon = (
                    "↗"
                    if right_kpi["trend"] == "up"
                    else "↘"
                    if right_kpi["trend"] == "down"
                    else "→"
                )
                right_text = f"{r_status_icon} {right_kpi['label']}: {right_kpi['value']}{right_kpi['unit']} {r_trend_icon}"
                print(left_text.ljust(50) + right_text)
            else:
                print(left_text)

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " TREND INDICATORS".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        for _key, indicator in trend_indicators.items():
            print(
                f"  {indicator['indicator']} {indicator['label']}: {indicator['value']}"
            )

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " STATUS INDICATORS".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        for _key, indicator in status_indicators.items():
            print(
                f"  {indicator['indicator']} {indicator['label']}: {indicator['value']}"
            )
            print(f"     └─ {indicator['description']}")

        if alerts:
            print("\n" + "┌" + "─" * 98 + "┐")
            print("│" + " ACTIVE ALERTS".center(98) + "│")
            print("└" + "─" * 98 + "┘")
            for i, alert in enumerate(alerts, 1):
                severity_icon = (
                    "🔴"
                    if alert["severity"] == "CRITICAL"
                    else "🟡"
                    if alert["severity"] == "HIGH"
                    else "🟠"
                )
                print(
                    f"  {severity_icon} [{alert['severity']}] {alert['message']} (Priority: {alert['priority']})"
                )

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " QUICK STATS SUMMARY".center(98) + "│")
        print("└" + "─" * 98 + "┘")
        print(f"  📊 Astronauts in Space: {quick_stats['astronauts_in_space']}")
        print(f"  🛸 Active Spacecraft: {quick_stats['spacecraft_active']}")
        print(f"  📈 Capacity Used: {quick_stats['capacity_used']}")
        print(f"  💚 Health Score: {quick_stats['health_score']}")
        print(f"  📊 Growth Rate: {quick_stats['growth_rate']}")
        print(f"  ⭐ Status: {quick_stats['status']}")

        print("\n" + "╔" + "═" * 98 + "╗")
        print("║" + " END OF DASHBOARD".center(98) + "║")
        print("╚" + "═" * 98 + "╝")
        print("\n")

        return dashboard

    @task(
        # Define a dataset outlet for regression model predictions
        outlets=[Dataset("regression_predictions")]
    )
    def build_regression_model(
        astronaut_list: list[dict],
        calculated_stats: dict,
        trend_calculations: dict,
        weather_data: dict,
        **context,
    ) -> dict:
        """
        This task builds a simple linear regression model to predict astronaut counts
        based on historical patterns and weather correlations. Uses least squares method
        to fit a trend line and generates predictions.
        """
        # Import datetime for timestamp handling

        execution_date = context.get("ds", "N/A")

        # ========== DATA PREPARATION ==========
        # Extract features for regression
        total_astronauts = calculated_stats.get("total_astronauts", 0)
        calculated_stats.get("spacecraft_count", 0)
        calculated_stats.get("mean_astronauts_per_craft", 0)
        capacity_utilization = calculated_stats.get("capacity_utilization_rate", 0)

        # Weather features (normalized)
        temperature = weather_data.get("temperature_celsius", 20)
        humidity = weather_data.get("humidity_percent", 50)
        pressure = weather_data.get("pressure_hpa", 1013)

        # Trend features
        growth_rate = trend_calculations.get("growth_rate", {}).get("rate_of_change", 0)
        momentum = trend_calculations.get("momentum_indicators", {}).get(
            "momentum_score", 50
        )

        print("\n")
        print("╔" + "═" * 98 + "╗")
        print("║" + " " * 34 + "REGRESSION MODEL BUILDER" + " " * 39 + "║")
        print("╠" + "═" * 98 + "╣")
        print("║" + f" Analysis Date: {execution_date}".ljust(98) + "║")
        print("╚" + "═" * 98 + "╝")

        # ========== SIMPLE LINEAR REGRESSION ==========
        # Create synthetic historical data points based on current metrics and trends
        # Simulating the last 10 time periods
        historical_periods = 10

        # Generate historical data points using current trends
        x_values = list(range(1, historical_periods + 1))  # Time periods
        y_values = []  # Astronaut counts

        # Simulate historical values with a trend
        base_count = max(1, total_astronauts - historical_periods // 2)
        trend_step = growth_rate / 100 if growth_rate != 0 else 0.1

        for i in range(historical_periods):
            # Add some variance based on weather and momentum
            weather_factor = (temperature - 20) / 100  # Normalize around 20°C
            momentum_factor = (momentum - 50) / 100  # Normalize around 50

            historical_value = (
                base_count + (i * trend_step) + weather_factor + momentum_factor
            )
            y_values.append(max(0, historical_value))

        # Calculate regression coefficients using least squares method
        n = len(x_values)
        sum_x = sum(x_values)
        sum_y = sum(y_values)
        sum_xy = sum(x * y for x, y in zip(x_values, y_values, strict=False))
        sum_x_squared = sum(x * x for x in x_values)

        # Calculate slope (m) and intercept (b) for y = mx + b
        if n * sum_x_squared - sum_x * sum_x != 0:
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x)
            intercept = (sum_y - slope * sum_x) / n
        else:
            slope = 0
            intercept = sum_y / n if n > 0 else 0

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " REGRESSION MODEL COEFFICIENTS".center(98) + "│")
        print("└" + "─" * 98 + "┘")
        print(f"  📐 Model Equation: y = {slope:.4f}x + {intercept:.4f}")
        print(f"  📊 Slope (m): {slope:.4f}")
        print(f"  📍 Intercept (b): {intercept:.4f}")
        print(f"  📈 Historical Data Points: {n}")

        # ========== CALCULATE R-SQUARED (COEFFICIENT OF DETERMINATION) ==========
        # Calculate predicted values
        y_predicted = [slope * x + intercept for x in x_values]

        # Calculate mean of actual values
        y_mean = sum_y / n

        # Calculate total sum of squares (SS_tot)
        ss_tot = sum((y - y_mean) ** 2 for y in y_values)

        # Calculate residual sum of squares (SS_res)
        ss_res = sum(
            (y - y_pred) ** 2 for y, y_pred in zip(y_values, y_predicted, strict=False)
        )

        # Calculate R-squared
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        # Calculate correlation coefficient
        correlation = r_squared**0.5 if r_squared >= 0 else -((-r_squared) ** 0.5)

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " MODEL ACCURACY METRICS".center(98) + "│")
        print("└" + "─" * 98 + "┘")
        print(f"  🎯 R-Squared (R²): {r_squared:.4f}")
        print(f"  🔗 Correlation Coefficient (r): {correlation:.4f}")
        print(
            f"  📊 Model Fit: {'Excellent' if r_squared > 0.9 else 'Good' if r_squared > 0.7 else 'Moderate' if r_squared > 0.5 else 'Poor'}"
        )

        # ========== GENERATE PREDICTIONS ==========
        # Predict for next 5 time periods
        future_periods = [historical_periods + i for i in range(1, 6)]
        predictions = []

        for period in future_periods:
            predicted_value = slope * period + intercept

            # Apply weather and momentum adjustments
            weather_adjustment = (temperature - 20) / 50
            momentum_adjustment = (momentum - 50) / 100
            adjusted_prediction = (
                predicted_value + weather_adjustment + momentum_adjustment
            )

            # Calculate confidence interval (simple approximation)
            # Using standard error estimation
            residuals = [
                y - y_pred for y, y_pred in zip(y_values, y_predicted, strict=False)
            ]
            standard_error = (
                (sum(r**2 for r in residuals) / (n - 2)) ** 0.5 if n > 2 else 1
            )

            confidence_margin = 1.96 * standard_error  # 95% confidence interval

            predictions.append(
                {
                    "period": period,
                    "predicted_astronauts": max(0, adjusted_prediction),
                    "lower_bound": max(0, adjusted_prediction - confidence_margin),
                    "upper_bound": adjusted_prediction + confidence_margin,
                    "confidence_interval": "95%",
                }
            )

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " FUTURE PREDICTIONS (Next 5 Periods)".center(98) + "│")
        print("└" + "─" + "─" * 96 + "┘")
        print(
            "  Period | Predicted Astronauts | Lower Bound (95% CI) | Upper Bound (95% CI)"
        )
        print("  " + "─" * 76)

        for pred in predictions:
            print(
                f"    {pred['period']:3d}  |        {pred['predicted_astronauts']:6.2f}        |        {pred['lower_bound']:6.2f}         |        {pred['upper_bound']:6.2f}"
            )

        # ========== FEATURE IMPORTANCE ANALYSIS ==========
        # Calculate normalized feature contributions
        features = {
            "trend_growth": abs(growth_rate) / 100,
            "momentum": abs(momentum - 50) / 100,
            "temperature": abs(temperature - 20) / 50,
            "humidity": abs(humidity - 50) / 100,
            "pressure": abs(pressure - 1013) / 100,
            "capacity_utilization": capacity_utilization / 100,
        }

        # Normalize feature importance
        total_importance = sum(features.values())
        feature_importance = (
            {k: (v / total_importance) * 100 for k, v in features.items()}
            if total_importance > 0
            else {k: 0 for k in features}
        )

        # Sort by importance
        sorted_features = sorted(
            feature_importance.items(), key=lambda x: x[1], reverse=True
        )

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " FEATURE IMPORTANCE ANALYSIS".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        for feature_name, importance in sorted_features:
            bar_length = int(importance / 2)  # Scale to max 50 chars
            bar = "█" * bar_length
            print(
                f"  {feature_name.replace('_', ' ').title():25s} │ {bar} {importance:5.2f}%"
            )

        # ========== MODEL INSIGHTS ==========
        # Determine trend direction
        if slope > 0.1:
            trend_direction = "Increasing"
            trend_icon = "📈"
        elif slope < -0.1:
            trend_direction = "Decreasing"
            trend_icon = "📉"
        else:
            trend_direction = "Stable"
            trend_icon = "📊"

        # Generate insights
        insights = []

        if r_squared > 0.8:
            insights.append("✅ Model shows strong predictive accuracy (R² > 0.8)")
        elif r_squared > 0.5:
            insights.append("⚠️  Model shows moderate predictive accuracy (R² > 0.5)")
        else:
            insights.append(
                "❌ Model shows weak predictive accuracy (R² < 0.5) - high variance expected"
            )

        if abs(slope) > 0.5:
            insights.append(
                f"{trend_icon} Strong {trend_direction.lower()} trend detected (slope: {slope:.3f})"
            )
        else:
            insights.append(
                f"{trend_icon} Weak or stable trend detected (slope: {slope:.3f})"
            )

        # Weather correlation insight
        top_feature = sorted_features[0]
        insights.append(
            f"🔍 Top influencing factor: {top_feature[0].replace('_', ' ').title()} ({top_feature[1]:.1f}% importance)"
        )

        # Prediction confidence
        avg_prediction = sum(p["predicted_astronauts"] for p in predictions) / len(
            predictions
        )
        if avg_prediction > total_astronauts * 1.2:
            insights.append(
                "⚠️  Model predicts significant growth in astronaut population"
            )
        elif avg_prediction < total_astronauts * 0.8:
            insights.append(
                "⚠️  Model predicts significant decline in astronaut population"
            )
        else:
            insights.append(
                "✅ Model predicts stable astronaut population with minor fluctuations"
            )

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " MODEL INSIGHTS & RECOMMENDATIONS".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        for i, insight in enumerate(insights, 1):
            print(f"  {i}. {insight}")

        # ========== COMPILE RESULTS ==========
        regression_results = {
            "model_metadata": {
                "generated_at": execution_date,
                "model_type": "Linear Regression (Least Squares)",
                "training_periods": historical_periods,
                "prediction_periods": len(predictions),
            },
            "model_coefficients": {
                "slope": slope,
                "intercept": intercept,
                "equation": f"y = {slope:.4f}x + {intercept:.4f}",
            },
            "accuracy_metrics": {
                "r_squared": r_squared,
                "correlation_coefficient": correlation,
                "standard_error": standard_error if n > 2 else None,
                "model_fit_quality": (
                    "Excellent"
                    if r_squared > 0.9
                    else "Good"
                    if r_squared > 0.7
                    else "Moderate"
                    if r_squared > 0.5
                    else "Poor"
                ),
            },
            "historical_data": {
                "x_values": x_values,
                "y_values": y_values,
                "y_predicted": y_predicted,
            },
            "predictions": predictions,
            "feature_importance": dict(sorted_features),
            "insights": insights,
            "trend_analysis": {
                "direction": trend_direction,
                "slope_magnitude": abs(slope),
                "trend_strength": (
                    "Strong"
                    if abs(slope) > 0.5
                    else "Moderate"
                    if abs(slope) > 0.2
                    else "Weak"
                ),
            },
        }

        print("\n" + "╔" + "═" * 98 + "╗")
        print("║" + " REGRESSION MODEL COMPLETED SUCCESSFULLY".center(98) + "║")
        print("╚" + "═" * 98 + "╝")
        print("\n")

        return regression_results

    @task(
        # Define a dataset outlet for model comparison results
        outlets=[Dataset("model_comparison")]
    )
    def compare_regression_models(
        astronaut_list: list[dict],
        calculated_stats: dict,
        trend_calculations: dict,
        weather_data: dict,
        linear_regression_result: dict,
        **context,
    ) -> dict:
        """
        This task compares multiple regression models (Linear, Polynomial degree 2, Polynomial degree 3)
        and evaluates their performance using R², RMSE, MAE, and AIC metrics to determine the best model.
        """
        import math

        execution_date = context.get("ds", "N/A")

        # ========== DATA PREPARATION ==========
        total_astronauts = calculated_stats.get("total_astronauts", 0)
        growth_rate = trend_calculations.get("growth_rate", {}).get("rate_of_change", 0)
        momentum = trend_calculations.get("momentum_indicators", {}).get(
            "momentum_score", 50
        )
        temperature = weather_data.get("temperature_celsius", 20)

        print("\n")
        print("╔" + "═" * 98 + "╗")
        print("║" + " " * 32 + "MODEL COMPARISON FRAMEWORK" + " " * 39 + "║")
        print("╠" + "═" * 98 + "╣")
        print("║" + f" Analysis Date: {execution_date}".ljust(98) + "║")
        print("╚" + "═" * 98 + "╝")

        # Generate synthetic historical data (same as in regression model)
        historical_periods = 10
        x_values = list(range(1, historical_periods + 1))
        y_values = []

        base_count = max(1, total_astronauts - historical_periods // 2)
        trend_step = growth_rate / 100 if growth_rate != 0 else 0.1

        for i in range(historical_periods):
            weather_factor = (temperature - 20) / 100
            momentum_factor = (momentum - 50) / 100
            historical_value = (
                base_count + (i * trend_step) + weather_factor + momentum_factor
            )
            y_values.append(max(0, historical_value))

        n = len(x_values)
        y_mean = sum(y_values) / n

        # ========== HELPER FUNCTIONS ==========
        def calculate_metrics(y_actual, y_predicted, num_parameters):
            """Calculate performance metrics for a model"""
            # R-squared
            ss_tot = sum((y - y_mean) ** 2 for y in y_actual)
            ss_res = sum(
                (y - y_pred) ** 2
                for y, y_pred in zip(y_actual, y_predicted, strict=False)
            )
            r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

            # RMSE (Root Mean Squared Error)
            mse = (
                sum(
                    (y - y_pred) ** 2
                    for y, y_pred in zip(y_actual, y_predicted, strict=False)
                )
                / n
            )
            rmse = math.sqrt(mse)

            # MAE (Mean Absolute Error)
            mae = (
                sum(
                    abs(y - y_pred)
                    for y, y_pred in zip(y_actual, y_predicted, strict=False)
                )
                / n
            )

            # AIC (Akaike Information Criterion) - lower is better
            # AIC = n * ln(MSE) + 2 * k, where k is number of parameters
            aic = n * math.log(mse) + 2 * num_parameters if mse > 0 else float("inf")

            # Adjusted R-squared
            adj_r_squared = (
                1 - ((1 - r_squared) * (n - 1) / (n - num_parameters - 1))
                if n > num_parameters + 1
                else r_squared
            )

            return {
                "r_squared": r_squared,
                "adj_r_squared": adj_r_squared,
                "rmse": rmse,
                "mae": mae,
                "aic": aic,
                "mse": mse,
            }

        def fit_polynomial(x, y, degree):
            """Fit polynomial regression using normal equations"""
            # Create design matrix X
            X = [[x_i**j for j in range(degree + 1)] for x_i in x]

            # Calculate X^T * X
            XtX = [
                [
                    sum(X[i][k] * X[i][j] for i in range(len(X)))
                    for j in range(degree + 1)
                ]
                for k in range(degree + 1)
            ]

            # Calculate X^T * y
            Xty = [
                sum(X[i][j] * y[i] for i in range(len(X))) for j in range(degree + 1)
            ]

            # Solve using Gaussian elimination
            coefficients = gaussian_elimination(XtX, Xty)

            return coefficients

        def gaussian_elimination(A, b):
            """Solve system of linear equations Ax = b using Gaussian elimination"""
            n = len(b)
            # Create augmented matrix
            aug = [A[i] + [b[i]] for i in range(n)]

            # Forward elimination
            for i in range(n):
                # Find pivot
                max_row = i
                for k in range(i + 1, n):
                    if abs(aug[k][i]) > abs(aug[max_row][i]):
                        max_row = k
                aug[i], aug[max_row] = aug[max_row], aug[i]

                # Make all rows below this one 0 in current column
                for k in range(i + 1, n):
                    if aug[i][i] != 0:
                        factor = aug[k][i] / aug[i][i]
                        for j in range(i, n + 1):
                            aug[k][j] -= factor * aug[i][j]

            # Back substitution
            x = [0] * n
            for i in range(n - 1, -1, -1):
                if aug[i][i] != 0:
                    x[i] = aug[i][n]
                    for j in range(i + 1, n):
                        x[i] -= aug[i][j] * x[j]
                    x[i] /= aug[i][i]

            return x

        def predict_polynomial(coefficients, x):
            """Predict y value using polynomial coefficients"""
            return sum(coef * (x**i) for i, coef in enumerate(coefficients))

        # ========== MODEL 1: LINEAR REGRESSION ==========
        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " MODEL 1: LINEAR REGRESSION".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        # Extract from existing linear regression result
        linear_slope = linear_regression_result["model_coefficients"]["slope"]
        linear_intercept = linear_regression_result["model_coefficients"]["intercept"]

        # Calculate predictions
        linear_predictions = [linear_slope * x + linear_intercept for x in x_values]
        linear_metrics = calculate_metrics(
            y_values, linear_predictions, 2
        )  # 2 parameters: slope, intercept

        print(f"  📐 Equation: y = {linear_slope:.4f}x + {linear_intercept:.4f}")
        print(f"  🎯 R²: {linear_metrics['r_squared']:.4f}")
        print(f"  📊 Adjusted R²: {linear_metrics['adj_r_squared']:.4f}")
        print(f"  📏 RMSE: {linear_metrics['rmse']:.4f}")
        print(f"  📐 MAE: {linear_metrics['mae']:.4f}")
        print(f"  🔢 AIC: {linear_metrics['aic']:.2f}")

        # ========== MODEL 2: POLYNOMIAL REGRESSION (DEGREE 2) ==========
        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " MODEL 2: POLYNOMIAL REGRESSION (Degree 2)".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        poly2_coefficients = fit_polynomial(x_values, y_values, 2)
        poly2_predictions = [
            predict_polynomial(poly2_coefficients, x) for x in x_values
        ]
        poly2_metrics = calculate_metrics(
            y_values, poly2_predictions, 3
        )  # 3 parameters

        print(
            f"  📐 Equation: y = {poly2_coefficients[2]:.4f}x² + {poly2_coefficients[1]:.4f}x + {poly2_coefficients[0]:.4f}"
        )
        print(f"  🎯 R²: {poly2_metrics['r_squared']:.4f}")
        print(f"  📊 Adjusted R²: {poly2_metrics['adj_r_squared']:.4f}")
        print(f"  📏 RMSE: {poly2_metrics['rmse']:.4f}")
        print(f"  📐 MAE: {poly2_metrics['mae']:.4f}")
        print(f"  🔢 AIC: {poly2_metrics['aic']:.2f}")

        # ========== MODEL 3: POLYNOMIAL REGRESSION (DEGREE 3) ==========
        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " MODEL 3: POLYNOMIAL REGRESSION (Degree 3)".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        poly3_coefficients = fit_polynomial(x_values, y_values, 3)
        poly3_predictions = [
            predict_polynomial(poly3_coefficients, x) for x in x_values
        ]
        poly3_metrics = calculate_metrics(
            y_values, poly3_predictions, 4
        )  # 4 parameters

        print(
            f"  📐 Equation: y = {poly3_coefficients[3]:.4f}x³ + {poly3_coefficients[2]:.4f}x² + {poly3_coefficients[1]:.4f}x + {poly3_coefficients[0]:.4f}"
        )
        print(f"  🎯 R²: {poly3_metrics['r_squared']:.4f}")
        print(f"  📊 Adjusted R²: {poly3_metrics['adj_r_squared']:.4f}")
        print(f"  📏 RMSE: {poly3_metrics['rmse']:.4f}")
        print(f"  📐 MAE: {poly3_metrics['mae']:.4f}")
        print(f"  🔢 AIC: {poly3_metrics['aic']:.2f}")

        # ========== MODEL COMPARISON TABLE ==========
        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " PERFORMANCE COMPARISON TABLE".center(98) + "│")
        print("└" + "─" * 98 + "┘")
        print(
            "  Model                    │    R²    │  Adj R²  │   RMSE   │   MAE    │    AIC    "
        )
        print("  " + "─" * 88)
        print(
            f"  Linear Regression        │  {linear_metrics['r_squared']:.4f}  │  {linear_metrics['adj_r_squared']:.4f}  │  {linear_metrics['rmse']:.4f}  │  {linear_metrics['mae']:.4f}  │  {linear_metrics['aic']:8.2f}"
        )
        print(
            f"  Polynomial (Degree 2)    │  {poly2_metrics['r_squared']:.4f}  │  {poly2_metrics['adj_r_squared']:.4f}  │  {poly2_metrics['rmse']:.4f}  │  {poly2_metrics['mae']:.4f}  │  {poly2_metrics['aic']:8.2f}"
        )
        print(
            f"  Polynomial (Degree 3)    │  {poly3_metrics['r_squared']:.4f}  │  {poly3_metrics['adj_r_squared']:.4f}  │  {poly3_metrics['rmse']:.4f}  │  {poly3_metrics['mae']:.4f}  │  {poly3_metrics['aic']:8.2f}"
        )

        # ========== SELECT BEST MODEL ==========
        models = [
            {
                "name": "Linear Regression",
                "metrics": linear_metrics,
                "type": "linear",
                "coefficients": [linear_intercept, linear_slope],
            },
            {
                "name": "Polynomial (Degree 2)",
                "metrics": poly2_metrics,
                "type": "polynomial_2",
                "coefficients": poly2_coefficients,
            },
            {
                "name": "Polynomial (Degree 3)",
                "metrics": poly3_metrics,
                "type": "polynomial_3",
                "coefficients": poly3_coefficients,
            },
        ]

        # Ranking system: score each model on different metrics
        def rank_models(models):
            scores = []
            for model in models:
                m = model["metrics"]
                # Higher R² is better (weight: 40%)
                r2_score = m["adj_r_squared"] * 40
                # Lower RMSE is better (weight: 30%)
                rmse_scores = [mod["metrics"]["rmse"] for mod in models]
                min_rmse = min(rmse_scores)
                rmse_score = (
                    (1 - (m["rmse"] - min_rmse) / max(rmse_scores)) * 30
                    if max(rmse_scores) > 0
                    else 30
                )
                # Lower AIC is better (weight: 30%)
                aic_scores = [
                    mod["metrics"]["aic"]
                    for mod in models
                    if mod["metrics"]["aic"] != float("inf")
                ]
                if aic_scores and m["aic"] != float("inf"):
                    min_aic = min(aic_scores)
                    max_aic = max(aic_scores)
                    aic_score = (
                        (1 - (m["aic"] - min_aic) / (max_aic - min_aic + 0.0001)) * 30
                        if max_aic > min_aic
                        else 30
                    )
                else:
                    aic_score = 0

                total_score = r2_score + rmse_score + aic_score
                scores.append(total_score)

            return scores

        scores = rank_models(models)
        best_model_idx = scores.index(max(scores))
        best_model = models[best_model_idx]

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " MODEL RANKING (Composite Score)".center(98) + "│")
        print("└" + "─" * 98 + "┘")

        ranked_models = sorted(
            zip(models, scores, strict=False), key=lambda x: x[1], reverse=True
        )
        for i, (model, score) in enumerate(ranked_models, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉"
            print(f"  {medal} Rank {i}: {model['name']:30s} │ Score: {score:6.2f}/100")

        # ========== BEST MODEL INSIGHTS ==========
        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " BEST MODEL SELECTION".center(98) + "│")
        print("└" + "─" * 98 + "┘")
        print(f"  🏆 Winner: {best_model['name']}")
        print(f"  ✅ R² Score: {best_model['metrics']['r_squared']:.4f}")
        print(f"  ✅ RMSE: {best_model['metrics']['rmse']:.4f}")
        print(f"  ✅ Composite Score: {scores[best_model_idx]:.2f}/100")

        # Generate recommendations
        recommendations = []

        if best_model["type"] == "linear":
            recommendations.append(
                "Linear model is sufficient - data shows a clear linear trend"
            )
            recommendations.append("Simple interpretation and low risk of overfitting")
        elif best_model["type"] == "polynomial_2":
            recommendations.append(
                "Quadratic relationship detected - trend shows acceleration/deceleration"
            )
            recommendations.append(
                "Moderate complexity with improved fit over linear model"
            )
        else:
            recommendations.append(
                "Cubic relationship detected - trend shows complex curvature"
            )
            recommendations.append(
                "⚠️  Higher complexity - monitor for overfitting on new data"
            )

        if best_model["metrics"]["r_squared"] > 0.9:
            recommendations.append(
                "Excellent model fit - predictions should be highly reliable"
            )
        elif best_model["metrics"]["r_squared"] > 0.7:
            recommendations.append(
                "Good model fit - predictions are reasonably reliable"
            )
        else:
            recommendations.append("⚠️  Moderate fit - use predictions with caution")

        print("\n" + "┌" + "─" * 98 + "┐")
        print("│" + " RECOMMENDATIONS".center(98) + "│")
        print("└" + "─" * 98 + "┘")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")

        # ========== COMPILE RESULTS ==========
        comparison_results = {
            "comparison_metadata": {
                "generated_at": execution_date,
                "models_evaluated": len(models),
                "evaluation_metrics": ["R²", "Adjusted R²", "RMSE", "MAE", "AIC"],
            },
            "models": {
                "linear": {
                    "name": "Linear Regression",
                    "metrics": linear_metrics,
                    "coefficients": [linear_intercept, linear_slope],
                    "equation": f"y = {linear_slope:.4f}x + {linear_intercept:.4f}",
                    "rank": next(
                        i
                        for i, (m, _) in enumerate(ranked_models, 1)
                        if m["type"] == "linear"
                    ),
                    "score": scores[0],
                },
                "polynomial_2": {
                    "name": "Polynomial (Degree 2)",
                    "metrics": poly2_metrics,
                    "coefficients": poly2_coefficients,
                    "equation": f"y = {poly2_coefficients[2]:.4f}x² + {poly2_coefficients[1]:.4f}x + {poly2_coefficients[0]:.4f}",
                    "rank": next(
                        i
                        for i, (m, _) in enumerate(ranked_models, 1)
                        if m["type"] == "polynomial_2"
                    ),
                    "score": scores[1],
                },
                "polynomial_3": {
                    "name": "Polynomial (Degree 3)",
                    "metrics": poly3_metrics,
                    "coefficients": poly3_coefficients,
                    "equation": f"y = {poly3_coefficients[3]:.4f}x³ + {poly3_coefficients[2]:.4f}x² + {poly3_coefficients[1]:.4f}x + {poly3_coefficients[0]:.4f}",
                    "rank": next(
                        i
                        for i, (m, _) in enumerate(ranked_models, 1)
                        if m["type"] == "polynomial_3"
                    ),
                    "score": scores[2],
                },
            },
            "best_model": {
                "name": best_model["name"],
                "type": best_model["type"],
                "metrics": best_model["metrics"],
                "coefficients": best_model["coefficients"],
                "score": scores[best_model_idx],
            },
            "recommendations": recommendations,
        }

        print("\n" + "╔" + "═" * 98 + "╗")
        print("║" + " MODEL COMPARISON COMPLETED SUCCESSFULLY".center(98) + "║")
        print("╚" + "═" * 98 + "╝")
        print("\n")

        return comparison_results

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
    comprehensive_analysis_result = perform_comprehensive_analysis(
        astronaut_list,
        astronaut_statistics,
        calculated_statistics,
        statistical_methods_result,
    )

    # Calculate trends and growth rates (produces trend_calculations Dataset)
    trend_calculations_result = calculate_trends(
        astronaut_list, calculated_statistics, statistical_methods_result
    )

    # Fetch weather data independently (produces weather_data Dataset)
    weather_info = get_weather_data()

    # Build regression model for predictions (produces regression_predictions Dataset)
    regression_result = build_regression_model(
        astronaut_list,
        calculated_statistics,
        trend_calculations_result,
        weather_info,
    )

    # Compare multiple regression models and select best (produces model_comparison Dataset)
    compare_regression_models(
        astronaut_list,
        calculated_statistics,
        trend_calculations_result,
        weather_info,
        regression_result,
    )

    # Analyze correlation between astronaut, weather, and calculated statistics data
    # (produces correlation_analysis Dataset)
    correlation_result = analyze_correlation(
        astronaut_statistics, weather_info, calculated_statistics
    )

    # Validate data quality across all sources (produces data_quality_validation Dataset)
    validation_result = validate_data_quality(
        astronaut_list,
        weather_info,
        calculated_statistics,
        correlation_result,
        astronaut_statistics,
    )

    # Generate executive insights report synthesizing all analysis
    # (produces insights_report Dataset)
    insights_report_result = generate_insights_report(
        comprehensive_analysis_result,
        trend_calculations_result,
        validation_result,
        correlation_result,
    )

    # Generate comprehensive summary report combining all data sources
    # (produces summary_report Dataset)
    generate_summary_report(
        astronaut_list, weather_info, correlation_result, calculated_statistics
    )

    # Generate performance dashboard with visual KPIs and metrics
    # (produces performance_dashboard Dataset)
    generate_performance_dashboard(
        astronaut_statistics,
        calculated_statistics,
        trend_calculations_result,
        insights_report_result,
    )

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronaut_list
    )


# Instantiate the DAG
example_astronauts()
