import json
import csv
from collections import Counter, defaultdict

from tqdm import tqdm
import click




def count_city_appearances(medline_file, min_year, max_year):
    cities = []
    reader = csv.reader(medline_file, delimiter='\t')
    next(reader)
    for row in tqdm(reader, total=37396670):
        try:
            float(row[-2])  # only cities with lat,lon
        except ValueError:
            continue
        year = int(row[4])
        if year < min_year or year > max_year:
            continue
        if '|' in row[6]:  # only clear cases
            continue

        cities.append(row[6])
    return Counter(cities)


def cities_with_name(name, counter):
    return {k for k in counter if name in k}


def create_mapping(path_medline, min_year, max_year):
    city_counts = count_city_appearances(path_medline, min_year, max_year)
    aggregate_mapping = {}
    for c_name, size in list(city_counts.most_common()):
        try:
            with_name = cities_with_name(c_name, city_counts)
            for alias in with_name:
                city_counts.pop(alias)
                aggregate_mapping[alias] = c_name
        except KeyError:
            continue
    return aggregate_mapping


def nested_keys(mapping):
    """creates a mapping from long to substring iff they have been merged"""
    cluster_to_city = defaultdict(list)
    for city, cluster in mapping.items():
        cluster_to_city[cluster].append(city)

    ambiguous_clusters = defaultdict(list)
    for cluster1 in tqdm(cluster_to_city):
        for cluster2 in cluster_to_city:
            if cluster1 == cluster2:
                continue
            if cluster1.endswith(cluster2):
                ambiguous_clusters[cluster2].append(cluster1)

    return cluster_to_city, ambiguous_clusters


@click.group()
def cli():
    pass


@cli.command()
@click.argument("medline2016", type=click.File(mode='r', encoding="ISO-8859-2"))
@click.argument("mapping_json", type=click.File(mode='w'))
@click.option("-s", "--start", type=click.INT, default=1990)
@click.option("-e", "--end", type=click.INT, default=2016)
def condense(medline2016, mapping_json, start, end):
    assert end >= start, "start is after end"
    mapping = create_mapping(medline2016, start, end)
    json.dump(mapping, mapping_json)


@cli.command()
@click.argument('city_mapping', type=click.File())
@click.argument('major_action_csv', type=click.File('w'))
def manual_check(city_mapping, major_action_csv):
    mapping = json.load(city_mapping)
    cluster_to_city, ambiguous_clusters = nested_keys(mapping)

    writer = csv.writer(major_action_csv)
    writer.writerow(["cluster_name", "action"])

    for i, (cluster, cities) in enumerate(ambiguous_clusters.items()):
        print(f"cluster {i}/{len(ambiguous_clusters)-1}: {cluster}")
        print(f"aggregated: {cities}")
        action = input("Action? y(es), s(plit), a(mbiguos):")
        writer.writerow([cluster, action])
        major_action_csv.flush()


@cli.command()
@click.argument('mapping_json', type=click.File('r'))
@click.argument('actions_csv', type=click.File('r'))
@click.argument('new_mapping_json', type=click.File('w'))
def correct_merging(mapping_json, actions_csv, new_mapping_json):
    import pandas as pd
    city_to_condensed = json.load(mapping_json)
    cluster_to_city, ambiguous_clusters = nested_keys(city_to_condensed)

    actions_df = pd.read_csv(actions_csv)
    actions_df['to_merge'] = actions_df.action.str.startswith('m')
    actions_df["rev_names"] = actions_df.cluster_name.apply(lambda x: x[::-1])
    to_merge = actions_df[actions_df['to_merge']].sort_values('rev_names')[
                   'cluster_name'].tolist()[::-1]

    ambiguous_clusters_rev = {}
    for main_cluster, sub_clusters in ambiguous_clusters.items():
        for sub_cluster in sub_clusters:
            ambiguous_clusters_rev[sub_cluster] = main_cluster

    new_city_to_condensed = dict()
    for city in city_to_condensed:
        old_condensed_cluster = city_to_condensed[city]

        try:   # is the old condensed cluster one of the potential merger targets?
            ambiguous_main_cluster = ambiguous_clusters_rev[old_condensed_cluster]
        except KeyError:  # the alias was not an ambiguous one
            new_city_to_condensed[city] = old_condensed_cluster
            continue

        if ambiguous_main_cluster in to_merge:
            new_city_to_condensed[city] = ambiguous_main_cluster
        else:
            new_city_to_condensed[city] = old_condensed_cluster

    json.dump(new_city_to_condensed, new_mapping_json)


if __name__ == '__main__':
    cli()
