import click

from src.utils import load_json, dump_pickle
from src.data import load_city_triplets
from src.data import count_motives
from src.measures import connection_probabilities


@click.command()
@click.option("-s", "--start", type=click.INT, help="start of period")
@click.option("-e", "--end", type=click.INT, help="end of period")
def main(start, end):
    city_to_condensed = load_json("data/raw/city_to_condensed_alt.json")
    pmid_to_location = load_city_triplets(
        "data/raw/mapaffil2016.tsv", start, end, verbose=True
    )

    triplets, pairs = count_motives(pmid_to_location, city_to_condensed, verbose=True)
    probs = connection_probabilities(triplets, pairs, verbose=True)

    dump_pickle(probs, f"data/interim/triple_probs_{start}_{end}.p")


if __name__ == "__main__":
    main()
