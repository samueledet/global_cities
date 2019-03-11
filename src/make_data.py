import click
import json
import pickle

from src.data import load_city_triplets
from src.data import count_motives
from src.measures import connection_probabilities


@click.command()
@click.argument("medline2016", type=click.File(mode='r', encoding="ISO-8859-2"))
@click.argument('city_to_condensed_json', type=click.File())
@click.argument('triple_probs_pickle', type=click.File('wb'))
@click.option("-s", "--start", type=click.INT, help="start of period")
@click.option("-e", "--end", type=click.INT, help="end of period")
def main(medline2016, city_to_condensed_json, triple_probs_pickle, start, end):
    city_to_condensed = json.load(city_to_condensed_json)
    pmid_to_location = load_city_triplets(medline2016, start, end, verbose=True)

    triplets, pairs = count_motives(pmid_to_location, city_to_condensed, verbose=True)
    probs = connection_probabilities(triplets, pairs, verbose=True)

    pickle.dump(probs, triple_probs_pickle)


if __name__ == "__main__":
    main()
