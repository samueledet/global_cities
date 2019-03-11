"""All functions used in the computation of the triplet probabilities of the cities
are included in this file

"""
import operator
import subprocess as sp
from collections import Counter, defaultdict
from tqdm import tqdm

from src.utils import create_pbar, load_pickle
from src.data import load_city_triplets




def similarity(file1):
    return file1


def prob_triplet(city_a, city_b, focal_city, triplet_counts, pair_counts):
    """Probability that two cities (a, b) are connected given the presence of a
    third city (c)

    Args:
        city_a
        city_b
        focal_city
        triplet_counts
        pair_counts
    """
    triplet_key = tuple(sorted([city_a, city_b, focal_city]))
    pair_key = tuple(sorted([city_a, city_b]))
    try:
        return triplet_counts[triplet_key] / pair_counts[pair_key]
    except ZeroDivisionError:
        return 0


def connection_probabilities(triplets, pairs, verbose=False):
    """computes all probabilities of the form P(a,b|c)

    Args:
        triplets:
        pairs:
    """
    prob_connections_if_c = defaultdict(dict)

    progress = create_pbar(verbose)
    for triplet in progress(triplets):
        city_a, city_b, city_c = triplet

        trip_prob_c = prob_triplet(city_a, city_b, city_c, triplets, pairs)
        prob_connections_if_c[(city_a, city_b)][city_c] = trip_prob_c

        trip_prob_b = prob_triplet(city_a, city_c, city_b, triplets, pairs)
        prob_connections_if_c[(city_a, city_c)][city_b] = trip_prob_b

        trip_prob_a = prob_triplet(city_b, city_c, city_a, triplets, pairs)
        prob_connections_if_c[(city_b, city_c)][city_a] = trip_prob_a

    return dict(prob_connections_if_c)


def maxprob_rank(start,end):
    """Compute 1. the pair of cities (connected to a focal city) with maximum
    probabilities

    Args:
        allprob:
        cities_code:
    """
    allprob = load_pickle("/storage/samuel.edet/PubMed/city_triplet/data/interim/triple_probs_%s_%s.p" %(start,end))
    maxprob_connection = {
        keys: max(allprob[keys].items(), key=operator.itemgetter(1))
        for keys in allprob.keys()
    }

    rank_globalcities = Counter(elem[0] for elem in maxprob_connection.values())
    #rank_globalcities = {cities_code[a]: b for a, b in rank_globalcities.items()}
    rank_globalcities = sorted(
        rank_globalcities.items(), key=lambda kv: kv[1], reverse=True
    )

    return maxprob_connection, rank_globalcities


def focal_pairs(focal_cities, maxprob_connection, cities_code):
    """
    Args:
        focal_cities
        maxprob_connection
        cities_code
    Return: dictionary with focal cities as keys and any 4 pair of
        cities (max prob) they are connected to alongside the probability of
        connection.
    """
    cities_code_invert = {v: k for k, v in cities_code.items()}

    focalids = [cities_code[x] for x in focal_cities]

    focal_pair = {}
    for focalid in focalids:

        cities_id_focal = [
            (keys, maxprob_connection[keys][1])
            for keys in maxprob_connection.keys()
            if maxprob_connection[keys][0] == focalid
        ][:3]

        cities_name_focal = [
            (cities_code_invert[pair[0][0]], cities_code_invert[pair[0][1]], pair[1])
            for pair in cities_id_focal
        ]

        focal_pair[cities_code_invert[focalid]] = cities_name_focal

    return focal_pair



def focal_split(maxprob_ranking, maxprob_connections):
    """
    split the number of ties where a focal city is dominant in three groups: 
    a) both cities are in the same country of the focal one; 
    b) one city is in the same country of the focal but the other is not; 
    c) none of the cities is in the same country of the focal.
    Args:
        maxprob_ranking
        maxprob_connections
        """
    
    focal_splits = []
    for focal in tqdm(maxprob_ranking):
        count_both_pair, count_one_pair,  count_none = 0,0,0
        focal_maxprob_pairs =  [keys for keys in maxprob_connections.keys() if maxprob_connections[keys][0] == focal[0]]
    
        for pair in focal_maxprob_pairs:
            if pair[0].rsplit(None, 1)[-1]== focal[0].rsplit(None,1)[-1] and pair[1].rsplit(None, 1)[-1]== focal[0].rsplit(None,1)[-1]:
                count_both_pair +=1
            
            if pair[0].rsplit(None, 1)[-1]== focal[0].rsplit(None,1)[-1] or pair[1].rsplit(None, 1)[-1]== focal[0].rsplit(None,1)[-1]:
                count_one_pair +=1
            
            if pair[0].rsplit(None, 1)[-1] != focal[0].rsplit(None,1)[-1] and pair[1].rsplit(None, 1)[-1] != focal[0].rsplit(None,1)[-1]:
                count_none +=1
         
        focal_splits.append((focal[0], len(focal_maxprob_pairs), count_both_pair, count_one_pair - count_both_pair, count_none ))
        #focal_split[focal[0]]=[len(focal_maxprob_pairs), count_both_pair, count_one_pair - count_both_pair, count_none]
        
    return focal_splits



def city_publication_count(start, end):
    '''
    Count the number of publications for each city in a given period
    
    Args: years(list), mapaffil_file 
    Note: I had to code the pmid_to_location here since I was having error using the load_city_triplets function from data.py 
    (it takes time to run, if you can help make it efficient that will be great)
    '''
    
    with open('/storage/samuel.edet/PubMed/city_triplet/data/raw/mapaffil2016.tsv', encoding="ISO-8859-2") as f:
        pmid_to_location = load_city_triplets(f, start, end, verbose=True)
    
    pmid_to_locations = {key: list(set(pmid_to_location[key])) for key in pmid_to_location.keys()}
        
    
    city_pub_count = Counter([cities for values in pmid_to_locations.values() for cities in values])
    
    return city_pub_count 


def weighted_triple_probs(maxprob_connections, citypubcount):
    '''
    Compute the weight of each pair where a focal city dominates
    
    Args:
    maxprob_connections, citypubcount
    '''
    weight_triple_probs = {}
    citypubcount_sorted = sorted(citypubcount.items(), key=lambda kv: kv[1], reverse=True)
    
    for pair in maxprob_connections.keys():
    
        pair_weight = (citypubcount[pair[0]]*citypubcount[pair[1]]) / (citypubcount_sorted[0][1]*citypubcount_sorted[1][1])
        weight_triple_probs[pair + (pair_weight, )] = maxprob_connections[pair]
        
        
    return weight_triple_probs  


def focalrank_pairweight(weight_triple_probs):
    '''
    Rank the dominant focal cities based on the sum of weight of the pairs they sit on.
    
    Args: weight_triple_probs
    '''
    focal_weightsum = {}
    
    focal_cities = set([focal[0] for focal in weight_triple_probs.values()])
    
    for focal in tqdm(focal_cities):
    
        focal_pairweightsum = sum([key[2] for key in weight_triple_probs.keys() if weight_triple_probs[key][0]==focal])
        focal_weightsum[focal] = focal_pairweightsum
        
    focal_weightsum = sorted(focal_weightsum.items(), key=lambda kv: kv[1], reverse=True)
    
    return focal_weightsum
        
    

def cross_border_dominance(country1, country2, periods):
    ''' 
    Rank (sum weighted pairs) of dominant cities involved in Eurpean cross-border links
    
    Args: country1, country2, periods: list of tupples e.g. [(2015,2016), (2010,2014), (2005,2009), ...]
    '''
    
    
    all_period_cross_border_rank = {}
    for period in periods:
        maxprob_connections = maxprob_rank(period[0], period[1])[0]
        citypubcount =  city_publication_count(period[0], period[1])
        weight_probs = weighted_triple_probs(maxprob_connections, citypubcount)

        cross_border = {}
        for key in tqdm(weight_probs.keys()):
            if (key[0].rsplit(None, 1)[-1] == country1 and key[1].rsplit(None, 1)[-1] == country2) or (key[0].rsplit(None, 1)[-1] == country2 and key[1].rsplit(None, 1)[-1] == country1):
                cross_border[key] = weight_probs[key]

        cross_border_rank = focalrank_pairweight(cross_border)
        
        all_period_cross_border_rank[period] = [rank[0] for rank in cross_border_rank[:5]]
    
    return all_period_cross_border_rank
        
        
    