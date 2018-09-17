from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.n1ql import N1QLQuery

SERVER = "192.168.1.15"
USR = "Administrator"
PWD = "password"

CURRENT_YEAR = "2018"


QUERY_GET_PINS_BY_REGION = \
            "SELECT ROUND(AVG(100 - TONUMBER(w.place)/w.total_teams*100)) AS rating , w.full_name, w.lat, w.lon, w.club  " \
            "FROM (SELECT MIN(TONUMBER(b1.final_place)) as place, b1.total_teams, b1.club, b2.full_name, b2.lat, b2.lon  " \
            "FROM {region} b1 INNER JOIN {region} b2 ON KEYS b1.club WHERE b1.doctype = 'result' " \
                    "AND b1.year = '{year}' " \
            "AND (b2.active = true OR b2.active IS MISSING) " \
            "GROUP BY b1.age, b1.total_teams, b1.club, b2.full_name, b2.lat, b2.lon) AS w " \
            "GROUP BY w.full_name, w.lat, w.lon, w.club";

QUERY_GET_BASIC_CLUB_INFO = \
            "SELECT address, full_name, website FROM {region} USE KEYS['{club_id}']"


QUERY_GET_CLUB_SEASON_RESULTS = \
            "SELECT team_name AS team, age AS age, regional_place AS finised_regionals_at, total_teams AS out_of, " \
            "ROUND(100 - TONUMBER(final_place)/total_teams*100) AS our_rating FROM {region} " \
            "WHERE doctype = 'result' AND club = '{club_id}' AND year = '{year}' " \
            "ORDER BY age, TO_NUMBER(final_place)"

cluster = Cluster('couchbase://{}'.format(SERVER))
authenticator = PasswordAuthenticator(USR, PWD)
cluster.authenticate(authenticator)
cb = cluster.open_bucket("regions")

def get_pins_by_region(region):
    statement = QUERY_GET_PINS_BY_REGION.format(region = region, year = CURRENT_YEAR)
    query_res = cb.n1ql_query(N1QLQuery(statement))
    all_pins = list()

    for r in query_res:
        all_pins.append(r)
    return all_pins


def get_basic_club_info(club_id, region):
    statement = QUERY_GET_BASIC_CLUB_INFO.format(club_id = club_id, region=region)
    query_res = cb.n1ql_query(N1QLQuery(statement))
    results = list()
    for r in query_res:
        results.append(r)
    return results


def get_club_season_results(club_id, region):
    statement = QUERY_GET_CLUB_SEASON_RESULTS.format(club_id = club_id, region=region, year=CURRENT_YEAR)
    query_res = cb.n1ql_query(N1QLQuery(statement))
    results = list()
    for r in query_res:
        results.append(r)
    return results

