import json
from pytasy_cricket.delivery_record import DeliveryRecord
from pytasy_cricket.match_metadata_record import MatchMetadataRecord
from typing import Dict, Any, List
from pyspark.sql import Row

def read_match_file(match_file_full_path: str) -> List[Row]:
    match_data = json.loads(open(match_file_full_path,'r').read())
    match_id = match_file_full_path.split('/')[-1].split('.')[0]
    match_metadata  = __extract_match_metadata(match_data=match_data, match_id=match_id)
    player_registry = match_data['info']['registry']['people']
    all_innings = match_data['innings']
    delivery_records = __extract_delivery_records(all_innings, player_registry, match_metadata)
    return __get_flattened_delivery_records(delivery_records, match_metadata)




def __extract_match_metadata(match_data: Dict[str, Any], match_id: str) -> MatchMetadataRecord:
    match_info = match_data['info']
    return MatchMetadataRecord(
        match_id = match_id,
        start_date= sorted(match_info['dates'])[0],
        event_name= match_info['event']['name'] if 'event' in match_info else None,
        match_type= match_info['match_type'],
        team_type= match_info['team_type'],
        venue_name= match_info['venue'],
        city= match_info['city'] if 'city' in match_info else None,
        gender= match_info['gender'],
        total_overs= match_info['overs'] if 'overs' in match_info else 0,
        season= match_info['season'],
        toss= match_info['toss']['winner'],
        team_1=match_info['teams'][0],
        team_2=match_info['teams'][1]
    )

def __extract_delivery_records(all_innings: List[Any], player_registry: Dict[str, str], match_metadata: MatchMetadataRecord) -> List[DeliveryRecord]:
    delivery_records: List[DeliveryRecord] = []
    for innings_number in range(len(all_innings)):
        innings = all_innings[innings_number]
        batter_team = innings['team']
        if 'overs' not in innings:
            continue
        for over in innings['overs']:
            for delivery_number in range(len(over['deliveries'])):
                delivery = over['deliveries'][delivery_number]
                wicket = delivery['wickets'][0] if "wickets" in delivery and len(delivery['wickets']) > 0 else None
                wicket_player_name = wicket["player_out"] if wicket is not None  else None
                wicket_fielder_name = wicket['fielders'][0]['name'] if wicket is not None and 'fielders' in wicket and len(wicket['fielders']) > 0 and 'name' in wicket['fielders'][0] else None
                delivery_records.append(
                    DeliveryRecord(
                        innings_number= innings_number,
                        batter_name= delivery['batter'],
                        batter_team= innings['team'],
                        batter_id= player_registry[delivery['batter']],
                        bowler_name= delivery['bowler'],
                        bowler_team= match_metadata.team_1 if match_metadata.team_1 != batter_team else match_metadata.team_2,
                        bowler_id= player_registry[delivery['bowler']],
                        batter_runs= delivery['runs']['batter'],
                        extra_runs= delivery['runs']['extras'],
                        total_runs= delivery['runs']['total'  ],
                        over= over['over'],
                        ball= delivery_number,
                        is_wicket= int(wicket is not None),
                        wicket_player_name= wicket_player_name,
                        wicket_player_id=player_registry[wicket_player_name] if wicket_player_name is not None else None,
                        wicket_fielder_name=wicket_fielder_name,
                        wicket_fielder_id=player_registry[wicket_fielder_name] if wicket_fielder_name is not None else None,
                        wicket_kind=wicket['kind'] if wicket is not None else None
                    )
                )
    return delivery_records


def __get_flattened_delivery_records(delivery_records: List[DeliveryRecord], match_metadata: MatchMetadataRecord) -> List[Row]:
    flattened_delivery_records: List[Row] = []
    for delivery_record in delivery_records:
        flattened_delivery_records.append(
            Row(
                innings_number= delivery_record.innings_number,
                batter_name= delivery_record.batter_name,
                batter_team= delivery_record.batter_team,
                batter_id= delivery_record.batter_id,
                bowler_name= delivery_record.bowler_name,
                bowler_team= delivery_record.bowler_team,
                bowler_id= delivery_record.bowler_id,
                batter_runs= delivery_record.batter_runs,
                extra_runs= delivery_record.extra_runs,
                total_runs= delivery_record.total_runs,
                over= delivery_record.over,
                ball= delivery_record.ball,
                is_wicket= delivery_record.is_wicket,
                wicket_player_name=delivery_record.wicket_player_name,
                wicket_player_id=delivery_record.wicket_player_id,
                wicket_fielder_name=delivery_record.wicket_fielder_name,
                wicket_fielder_id=delivery_record.wicket_fielder_id,
                wicket_kind=delivery_record.wicket_kind,
                match_id= match_metadata.match_id,
                start_date= match_metadata.start_date,
                event_name= match_metadata.event_name,
                match_type= match_metadata.match_type,
                team_type= match_metadata.team_type,
                venue_name= match_metadata.venue_name,
                city= match_metadata.city,
                gender= match_metadata.gender,
                total_overs= match_metadata.total_overs,
                season= match_metadata.season,
                toss= match_metadata.toss,
                team_1= match_metadata.team_1,
                team_2= match_metadata.team_2,
                year=int(match_metadata.start_date.split('-')[0]),
                month=int(match_metadata.start_date.split('-')[1]),
                dt=int(match_metadata.start_date.split('-')[2]),
            )
        )
    return flattened_delivery_records
