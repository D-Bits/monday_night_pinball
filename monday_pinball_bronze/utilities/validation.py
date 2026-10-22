# Row-level expectations per bronze table. Each expression must reference
# a column that exists in that table; the tables share no common column.
bronze_validations = {
    "raw_standings": {"valid_rank": "`#` IS NOT NULL"},
    "raw_stats": {"valid_player": "Player IS NOT NULL"},
    "raw_team_roster": {"valid_roster": "Roster IS NOT NULL"},
    "raw_venue": {"valid_venue": "venue_init IS NOT NULL"},
    "raw_machine": {"valid_machine": "machine_name IS NOT NULL"},
    "raw_venue_machine": {"valid_machine": "machine_name IS NOT NULL"},
}