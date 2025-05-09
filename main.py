import requests
import csv
import os
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === CONFIG ===
BASE_URL = "https://www.robotevents.com/api/v2"
BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIzIiwianRpIjoiMDU5ODk5Y2MyZTk1YWUwZWM1ZTc1M2Y5YTdhODhlYmZlN2ViYmRjNmU2ZjIzOGU3NjcxOGZlOTgyYmMwMzk3NWMzOWFhNjcyMzFiMDUxMGQiLCJpYXQiOjE3NDY3NzQ2NzguNzg4NDU1LCJuYmYiOjE3NDY3NzQ2NzguNzg4NDU2OSwiZXhwIjoyNjkzNDU5NDc4Ljc4NDA4NjIsInN1YiI6IjE0NTg2MiIsInNjb3BlcyI6W119.pG2KN2W_6oKfsTvxTTLOv58hiNH4hThIW79Iq7D_RPzxRh8u-8B0I8oiOzF739c9OFtEpKcmxFlkvo6UmVx1F-qMXQVNpxrUfXVfjor4PWcQW743nCG-IhAdDwQC8gu5296RC5Kw1FnxghVEFbs-GWdZ0jcfvTk0j26Pyv7I5pp8ctKRQW0-OxKm4SQilM2k3s_bjjFQxcZBX3iWu6MxxI4RwYnEnVRsDTqKIDS0YCLO6UrOddsZjLnUtaiqr_TBEN-eXTlTeSkqGqQNHUzIPOzmSXSmy5HmmFsTYiYW5H7Ptv54TXKbj2I3BxxgfPKtS8zxIX_xbwSL0X56mMWPtXPlmH-nIt2e_ypaBu8YbED4SFtsLOB1i2XW-4Gh7750VMBW6mWncF3nvInoFrQUhAOqCkCmJuvDLEkjyysI9xlEePWqrczRSTx-9pH5UCw03s945RwbzO2-w0oHQx2uRcpdf4RNMuwnUZnX33vybtZkzXkrPTKqlMM6ZlyhjPzyj_AcyN3RMCW2jrT8rGY0LhkqXGv-XrOc2HNHDlHJDtOBmdpEIeSmvapkgjoelN4zQnx6Lc5gxSdSOLwDnHpJgy7n180b_oNuAEkOSmv40oQhei5ixZ8-nA13yZUpXiMMDVOCRKXV11FxNtHyttPOOThXP0bqHGLC6EnLPBl_9fM"

# Configure session with retries
session = requests.Session()
retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://www.robotevents.com/",
    "Origin": "https://www.robotevents.com"
}

# === FUNCTIONS ===

def make_request(url, params=None):
    try:
        response = session.get(url, headers=HEADERS, params=params, timeout=10)
        response.raise_for_status()
        
        # Check if we got a Cloudflare challenge
        if "cf-chl-bypass" in response.text.lower() or "enable javascript" in response.text.lower():
            raise requests.exceptions.RequestException("Cloudflare challenge detected")
            
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except ValueError as e:
        print(f"JSON decode error: {e}")
        return None

TEAM_INFO_FILE = "team_info.csv"
team_info_cache = {}

def get_team_id(team_number):
    global team_info_cache
    
    # Load cache if empty but file exists
    if not team_info_cache and os.path.exists(TEAM_INFO_FILE):
        with open(TEAM_INFO_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                team_info_cache[row['team_number']] = {
                    'id': int(row['id']),
                    'name': row['name'],
                    'location': row['location']
                }
    
    # Return cached data if available
    if team_number in team_info_cache:
        print(f"✅ {team_number}'s team id {team_info_cache[team_number]['id']} stored in cache.")
        return team_info_cache[team_number]['id']
    
    # API request for new teams
    params = {"grade[]": "Middle School", "program[]": 1}
    url = f"{BASE_URL}/teams?number={team_number}"
    data = make_request(url, params)
    time.sleep(1) # rate limiting
    
    if not data or not data.get('data'):
        print(f"❗ Team {team_number} not found")
        return None
    
    team_data = data['data'][0]
    team_info = {
        'id': team_data['id'],
        'name': team_data['team_name'],
        'location': f"{team_data['location']['region']}; {team_data['location']['country']}"
    }
    
    # Update cache and save to CSV
    team_info_cache[team_number] = team_info
    
    file_exists = os.path.exists(TEAM_INFO_FILE)
    with open(TEAM_INFO_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['team_number', 'id', 'name', 'location'])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            'team_number': team_number,
            'id': team_info['id'],
            'name': team_info['name'],
            'location': team_info['location']
        })
    
    print(f"✅ {team_number}'s team id {team_info_cache[team_number]['id']} requested from API.")
    return team_info['id']

def get_team_matches(team_id):
    def fetch_all_matches(round_params):
        page = 1
        per_page = 250
        all_matches = []

        while True:
            params = {
                "season[]": 190,
                "round[]": round_params,
                "page": page,
                "per_page": per_page
            }
            url = f"{BASE_URL}/teams/{team_id}/matches"
            response = make_request(url, params)
            if not response or 'data' not in response:
                break

            matches = response['data']
            if not matches:
                break  # No more data

            all_matches.extend(matches)
            page += 1

        return all_matches

    # Qualifying matches (round 2)
    data1 = fetch_all_matches(2)
    # Elimination and others (rounds 3–6)
    data2 = fetch_all_matches([3, 4, 5, 6])

    print(f"✅ {team_id}'s matches requested from API: {len(data1) + len(data2)} matches total.")
    return data1 + data2


def get_team_awards(team_id):
    params = {
        "season[]": 190
    }
    
    url = f"{BASE_URL}/teams/{team_id}/awards"
    data = make_request(url, params)
    print(f"✅ {team_id}'s awards requested from API.")
    return data.get('data', []) if data else []

EVENT_INFO_FILE = "event_info.csv"
event_info_cache = {}

def get_event_type(event_id):
    global event_info_cache

    # Load from CSV cache if memory cache is empty
    if not event_info_cache and os.path.exists(EVENT_INFO_FILE):
        with open(EVENT_INFO_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                event_info_cache[row['event_id']] = {
                    'level': row['level']
                }

    # Return cached result
    if event_id in event_info_cache:
        print(f"✅ Event {event_id} level {event_info_cache[event_id]} stored in cache.")
        return event_info_cache[event_id]['level']

    # Make API call
    url = f"{BASE_URL}/events?id={event_id}"
    data = make_request(url)
    time.sleep(1)

    if not data or not data.get('data'):
        print(f"⚠️ Event {event_id} not found")
        return None

    event_data = data['data'][0]
    event_level = event_data['level']

    # Cache and save
    event_info_cache[event_id] = {'level': event_level}

    file_exists = os.path.exists(EVENT_INFO_FILE)
    with open(EVENT_INFO_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['event_id', 'level'])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            'event_id': event_id,
            'level': event_level
        })

    print(f"✅ Event {event_id} level {event_level} requested from API.")
    return event_level

def save_matches_to_csv_and_md(matches, awards, team_number):
    # Process matches data
    for match in matches:
        if match.get('started') is None:
            match['started'] = match.get('scheduled')
    
    matches = sorted(matches, key=lambda x: (x['started'] is None, x['started']))
    
    # Prepare filenames
    filename_csv = f"{team_number}_matches.csv"
    filename_md = f"{team_number}_matches.md"

    # Writing to CSV
    with open(filename_csv, mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = [
            'Event Name', 'Event Type', 'Qualification', 'Match Name', 'Start Time',
            'Team Score', 'Opponent Score',
            'Winning Margin', 'Normalised Winning Margin', 'Verdict', 'Team Alliance', 'Winning Alliance',
            'Red Team 1', 'Red Team 2', 'Blue Team 1', 'Blue Team 2'
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        # Writing to Markdown
        with open(filename_md, mode='w', encoding='utf-8') as md_file:
            md_file.write(f"# Match Results for Team {team_number}\n\n")
            md_file.write("| Event Name | Event Type | Qualification | Match Name | Start Time | Team Score | Opponent Score | Winning Margin | Normalised Winning Margin | Verdict | Team Alliance | Winning Alliance | Red Team 1 | Red Team 2 | Blue Team 1 | Blue Team 2 |\n")
            md_file.write("|------------|------------|---------------|------------|------------|------------|-----------------|----------------|---------------------------|---------|---------------|------------------|------------|------------|-------------|-------------|\n")

            for match in matches:
                # Extract match data
                event = match.get('event', {})
                event_name = event.get('name', 'Unknown').replace(",", "")
                event_type = get_event_type(event.get('id', -1))
                match_name = match.get('name', 'Unknown')
                
                # Find qualification from awards
                qualification = 'None'
                for award in awards:
                    award_event = award.get('event', {})
                    if award_event.get('name', '').replace(",", "") == event_name:
                        qualifications_list = award.get('qualifications', [])
                        qualification = qualifications_list[0] if qualifications_list else 'None'
                        break

                # Handle start time
                start_time = match.get('started') or match.get('scheduled') or 'TBD'
                
                # Process alliances
                alliances = match.get('alliances', [])
                red_teams = []
                blue_teams = []
                red_score = blue_score = None
                team_alliance = None

                for alliance in alliances:
                    color = alliance.get('color')
                    score = alliance.get('score')
                    teams = [team['team']['name'] for team in alliance.get('teams', [])]

                    if color == 'red':
                        red_teams = teams
                        red_score = score
                        if team_number in [t.split()[0] for t in teams]:  # Check if team number is in alliance
                            team_alliance = 'red'
                    elif color == 'blue':
                        blue_teams = teams
                        blue_score = score
                        if team_number in [t.split()[0] for t in teams]:  # Check if team number is in alliance
                            team_alliance = 'blue'

                # Calculate scores and margins
                if None in (red_score, blue_score, team_alliance):
                    team_score = opponent_score = margin = normalised_win_margin = 'N/A'
                    verdict = 'D'
                    winning_alliance = 'Unknown'
                else:
                    if team_alliance == 'red':
                        team_score = red_score
                        opponent_score = blue_score
                    else:
                        team_score = blue_score
                        opponent_score = red_score

                    margin = team_score - opponent_score
                    normalised_win_margin = margin / (team_score + opponent_score) if (team_score + opponent_score) != 0 else -1

                    if margin > 0:
                        winning_alliance = team_alliance
                        verdict = 'W'
                    elif margin < 0:
                        winning_alliance = 'blue' if team_alliance == 'red' else 'red'
                        verdict = 'L'
                    else:
                        winning_alliance = 'Tie'
                        verdict = 'D'

                # Ensure we have 2 teams per alliance
                red_teams.extend([''] * (2 - len(red_teams)))
                blue_teams.extend([''] * (2 - len(blue_teams)))

                # Write to CSV
                writer.writerow({
                    'Event Name': event_name,
                    'Event Type': event_type,
                    'Qualification': qualification,
                    'Match Name': match_name,
                    'Start Time': start_time,
                    'Team Score': team_score,
                    'Opponent Score': opponent_score,
                    'Winning Margin': margin,
                    'Normalised Winning Margin': normalised_win_margin,
                    'Verdict': verdict,
                    'Team Alliance': team_alliance or 'Unknown',
                    'Winning Alliance': winning_alliance,
                    'Red Team 1': red_teams[0] if len(red_teams) > 0 else '',
                    'Red Team 2': red_teams[1] if len(red_teams) > 1 else '',
                    'Blue Team 1': blue_teams[0] if len(blue_teams) > 0 else '',
                    'Blue Team 2': blue_teams[1] if len(blue_teams) > 1 else ''
                })

                # Write to Markdown
                md_file.write(f"| {event_name} | {event_type} | {qualification} | {match_name} | {start_time} | {team_score} | {opponent_score} | {margin} | {normalised_win_margin} | {verdict} | {team_alliance or 'Unknown'} | {winning_alliance} | {red_teams[0] if len(red_teams) > 0 else ''} | {red_teams[1] if len(red_teams) > 1 else ''} | {blue_teams[0] if len(blue_teams) > 0 else ''} | {blue_teams[1] if len(blue_teams) > 1 else ''} |\n")

    print(f"✅ Match results saved to {filename_csv} and {filename_md}")

def save_awards_to_csv_and_md(awards, team_number):
    filename_csv = f"{team_number}_awards.csv"
    filename_md = f"{team_number}_awards.md"

    with open(filename_csv, mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['Event Name', 'Event Type', 'Title', 'Qualifications']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        with open(filename_md, mode='w', encoding='utf-8') as md_file:
            md_file.write(f"# Awards for Team {team_number}\n\n")
            md_file.write("| Event Name | Event Type | Title | Qualifications |\n")
            md_file.write("|------------|------------|-------|----------------|\n")

            for award in awards:
                event = award.get('event', {})
                event_name = event.get('name', 'Unknown').replace(",", "")
                event_type = get_event_type(event.get('id', -1))
                title = award.get('title', 'Unknown')
                qualifications = ";".join(award.get('qualifications', []))

                writer.writerow({
                    'Event Name': event_name,
                    'Event Type': event_type,
                    'Title': title,
                    'Qualifications': qualifications
                })

                md_file.write(f"| {event_name} | {event_type} | {title} | {qualifications} |\n")

    print(f"✅ Award results saved to {filename_csv} and {filename_md}")

failed = []

def main_get_data(team_number):
    print(f"\nFetching data for Team {team_number}...")

    if team_number == "3946S":
        print("Skipping team 3946S (special case, no awards)")
        return
    
    # Check if files already exist
    # if os.path.exists(f"{team_number}_matches.csv") and os.path.exists(f"{team_number}_awards.csv"):
    #     print("Data already exists for this team. Skipping...")
    #     return

    # Get team ID
    team_id = get_team_id(team_number)
    if not team_id:
        failed.append(team_number)
        print(f"Failed to retrieve team ID for {team_number}")
        return

    print(f"Found Team ID: {team_id}")

    # Fetch data with rate limiting
    print("Fetching awards...")
    awards = get_team_awards(team_id)
    time.sleep(1)
    print("Fetching matches...")
    matches = get_team_matches(team_id)
    time.sleep(1)  # Rate limiting
    
    # Save data
    if matches:
        # for match in matches:
        #     print(f"Match type: {match['name']}")
        save_matches_to_csv_and_md(matches, awards, team_number)
    else:
        failed.append(team_number)
        print(f"No matches found for team {team_number}")

    if awards:
        save_awards_to_csv_and_md(awards, team_number)
    else:
        failed.append(team_number)
        print(f"No awards found for team {team_number}")
    time.sleep(5)

event_type_weights = {
    'World': 6,
    'Signature': 4,
    'National': 2.5,
    'Regional': 2.5,
    'State': 2,
    'Other': 1,
}

def compute_kpi(team_list, match_folder="./", output_file="research_kpi_summary.csv"):
    # team_list.remove('30214A')
    all_team_stats = {}

    for team in team_list:
        filepath = os.path.join(match_folder, f"{team}_matches.csv")
        if not os.path.exists(filepath):
            print(f"❗ File not found for team {team}: {filepath}")
            continue
        
        df = pd.read_csv(filepath)

        # Clean missing or incomplete matches
        df = df[(df['Team Score'] != 'N/A') & (df['Opponent Score'] != 'N/A')]
        df['Team Score'] = pd.to_numeric(df['Team Score'])
        df['Opponent Score'] = pd.to_numeric(df['Opponent Score'])

        # Remove anomalies: one team scores 0
        df = df[(df['Team Score'] > 0) & (df['Opponent Score'] > 0)]

        df['Event Type'] = df['Event Type'].fillna('Other')
        df['Qualification'] = df['Qualification'].fillna('None')

        # Determine match categories
        df['Is Regional+'] = df['Event Type'].isin(['World', 'Signature', 'National', 'Regional'])
        df['Is Signature+'] = df['Event Type'].isin(['World', 'Signature'])
        df['Is Elimination'] = df['Match Name'].str.contains('QF|SF|Final|R16|R-16', case=False, na=False)

        # Compute match weights
        df['Weight'] = df['Event Type'].map(event_type_weights).fillna(1.0)

        # --- Compute categories ---
        stats = {}

        def compute_win_rate(sub_df):
            if len(sub_df) == 0:
                return 0
            wins = (sub_df['Verdict'] == 'W').sum()
            return wins / len(sub_df)

        def compute_avg(sub_df, column):
            if len(sub_df) == 0:
                return 0
            return sub_df[column].mean()

        def compute_weighted_avg(sub_df, column):
            if len(sub_df) == 0:
                return 0
            return (sub_df[column] * sub_df['Weight']).sum() / sub_df['Weight'].sum()

        def compute_weighted_normalized_margin(sub_df):
            if len(sub_df) == 0:
                return 0
            margins = (sub_df['Team Score'] - sub_df['Opponent Score']) / (sub_df['Team Score'] + sub_df['Opponent Score'])
            weighted_margins = margins * sub_df['Weight']
            return weighted_margins.sum() / sub_df['Weight'].sum()

        # All Matches
        stats['all_win_rate'] = compute_win_rate(df)
        stats['all_avg_for'] = compute_avg(df, 'Team Score')
        stats['all_avg_against'] = compute_avg(df, 'Opponent Score')
        stats['all_match_count'] = len(df)

        stats['weighted_avg_for'] = compute_weighted_avg(df, 'Team Score')
        stats['weighted_avg_against'] = compute_weighted_avg(df, 'Opponent Score')
        stats['weighted_normalized_margin'] = compute_weighted_normalized_margin(df)

        # Regional+ Matches
        regional_df = df[df['Is Regional+']]
        stats['regional_plus_win_rate'] = compute_win_rate(regional_df)
        stats['regional_plus_match_count'] = len(regional_df)

        # Signature+ Matches
        signature_df = df[df['Is Signature+']]
        stats['signature_plus_win_rate'] = compute_win_rate(signature_df)
        stats['signature_plus_match_count'] = len(signature_df)

        # Elimination Matches
        elim_df = df[df['Is Elimination']]
        stats['elim_win_rate'] = compute_win_rate(elim_df)
        stats['elim_avg_for'] = compute_avg(elim_df, 'Team Score')
        stats['elim_avg_against'] = compute_avg(elim_df, 'Opponent Score')
        stats['elim_match_count'] = len(elim_df)

        # Elimination + Regional+
        elim_regional_df = df[df['Is Elimination'] & df['Is Regional+']]
        stats['elim_regional_plus_win_rate'] = compute_win_rate(elim_regional_df)
        stats['elim_regional_plus_match_count'] = len(elim_regional_df)

        # Elimination + Signature+
        elim_signature_df = df[df['Is Elimination'] & df['Is Signature+']]
        stats['elim_signature_plus_win_rate'] = compute_win_rate(elim_signature_df)
        stats['elim_signature_plus_match_count'] = len(elim_signature_df)

        all_team_stats[team] = stats

    # === Create DataFrame ===
    rows = []
    for team, stat in all_team_stats.items():
        rows.append({
            'Team': team,
            'All Win Rate': stat['all_win_rate'],
            'All Avg For': stat['all_avg_for'],
            'All Avg Against': stat['all_avg_against'],
            'All Matches Played': stat['all_match_count'],
            'Weighted Avg For': stat['weighted_avg_for'],
            'Weighted Avg Against': stat['weighted_avg_against'],
            'Weighted Normalized Win Margin': stat['weighted_normalized_margin'],
            'Regional+ Win Rate': stat['regional_plus_win_rate'],
            'Regional+ Matches Played': stat['regional_plus_match_count'],
            'Signature+ Win Rate': stat['signature_plus_win_rate'],
            'Signature+ Matches Played': stat['signature_plus_match_count'],
            'Elim Win Rate': stat['elim_win_rate'],
            'Elim Avg For': stat['elim_avg_for'],
            'Elim Avg Against': stat['elim_avg_against'],
            'Elim Matches Played': stat['elim_match_count'],
            'Elim Regional+ Win Rate': stat['elim_regional_plus_win_rate'],
            'Elim Regional+ Matches Played': stat['elim_regional_plus_match_count'],
            'Elim Signature+ Win Rate': stat['elim_signature_plus_win_rate'],
            'Elim Signature+ Matches Played': stat['elim_signature_plus_match_count'],
        })

    kpi_df = pd.DataFrame(rows)

    # === Add Rankings ===
    metric_cols = [
        'All Win Rate',
        'All Avg For',
        'All Avg Against',
        'Weighted Avg For',
        'Weighted Avg Against',
        'Weighted Normalized Win Margin',
        'Regional+ Win Rate',
        'Signature+ Win Rate',
        'Elim Win Rate',
        'Elim Avg For',
        'Elim Avg Against',
        'Elim Regional+ Win Rate',
        'Elim Signature+ Win Rate',
    ]

    for col in metric_cols:
        ascending = True if ("Against" in col) else False  # lower avg against is better
        kpi_df[f"{col} Rank"] = kpi_df[col].rank(ascending=ascending, method='min').astype(int)

    # === Save output ===
    kpi_df.sort_values('All Win Rate Rank').to_csv(output_file, index=False)
    print(f"✅ KPI Summary saved to {output_file}")

def main_analyse_data(team_number, match_folder="./", kpi_file="research_kpi_summary.csv", output_folder="./"):
    # === Load Files ===
    awards_path = os.path.join(match_folder, f"{team_number}_awards.csv")
    matches_path = os.path.join(match_folder, f"{team_number}_matches.csv")
    kpi_path = os.path.join(match_folder, kpi_file)

    if not (os.path.exists(awards_path) and os.path.exists(matches_path) and os.path.exists(kpi_path)):
        print(f"❗ Missing one or more files for team {team_number}.")
        return

    awards_df = pd.read_csv(awards_path)
    matches_df = pd.read_csv(matches_path)
    kpi_df = pd.read_csv(kpi_path)

    # === 1. Most important KPIs ===
    important_metrics = [
        ('All Win Rate', 'All Win Rate Rank', 'All Matches Played'),
        ('Weighted Avg For', 'Weighted Avg For Rank', None),
        ('Weighted Avg Against', 'Weighted Avg Against Rank', None),
        ('Weighted Normalized Win Margin', 'Weighted Normalized Win Margin Rank', None),
        ('Regional+ Win Rate', 'Regional+ Win Rate Rank', 'Regional+ Matches Played'),
        ('Signature+ Win Rate', 'Signature+ Win Rate Rank', 'Signature+ Matches Played'),
        ('Elim Win Rate', 'Elim Win Rate Rank', 'Elim Matches Played'),
    ]

    team_kpi_row = kpi_df[kpi_df['Team'] == team_number]
    if team_kpi_row.empty:
        print(f"❗ Team {team_number} not found in KPI summary.")
        return

    # Pre-calculate max rank for each metric
    max_ranks = {rank_metric: kpi_df[rank_metric].max() for _, rank_metric, _ in important_metrics}

    # Build KPI Markdown Table
    kpi_table_md = "| KPI | Value | Matches Played | Rank (Research) | Top % |\n|:---|:-----|:--------------:|:----|:-----|\n"
    for metric, rank_metric, matches_col in important_metrics:
        value = f"{team_kpi_row.iloc[0][metric]:.3f}"
        rank = int(team_kpi_row.iloc[0][rank_metric])
        max_rank = float(max_ranks[rank_metric])
        rank_pct = f"{(rank / max_rank * 100 if max_rank > 0 else 0):.3f}%"
        
        # Add matches played if available for this metric
        matches_played = str(int(team_kpi_row.iloc[0][matches_col])) if matches_col else ""
        
        kpi_table_md += f"| {metric} | {value} | {matches_played} | {rank} | {rank_pct} |\n"

    # === 2. How the team qualified for Worlds ===
    awards_df['Qualifications'] = awards_df['Qualifications'].astype(str)
    qualifications = awards_df[awards_df['Qualifications'].str.contains('World Championship', na=False)]
    if qualifications.empty:
        worlds_qualifications_md = "No recorded qualifications for Worlds found."
    else:
        worlds_qualifications_md = "; ".join(qualifications['Event Name'].unique())

    # === 3. Signature Events Participation ===
    signature_matches = matches_df[matches_df['Event Type'] == 'Signature']
    signature_events = signature_matches['Event Name'].dropna().unique()

    sig_summary_table_md = "| Signature Event | Elim Stage Reached | Won? |\n|:----------------|:-------------------|:----|\n"
    for event in signature_events:
        event_matches = signature_matches[signature_matches['Event Name'] == event]
        max_stage = "Qualification"
        won_event = False
        if any(event_matches['Match Name'].str.contains('Final', case=False, na=False)):
            max_stage = "Finals"
            final_matches = event_matches[event_matches['Match Name'].str.contains('Final', case=False, na=False)]
            wincnt = 0
            for match in final_matches.itertuples():
                # print(match)
                if match.Verdict == 'W':
                    wincnt += 1
            if wincnt >= 2:
                won_event = True
        elif any(event_matches['Match Name'].str.contains('SF', case=False, na=False)):
            max_stage = "Semifinals"
        elif any(event_matches['Match Name'].str.contains('QF', case=False, na=False)):
            max_stage = "Quarterfinals"
        elif any(event_matches['Match Name'].str.contains('R16', case=False, na=False)) or any(event_matches['Match Name'].str.contains('R-16', case=False, na=False)):
            max_stage = "Round of 16"

        sig_summary_table_md += f"| {event} | {max_stage} | {'🏆' if won_event else ''} |\n"

    sig_events_count = len(signature_events)

    # === 4. Major Awards (ALL awards, sorted by importance) ===
    awards_filtered = awards_df.copy()

    # Event Type Priority
    event_type_priority = {
        'World': 0,
        'Signature': 1,
        'Regional': 2,
        'National': 3,
    }
    # Award Title Priority
    award_title_priority = {
        'Excellence Award': 1,
        'Tournament Champion': 2,
        'Design Award': 3,
    }
    default_event_priority = 99
    default_award_priority = 99

    def get_event_priority(event_type):
        return event_type_priority.get(event_type, default_event_priority)

    def get_award_priority(title):
        for key in award_title_priority:
            if key.lower() in title.lower():
                return award_title_priority[key]
        return default_award_priority

    awards_filtered['Event Priority'] = awards_filtered['Event Type'].apply(get_event_priority)
    awards_filtered['Award Priority'] = awards_filtered['Title'].apply(get_award_priority)

    # Sort by Event Priority first, then Award Priority
    awards_filtered = awards_filtered.sort_values(
        ['Event Priority', 'Award Priority'],
        ascending=[True, True]
    )

    # Build Awards Table
    if awards_filtered.empty:
        awards_md = "No awards found."
    else:
        awards_md = "| Award | Event | Event Type | Qualification |\n|:------|:------|:-----------|:--------------|\n"
        for idx, row in awards_filtered.iterrows():
            title = row['Title']
            event_name = row['Event Name']
            event_type = row['Event Type']
            qualification = row.get('Qualifications', '-')
            if pd.isna(qualification) or qualification.strip() == "":
                qualification = "-"

            # Apply bolding rules
            bold_title = False
            if event_type == 'Signature':
                bold_title = True
            elif event_type in ['Regional', 'National', 'World']:
                if ('Excellence' in title) or ('Tournament Champion' in title):
                    bold_title = True

            if bold_title:
                title = f"**{title}**"
                event_name = f"**{event_name}**"

            awards_md += f"| {title} | {event_name} | {event_type} | {qualification} |\n"

    # === Build Markdown ===
    markdown_content = f"""# [Team {team_number}](https://https://www.robotevents.com/teams/V5RC/{team_number}) Performance Summary

##  Team Information
- **Team Name**: {team_info_cache[team_number]['name']}
- **Location**: {team_info_cache[team_number]['location']}

## 📈 Key Performance Indicators
- Important metrics for the team

{kpi_table_md}

## 🎯 Worlds Qualification
{worlds_qualifications_md}

## 🏆 Signature Events Participation
- Total Signature Events Attended: **{sig_events_count}**

{sig_summary_table_md}

## 🥇 Major Awards
- Awards that qualified the team for at least something
- Sorted by dec. importance

{awards_md}
"""

    # === Save Output ===
    output_path = os.path.join(output_folder, f"{team_number}.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"✅ Summary for {team_number} saved to {output_path}")

def div_analyse(team_numbers, match_folder="./", kpi_file="research_kpi_summary.csv", output_folder="./"):
    """
    Analyze multiple teams and generate a markdown report of strong teams
    Strong teams are defined as:
    1. Award winners at any Signature event, OR
    2. Tournament Champion/Excellence Award winners at Regional events
    """
    # Load data files
    kpi_df = pd.read_csv(os.path.join(match_folder, kpi_file))
    
    # Load team info if available
    team_info = {}
    team_info_path = os.path.join(match_folder, "team_info.csv")
    if os.path.exists(team_info_path):
        team_info_df = pd.read_csv(team_info_path)
        team_info = team_info_df.set_index('team_number').to_dict('index')
    
    # Initialize markdown content
    md_content = "# Strong Teams Analysis\n\n"
    strong_teams = []
    
    for team_number in team_numbers:
        # Load team-specific data
        matches_path = os.path.join(match_folder, f"{team_number}_matches.csv")
        awards_path = os.path.join(match_folder, f"{team_number}_awards.csv")
        
        if not (os.path.exists(matches_path) and os.path.exists(awards_path)):
            continue
            
        matches_df = pd.read_csv(matches_path)
        awards_df = pd.read_csv(awards_path)
        
        # Check for strong team criteria
        is_strong = False
        team_details = {
            'team_number': team_number,
            'team_name': team_info.get(team_number, {}).get('name', 'Unknown'),
            'location': team_info.get(team_number, {}).get('location', 'Unknown'),
            'signature_awards': [],
            'regional_awards': [],
            'kpi': {}
        }
        
        # 1. Check Signature event awards
        signature_awards = awards_df[awards_df['Event Type'] == 'Signature']
        if not signature_awards.empty:
            is_strong = True
            # if signature_awards['Title'].str.contains('Tournament Champion', case=False).any() or signature_awards['Title'].str.contains('Excellence', case=False).any():
            #     signature_awards['Title'].append("🏆")
            team_details['signature_awards'] = signature_awards[['Title', 'Event Name', 'Event Type']].to_dict('records')

        signature_matches = matches_df[matches_df['Event Type'] == 'Signature']
        signature_events = signature_matches['Event Name'].dropna().unique()
        for event in signature_events:
            max_stage = "NA"
            event_matches = signature_matches[signature_matches['Event Name'] == event]
            max_stage = "Qualification"
            if any(event_matches['Match Name'].str.contains('Final', case=False, na=False)):
                max_stage = "Finals"
            elif any(event_matches['Match Name'].str.contains('SF', case=False, na=False)):
                max_stage = "SF"
            elif any(event_matches['Match Name'].str.contains('QF', case=False, na=False)):
                max_stage = "QF"
            elif any(event_matches['Match Name'].str.contains('R16', case=False, na=False)) or any(event_matches['Match Name'].str.contains('R-16', case=False, na=False)):
                max_stage = "R-16"
            if max_stage == "Finals" or max_stage == "SF" or max_stage == "QF":
                is_strong = True
                team_details['signature_awards'].append({
                    'Title': f"{max_stage}",
                    'Event Name': event,
                    'Event Type': 'Signature'
                })
        
        # 2. Check Regional Tournament Champion/Excellence
        regional_awards = awards_df[
            (awards_df['Event Type'] == 'Regional') & 
            (awards_df['Title'].str.contains('Tournament Champion|Excellence Award', case=False))
        ]
        if not regional_awards.empty:
            is_strong = True
            team_details['regional_awards'] = regional_awards[['Title', 'Event Name', 'Event Type']].to_dict('records')
        
        # Get KPI data if available
        team_kpi = kpi_df[kpi_df['Team'] == team_number]
        if not team_kpi.empty:
            team_details['kpi'] = {
                'win_rate': team_kpi.iloc[0]['All Win Rate'],
                'rank': team_kpi.iloc[0]['All Win Rate Rank']
            }
        
        if is_strong:
            strong_teams.append(team_details)
    
    # Sort strong teams by KPI rank (best first)
    strong_teams.sort(key=lambda x: x['kpi'].get('rank', float('inf')))
    
    # Generate markdown content
    for team in strong_teams:
        md_content += f"## Team [{team['team_number']}](/{team['team_number']}.md): {team['team_name']}\n"
        md_content += f"*Location: {team['location']}*\n\n"
        
        # Add KPI info if available
        if team['kpi']:
            md_content += (
                f"- **Win Rate**: {team['kpi']['win_rate']:.3f} "
                f"(Rank: {team['kpi']['rank']})\n"
            )
        
        # Add Signature awards
        if team['signature_awards']:
            md_content += "### Signature Event Awards\n\n"
            md_content += "| Award | Event | Event Type |\n|:------|:------|:-----------|\n"
            for award in team['signature_awards']:
                md_content += f"| {award['Title']} | {award['Event Name']} | {award['Event Type']} |\n"
        
        # Add Regional awards
        if team['regional_awards']:
            md_content += "\n### Regional Event Awards\n\n"
            md_content += "| Award | Event | Event Type |\n|:------|:------|:-----------|\n"
            for award in team['regional_awards']:
                md_content += f"| {award['Title']} | {award['Event Name']} | {award['Event Type']} |\n"
        
        md_content += "\n---\n\n\n"
    
    # Save to file
    output_path = os.path.join(output_folder, "research.md")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)
    
    print(f"✅ Report generated: {output_path}")
    return output_path

if __name__ == "__main__":
    research_teams = [
        "38Z", "106D", "393V", "663J", "857V", "938E", "1028G", "1089F", "1231B", 
        "1281Z",
        "1474X", "1698A", "1831A", "2088B", "2502M", "2616V", "3012A", "3168H", "3588Y",
        "4139B", "4591X", "4613P", "4886P", "6390F", "7139X", "7481E", "7830N", "8223Z",
        "8778Z", "8871A", "9039N", "9364X", "9568X", "10012W", "10102Z", "11556A", "12161N",
        "12768A", "15155E", "17760R", "18190Y", "18716A", "20315B", "23118R", "24658J",
        "27908V", "29566V", "31319X", "33705X", "35993B", "38247A", "40994B", "43010S",
        "45000B", "47510A", "50021A", "52800B", "54008B", "56448B", "57711V", "61187E",
        "64410A", "66799T", "70699A", "71610R", "73548A", "74074E", "75442A", "76426C",
        "77774V", "78877A", "79298P", "79901A", "83561A", "86254X", "89089Y", "95070F",
        "97310C", "98548N", "99009B", "99656A"
    ]

    team_numbers = research_teams.copy()

    for team in team_numbers:
        get_team_id(team)

    # for team_number in team_numbers:
    #     main_get_data(team_number)
    compute_kpi(research_teams) # works on research only
    for team_number in team_numbers:
        main_analyse_data(team_number) # works on research only
    div_analyse(research_teams) # works on research only

    for x in failed:
        print(x)