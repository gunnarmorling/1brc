#!/usr/bin/env python3
"""
Bundesliga Match Analysis Tool
Analyzes upcoming Bundesliga matches using ELO ratings, xG values,
squad values, and injury reports.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import json
import time
from typing import Dict, List, Optional
import re
import sys
import argparse


class BundesligaAnalyzer:
    """Main class for analyzing Bundesliga matches."""

    def __init__(self, demo_mode: bool = False):
        self.demo_mode = demo_mode
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Team name mappings for different data sources
        self.team_mappings = {
            'Bayern Munich': 'FC Bayern München',
            'RB Leipzig': 'RasenBallsport Leipzig',
            'Dortmund': 'Borussia Dortmund',
            'Leverkusen': 'Bayer 04 Leverkusen',
            'Frankfurt': 'Eintracht Frankfurt',
            'Freiburg': 'SC Freiburg',
            'Union Berlin': '1. FC Union Berlin',
            'Stuttgart': 'VfB Stuttgart',
            'Hoffenheim': 'TSG 1899 Hoffenheim',
            'Wolfsburg': 'VfL Wolfsburg',
            'Gladbach': 'Borussia Mönchengladbach',
            'Mainz': '1. FSV Mainz 05',
            'Köln': '1. FC Köln',
            'Augsburg': 'FC Augsburg',
            'Bochum': 'VfL Bochum 1848',
            'Bremen': 'Werder Bremen',
            'Heidenheim': '1. FC Heidenheim 1846',
            'Darmstadt': 'SV Darmstadt 98'
        }

    def normalize_team_name(self, name: str) -> str:
        """Normalize team names across different sources."""
        for short, full in self.team_mappings.items():
            if short.lower() in name.lower() or full.lower() in name.lower():
                return full
        return name

    def get_upcoming_matches(self) -> List[Dict]:
        """Fetch upcoming Bundesliga matches from OpenLigaDB API."""
        print("📅 Fetching upcoming matches...")

        if self.demo_mode:
            print("  (Using demo data)")
            return self._get_example_matches()

        try:
            # Get current Bundesliga season and matchday
            url = "https://api.openligadb.de/getavailableleagues"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            # Get next matches for Bundesliga (bl1)
            url = "https://api.openligadb.de/getnextmatchbyleagueid/4555"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            matches_data = response.json()
            matches = []

            # Get the next 5 upcoming matches
            for match in matches_data[:5]:
                match_info = {
                    'home_team': match['team1']['teamName'],
                    'away_team': match['team2']['teamName'],
                    'date': match['matchDateTime'],
                    'matchday': match['group']['groupOrderID']
                }
                matches.append(match_info)

            print(f"✓ Found {len(matches)} upcoming matches")
            return matches

        except Exception as e:
            print(f"⚠ Error fetching matches: {e}")
            # Fallback to example matches
            return self._get_example_matches()

    def _get_example_matches(self) -> List[Dict]:
        """Return example matches if API fails."""
        base_date = datetime.now() + timedelta(days=3)
        return [
            {
                'home_team': 'FC Bayern München',
                'away_team': 'Borussia Dortmund',
                'date': base_date.isoformat(),
                'matchday': 10
            },
            {
                'home_team': 'RasenBallsport Leipzig',
                'away_team': 'Bayer 04 Leverkusen',
                'date': base_date.isoformat(),
                'matchday': 10
            },
            {
                'home_team': 'VfB Stuttgart',
                'away_team': 'SC Freiburg',
                'date': (base_date + timedelta(hours=2)).isoformat(),
                'matchday': 10
            },
            {
                'home_team': 'Eintracht Frankfurt',
                'away_team': 'Borussia Mönchengladbach',
                'date': (base_date + timedelta(days=1)).isoformat(),
                'matchday': 10
            },
            {
                'home_team': 'VfL Wolfsburg',
                'away_team': '1. FC Union Berlin',
                'date': (base_date + timedelta(days=1, hours=2)).isoformat(),
                'matchday': 10
            }
        ]

    def get_elo_ratings(self) -> Dict[str, float]:
        """Fetch ELO ratings from clubelo.com."""
        print("🎯 Fetching ELO ratings...")

        if self.demo_mode:
            print("  (Using demo data)")
            return self._get_example_elo()

        try:
            url = "http://api.clubelo.com/GER"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            elo_data = {}
            data = response.json()

            # Get latest ELO for each team
            teams_seen = set()
            for entry in reversed(data):  # Start from most recent
                team = entry['Club']
                if team not in teams_seen:
                    elo_data[team] = round(entry['Elo'], 1)
                    teams_seen.add(team)

            print(f"✓ Found ELO ratings for {len(elo_data)} teams")
            return elo_data

        except Exception as e:
            print(f"⚠ Error fetching ELO ratings: {e}")
            return self._get_example_elo()

    def _get_example_elo(self) -> Dict[str, float]:
        """Return example ELO ratings if API fails."""
        return {
            'Bayern Munich': 1850.5,
            'Dortmund': 1780.3,
            'Leipzig': 1795.2,
            'Leverkusen': 1765.8,
            'Frankfurt': 1720.4,
            'Freiburg': 1695.1,
            'Stuttgart': 1705.8,
            'Union Berlin': 1680.2,
            'Wolfsburg': 1665.5,
            'Gladbach': 1660.3,
            'Hoffenheim': 1655.7,
            'Mainz': 1640.2,
            'Augsburg': 1625.8,
            'Bremen': 1620.4,
            'Bochum': 1610.1,
            'Köln': 1605.3,
            'Heidenheim': 1595.7,
            'Darmstadt': 1580.2
        }

    def get_xg_data(self) -> Dict[str, Dict]:
        """Fetch xG (Expected Goals) data from FBref."""
        print("⚽ Fetching xG data...")

        if self.demo_mode:
            print("  (Using demo data)")
            return self._get_example_xg()

        try:
            url = "https://fbref.com/en/comps/20/Bundesliga-Stats"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            xg_data = {}

            # Find the shooting table
            tables = soup.find_all('table')
            for table in tables:
                if table.get('id') and 'shooting' in table.get('id', '').lower():
                    rows = table.find('tbody').find_all('tr')
                    for row in rows:
                        team_cell = row.find('th', {'data-stat': 'team'})
                        if team_cell:
                            team_name = team_cell.get_text(strip=True)

                            xg = row.find('td', {'data-stat': 'xg_per_90'})
                            xga = row.find('td', {'data-stat': 'xg_against_per_90'})

                            if xg and xga:
                                xg_data[team_name] = {
                                    'xG_per_90': float(xg.get_text(strip=True) or 0),
                                    'xGA_per_90': float(xga.get_text(strip=True) or 0)
                                }

            if xg_data:
                print(f"✓ Found xG data for {len(xg_data)} teams")
                return xg_data
            else:
                raise Exception("No xG data found in table")

        except Exception as e:
            print(f"⚠ Error fetching xG data: {e}")
            return self._get_example_xg()

    def _get_example_xg(self) -> Dict[str, Dict]:
        """Return example xG data if scraping fails."""
        return {
            'Bayern Munich': {'xG_per_90': 2.3, 'xGA_per_90': 0.9},
            'Dortmund': {'xG_per_90': 2.1, 'xGA_per_90': 1.2},
            'Leipzig': {'xG_per_90': 2.0, 'xGA_per_90': 1.0},
            'Leverkusen': {'xG_per_90': 2.2, 'xGA_per_90': 1.1},
            'Frankfurt': {'xG_per_90': 1.8, 'xGA_per_90': 1.3},
            'Freiburg': {'xG_per_90': 1.7, 'xGA_per_90': 1.2},
            'Stuttgart': {'xG_per_90': 1.9, 'xGA_per_90': 1.4},
            'Union Berlin': {'xG_per_90': 1.5, 'xGA_per_90': 1.3},
            'Wolfsburg': {'xG_per_90': 1.6, 'xGA_per_90': 1.4},
            'Gladbach': {'xG_per_90': 1.7, 'xGA_per_90': 1.5},
            'Hoffenheim': {'xG_per_90': 1.6, 'xGA_per_90': 1.6},
            'Mainz': {'xG_per_90': 1.4, 'xGA_per_90': 1.7},
            'Augsburg': {'xG_per_90': 1.3, 'xGA_per_90': 1.8},
            'Bremen': {'xG_per_90': 1.4, 'xGA_per_90': 1.7},
            'Bochum': {'xG_per_90': 1.2, 'xGA_per_90': 1.9},
            'Köln': {'xG_per_90': 1.3, 'xGA_per_90': 1.8},
            'Heidenheim': {'xG_per_90': 1.4, 'xGA_per_90': 1.6},
            'Darmstadt': {'xG_per_90': 1.1, 'xGA_per_90': 2.0}
        }

    def get_squad_values(self) -> Dict[str, float]:
        """Fetch squad market values from Transfermarkt."""
        print("💰 Fetching squad values...")

        if self.demo_mode:
            print("  (Using demo data)")
            return self._get_example_squad_values()

        try:
            url = "https://www.transfermarkt.com/bundesliga/startseite/wettbewerb/L1"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            squad_values = {}

            # Find team value information
            items = soup.find_all('td', {'class': 'hauptlink'})
            for item in items:
                team_link = item.find('a')
                if team_link:
                    team_name = team_link.get_text(strip=True)

                    # Find the value cell (typically in the same row)
                    row = item.find_parent('tr')
                    if row:
                        value_cell = row.find('td', {'class': 'rechts'})
                        if value_cell:
                            value_text = value_cell.get_text(strip=True)
                            # Parse value (e.g., "500.00m €" or "50.00m €")
                            value = self._parse_transfermarkt_value(value_text)
                            if value:
                                squad_values[team_name] = value

            if squad_values:
                print(f"✓ Found squad values for {len(squad_values)} teams")
                return squad_values
            else:
                raise Exception("No squad values found")

        except Exception as e:
            print(f"⚠ Error fetching squad values: {e}")
            return self._get_example_squad_values()

    def _parse_transfermarkt_value(self, value_text: str) -> Optional[float]:
        """Parse Transfermarkt value string to float (in millions)."""
        try:
            # Remove € and spaces
            value_text = value_text.replace('€', '').replace(' ', '')

            if 'bn' in value_text or 'Mrd' in value_text:
                # Billions
                number = float(re.findall(r'[\d.]+', value_text)[0])
                return number * 1000
            elif 'm' in value_text or 'Mio' in value_text:
                # Millions
                number = float(re.findall(r'[\d.]+', value_text)[0])
                return number
            elif 'k' in value_text or 'Th' in value_text:
                # Thousands
                number = float(re.findall(r'[\d.]+', value_text)[0])
                return number / 1000
            return None
        except:
            return None

    def _get_example_squad_values(self) -> Dict[str, float]:
        """Return example squad values if scraping fails."""
        return {
            'Bayern Munich': 850.5,
            'Dortmund': 520.3,
            'Leipzig': 480.2,
            'Leverkusen': 445.8,
            'Frankfurt': 280.4,
            'Freiburg': 195.5,
            'Stuttgart': 310.7,
            'Union Berlin': 125.3,
            'Wolfsburg': 215.8,
            'Gladbach': 185.2,
            'Hoffenheim': 220.5,
            'Mainz': 135.8,
            'Augsburg': 105.2,
            'Bremen': 155.6,
            'Bochum': 85.4,
            'Köln': 95.7,
            'Heidenheim': 45.3,
            'Darmstadt': 38.2
        }

    def get_injuries(self, team_name: str) -> List[str]:
        """Fetch injury information for a specific team."""
        try:
            # Simplified - in production you'd scrape Transfermarkt injury pages
            # This would require team-specific URLs
            return []
        except Exception as e:
            return []

    def _fuzzy_match_team(self, team_name: str, team_dict: Dict) -> Optional[any]:
        """Fuzzy match team name against dictionary keys."""
        team_lower = team_name.lower()

        # Remove common prefixes/suffixes for better matching
        team_clean = team_lower.replace('fc ', '').replace('1. ', '').replace('sv ', '').replace('vfl ', '').replace('vfb ', '').replace('tsg ', '')

        # Try exact match first
        for key in team_dict.keys():
            if team_lower == key.lower():
                return team_dict[key]

        # Try partial match - order matters (check more specific first)
        for key in team_dict.keys():
            key_lower = key.lower()
            key_clean = key_lower.replace('fc ', '').replace('1. ', '').replace('sv ', '').replace('vfl ', '').replace('vfb ', '').replace('tsg ', '').replace('borussia ', '')

            # Check Gladbach first to avoid confusion with other Borussias
            if ('gladbach' in team_clean or 'mönchengladbach' in team_clean) and 'gladbach' in key_clean:
                return team_dict[key]

        # Now check other teams
        for key in team_dict.keys():
            key_lower = key.lower()
            key_clean = key_lower.replace('fc ', '').replace('1. ', '').replace('sv ', '').replace('vfl ', '').replace('vfb ', '').replace('tsg ', '').replace('borussia ', '')

            # Check if key words match
            if 'bayern' in team_clean and 'bayern' in key_clean:
                return team_dict[key]
            if 'dortmund' in team_clean and 'dortmund' in key_clean:
                return team_dict[key]
            if 'leipzig' in team_clean and 'leipzig' in key_clean:
                return team_dict[key]
            if 'leverkusen' in team_clean and 'leverkusen' in key_clean:
                return team_dict[key]
            if 'frankfurt' in team_clean and 'frankfurt' in key_clean:
                return team_dict[key]
            if 'freiburg' in team_clean and 'freiburg' in key_clean:
                return team_dict[key]
            if 'stuttgart' in team_clean and 'stuttgart' in key_clean:
                return team_dict[key]
            if 'union' in team_clean and 'union' in key_clean:
                return team_dict[key]
            if 'wolfsburg' in team_clean and 'wolfsburg' in key_clean:
                return team_dict[key]
            if 'hoffenheim' in team_clean and 'hoffenheim' in key_clean:
                return team_dict[key]
            if 'mainz' in team_clean and 'mainz' in key_clean:
                return team_dict[key]
            if 'köln' in team_clean and 'köln' in key_clean:
                return team_dict[key]
            if 'augsburg' in team_clean and 'augsburg' in key_clean:
                return team_dict[key]
            if 'bochum' in team_clean and 'bochum' in key_clean:
                return team_dict[key]
            if 'bremen' in team_clean and 'bremen' in key_clean:
                return team_dict[key]
            if 'heidenheim' in team_clean and 'heidenheim' in key_clean:
                return team_dict[key]
            if 'darmstadt' in team_clean and 'darmstadt' in key_clean:
                return team_dict[key]

        return None

    def analyze_match(self, home_team: str, away_team: str,
                     elo_ratings: Dict, xg_data: Dict,
                     squad_values: Dict) -> Dict:
        """Analyze a single match based on available data."""

        analysis = {
            'home_team': home_team,
            'away_team': away_team,
            'home_elo': None,
            'away_elo': None,
            'elo_diff': None,
            'home_xg': None,
            'away_xg': None,
            'home_value': None,
            'away_value': None,
            'prediction': None
        }

        # Find ELO ratings using fuzzy matching
        analysis['home_elo'] = self._fuzzy_match_team(home_team, elo_ratings)
        analysis['away_elo'] = self._fuzzy_match_team(away_team, elo_ratings)

        if analysis['home_elo'] and analysis['away_elo']:
            analysis['elo_diff'] = round(analysis['home_elo'] - analysis['away_elo'], 1)

        # Find xG data using fuzzy matching
        analysis['home_xg'] = self._fuzzy_match_team(home_team, xg_data)
        analysis['away_xg'] = self._fuzzy_match_team(away_team, xg_data)

        # Find squad values using fuzzy matching
        analysis['home_value'] = self._fuzzy_match_team(home_team, squad_values)
        analysis['away_value'] = self._fuzzy_match_team(away_team, squad_values)

        # Simple prediction based on available metrics
        analysis['prediction'] = self._predict_outcome(analysis)

        return analysis

    def _predict_outcome(self, analysis: Dict) -> str:
        """Simple prediction based on available metrics."""
        home_score = 0
        away_score = 0

        # ELO advantage
        if analysis['elo_diff']:
            if analysis['elo_diff'] > 50:
                home_score += 2
            elif analysis['elo_diff'] > 20:
                home_score += 1
            elif analysis['elo_diff'] < -50:
                away_score += 2
            elif analysis['elo_diff'] < -20:
                away_score += 1

        # xG advantage
        if analysis['home_xg'] and analysis['away_xg']:
            home_xg_attack = analysis['home_xg'].get('xG_per_90', 0)
            away_xg_attack = analysis['away_xg'].get('xG_per_90', 0)

            if home_xg_attack > away_xg_attack + 0.3:
                home_score += 1
            elif away_xg_attack > home_xg_attack + 0.3:
                away_score += 1

        # Squad value advantage
        if analysis['home_value'] and analysis['away_value']:
            if analysis['home_value'] > analysis['away_value'] * 1.5:
                home_score += 1
            elif analysis['away_value'] > analysis['home_value'] * 1.5:
                away_score += 1

        # Home advantage
        home_score += 1

        if home_score > away_score + 1:
            return f"Clear Home Win ({home_score - away_score})"
        elif home_score > away_score:
            return "Home Win"
        elif away_score > home_score + 1:
            return f"Clear Away Win ({away_score - home_score})"
        elif away_score > home_score:
            return "Away Win"
        else:
            return "Draw"

    def format_analysis(self, match: Dict, analysis: Dict) -> str:
        """Format analysis results for display."""
        output = []
        output.append("=" * 80)
        output.append(f"🏟️  {analysis['home_team']} vs {analysis['away_team']}")
        output.append(f"📅 Date: {match['date'][:10]} | Matchday: {match['matchday']}")
        output.append("-" * 80)

        # ELO Section
        output.append("ELO RATINGS:")
        if analysis['home_elo'] and analysis['away_elo']:
            output.append(f"  Home: {analysis['home_elo']:.1f} | Away: {analysis['away_elo']:.1f}")
            output.append(f"  Difference: {analysis['elo_diff']:+.1f} (Home advantage)")
        else:
            output.append("  Data not available")

        # xG Section
        output.append("\nEXPECTED GOALS (xG per 90):")
        if analysis['home_xg'] and analysis['away_xg']:
            output.append(f"  Home Attack: {analysis['home_xg']['xG_per_90']:.2f} | Defense: {analysis['home_xg']['xGA_per_90']:.2f}")
            output.append(f"  Away Attack: {analysis['away_xg']['xG_per_90']:.2f} | Defense: {analysis['away_xg']['xGA_per_90']:.2f}")
        else:
            output.append("  Data not available")

        # Squad Value Section
        output.append("\nSQUAD VALUES:")
        if analysis['home_value'] and analysis['away_value']:
            output.append(f"  Home: €{analysis['home_value']:.1f}M | Away: €{analysis['away_value']:.1f}M")
        else:
            output.append("  Data not available")

        # Prediction
        output.append("\n" + "=" * 80)
        output.append(f"📊 PREDICTION: {analysis['prediction']}")
        output.append("=" * 80)
        output.append("")

        return "\n".join(output)

    def run_analysis(self):
        """Run the complete analysis."""
        print("\n🇩🇪 BUNDESLIGA MATCH ANALYZER")
        print("=" * 80)
        print()

        # Fetch all data
        matches = self.get_upcoming_matches()
        time.sleep(1)

        elo_ratings = self.get_elo_ratings()
        time.sleep(1)

        xg_data = self.get_xg_data()
        time.sleep(1)

        squad_values = self.get_squad_values()
        time.sleep(1)

        print("\n" + "=" * 80)
        print("ANALYSIS RESULTS")
        print("=" * 80)
        print()

        # Analyze each match
        for match in matches:
            analysis = self.analyze_match(
                match['home_team'],
                match['away_team'],
                elo_ratings,
                xg_data,
                squad_values
            )

            print(self.format_analysis(match, analysis))

        print("\n✓ Analysis complete!")
        print("\nNote: Predictions are based on statistical models and should not be")
        print("used for betting purposes. Actual results may vary significantly.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Bundesliga Match Analyzer - Analyze upcoming matches using ELO, xG, and squad values'
    )
    parser.add_argument(
        '--demo',
        action='store_true',
        help='Use demo data instead of fetching from live sources (recommended due to API restrictions)'
    )
    parser.add_argument(
        '--live',
        action='store_true',
        help='Attempt to fetch live data from web sources (may fail due to rate limiting)'
    )

    args = parser.parse_args()

    # Default to demo mode if neither flag is specified
    demo_mode = args.demo or not args.live

    if demo_mode:
        print("ℹ️  Running in DEMO mode with example data")
        print("   Use --live flag to attempt fetching real data\n")
    else:
        print("ℹ️  Running in LIVE mode - attempting to fetch real data")
        print("   Note: Some sources may block requests. Use --demo for reliable output\n")

    analyzer = BundesligaAnalyzer(demo_mode=demo_mode)
    analyzer.run_analysis()


if __name__ == "__main__":
    main()
