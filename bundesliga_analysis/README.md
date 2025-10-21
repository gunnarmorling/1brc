# Bundesliga Match Analyzer

Ein Python-Tool zur Analyse bevorstehender Bundesliga-Spiele basierend auf verschiedenen Metriken.

## Features

- **ELO-Ratings**: Aktuelle ELO-Bewertungen aller Bundesliga-Teams
- **xG-Werte**: Expected Goals (xG) pro 90 Minuten, sowohl offensiv als auch defensiv
- **Kaderwerte**: Marktwerte der Mannschaften von Transfermarkt
- **Spielplan**: Bevorstehende Bundesliga-Spiele über OpenLigaDB API
- **Match-Vorhersagen**: Einfache Vorhersagen basierend auf den gesammelten Daten

## Installation

1. **Python-Abhängigkeiten installieren:**

```bash
pip install -r requirements.txt
```

## Verwendung

Das Script einfach ausführen:

```bash
python bundesliga_analyzer.py
```

oder:

```bash
python3 bundesliga_analyzer.py
```

## Datenquellen

Das Tool bezieht Daten aus folgenden Quellen:

1. **ELO-Ratings**: [ClubElo API](http://clubelo.com)
2. **xG-Daten**: [FBref](https://fbref.com) (StatsBomb Daten)
3. **Kaderwerte**: [Transfermarkt](https://www.transfermarkt.com)
4. **Spielplan**: [OpenLigaDB API](https://www.openligadb.de)

## Ausgabe-Beispiel

```
🇩🇪 BUNDESLIGA MATCH ANALYZER
================================================================================

📅 Fetching upcoming matches...
✓ Found 5 upcoming matches
🎯 Fetching ELO ratings...
✓ Found ELO ratings for 18 teams
⚽ Fetching xG data...
✓ Found xG data for 18 teams
💰 Fetching squad values...
✓ Found squad values for 18 teams

================================================================================
ANALYSIS RESULTS
================================================================================

================================================================================
🏟️  FC Bayern München vs Borussia Dortmund
📅 Date: 2025-10-24 | Matchday: 10
--------------------------------------------------------------------------------
ELO RATINGS:
  Home: 1850.5 | Away: 1780.3
  Difference: +70.2 (Home advantage)

EXPECTED GOALS (xG per 90):
  Home Attack: 2.30 | Defense: 0.90
  Away Attack: 2.10 | Defense: 1.20

SQUAD VALUES:
  Home: €850.5M | Away: €520.3M

================================================================================
📊 PREDICTION: Clear Home Win (3)
================================================================================
```

## Funktionsweise

Das Tool:

1. Ruft bevorstehende Bundesliga-Spiele ab
2. Sammelt ELO-Ratings für alle Teams
3. Holt xG-Statistiken (Expected Goals)
4. Bezieht aktuelle Kaderwerte
5. Analysiert jedes Spiel basierend auf:
   - ELO-Differenz
   - Offensive/Defensive xG-Werte
   - Kaderwert-Verhältnis
   - Heimvorteil

## Hinweise

- Das Script verwendet Web Scraping für einige Datenquellen
- Rate Limiting: Es gibt 1-Sekunden-Pausen zwischen Anfragen
- Bei Verbindungsproblemen werden Beispieldaten verwendet
- Vorhersagen sind rein statistisch und nicht für Wetten gedacht

## Mögliche Erweiterungen

- Verletzungsdaten-Integration (erfordert erweiterte Transfermarkt-Scraping)
- Formkurven der letzten 5 Spiele
- Head-to-Head Statistiken
- Wettervorhersagen
- Schiedsrichter-Statistiken
- Detaillierte Spieler-Analysen

## Fehlerbehandlung

Falls eine Datenquelle nicht erreichbar ist, verwendet das Tool Fallback-Beispieldaten.
Die Ausgabe zeigt Warnungen für fehlgeschlagene Datenabfragen.

## Lizenz

Dieses Tool ist für Bildungszwecke gedacht. Bitte respektiere die Terms of Service
der verwendeten Datenquellen.
