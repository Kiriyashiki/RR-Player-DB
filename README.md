# RR Player DB

Pulls data from the [RWFC API](http://rwfc.net/api/groups) and aggregates it in a `SQLite3` database.<br>
The data is then exposed via API endpoint.

## Endpoints

- `/player?pid=[int] | /player?fc=[str]` - Obtain player data by PID or Friend Code
- `/leaderboard?start=<int>&end=<int>&q=<str>` - Obtain the leaderboard. Optional start/end to obtain a specific chunk (
  default 100 first). Optional query to filter players by name or friend code.
- `/vrhistory/<int>` - Obtain the VR history log for a specified PID.
- `/updatePlayer?pid=[int]&key=[str]&ban=[0|1]&rizz=[0|1]` - Updates a player's ban or name censor status.
- `/load_json?key=[str]` - Loads `rr-players.json`
  from [ImpactCoding/rr-player-database](https://github.com/ImpactCoding/rr-player-database). JSON file must be placed
  at project root directory.

*Endpoints with `key` require the `ADMIN_KEY` set in `.env`.*

## Run

First copy `.env.example` to `.env` and configure the variables.

Then run `pip install -r requirements.txt`

Then `gunicorn --config gunicorn_config.py app:app`

And it will run by default of localhost port 8338
