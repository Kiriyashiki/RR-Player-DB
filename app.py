import json
import sqlite3
from datetime import datetime, timezone
from os import getenv, path

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request, abort
from flask_apscheduler import APScheduler

from util import round_down_to_interval, get_last_refresh

load_dotenv()

app = Flask(__name__)

FLASK_HOST = getenv('FLASK_HOST', '127.0.0.1')
FLASK_PORT = int(getenv('FLASK_PORT', 8338))
DB_PATH = getenv('DB_PATH', './rr-player-db.db')
API_URL = getenv('API_URL', 'http://rwfc.net/api/groups')
MII_API_URL = getenv('MII_API_URL', 'https://umapyoi.net/api/v1/mii')
NEW_PLAYER_BAN_CHECK = int(getenv('NEW_PLAYER_BAN_CHECK', "0"))
VR_BAN_CHECK = int(getenv('VR_BAN_CHECK', "0"))
VALID_RK = {'vs_10', 'vs_11', 'vs_12', 'vs_20', 'vs_21', 'vs_22'}
grace = int(datetime.now(timezone.utc).timestamp() * 1000)

scheduler = APScheduler()
scheduler.init_app(app)

def init_sqlite_db(db_path: str) -> None:
    if path.isfile(db_path):
        return
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE metadata
                   (
                       last_refresh INTEGER
                   )''')
    cur.execute('''
                CREATE TABLE players
                (
                    pid          TEXT PRIMARY KEY,
                    fc           TEXT,
                    eb           INTEGER,
                    ev           INTEGER,
                    name         TEXT,
                    raw_mii_data TEXT,
                    mii_data     TEXT,
                    mii_name     TEXT,
                    suspend      INTEGER,
                    lastupdated  INTEGER,
                    openhost     BOOLEAN,
                    banned       BOOLEAN,
                    rizz         BOOLEAN
                )
                ''')
    cur.execute('''
                CREATE TABLE VRHistory
                (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER,
                    pid       TEXT,
                    vr        INTEGER,
                    UNIQUE (timestamp, pid)
                )
                ''')
    conn.commit()
    conn.close()


def fetch_and_insert_from_api():
    try:
        rooms = requests.get(API_URL, timeout=10).json()
    except Exception as e:
        app.logger.error(f"API fetch failed: {e}")
        return

    players = {}
    for room in rooms:
        if room.get('type') != 'anybody' or room.get('rk') not in VALID_RK:
            continue
        players.update(room.get('players', {}))

    init_sqlite_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('BEGIN')
        cur = conn.cursor()

        pids = [p.get('pid') for p in players.values()]
        if pids:
            placeholder = ','.join('?' for _ in pids)
            cur.execute(f"""
                SELECT pid, raw_mii_data, mii_data, ev, banned, rizz, lastupdated
                FROM players 
                WHERE pid IN ({placeholder})
            """, pids)
            existing = {
                pid: {'raw': raw, 'proc': proc, 'ev': ev0, 'banned': banned0, 'rizz': rizz0,
                      'lastupdated': lastupdated0}
                for pid, raw, proc, ev0, banned0, rizz0, lastupdated0 in cur.fetchall()
            }
        else:
            existing = {}

        raw_map = {}
        to_fetch = []
        for p in players.values():
            pid = p.get('pid')
            mii_list = p.get('mii') or []
            if not mii_list:
                continue
            raw = mii_list[0]['data']
            raw_map[pid] = raw
            # only queue for API if it changed
            if existing.get(pid, {}).get('raw') != raw:
                to_fetch.append(raw)
            else:
                # reuse cached processed
                p['mii'][0]['data'] = existing[pid]['proc']

        if to_fetch:
            try:
                resp = requests.post(MII_API_URL, json=to_fetch, timeout=10)
                resp.raise_for_status()
                mii_map = resp.json()
                for p in players.values():
                    mii_list = p.get('mii') or []
                    if mii_list:
                        raw = raw_map[p.get('pid')]
                        if raw in mii_map:
                            p['mii'][0]['data'] = mii_map[raw]
            except Exception as ex:
                app.logger.error(f"Mii API failed: {ex}")

        for p in players.values():
            pid = p.get('pid')
            ev = int(p.get('ev', 0))
            if ev == 0:
                continue  # skip invalid

            prev = existing.get(pid)
            banned_flag = prev['banned'] if prev else 0
            rizz_flag = prev['rizz'] if prev else 0
            lastupdated = prev['lastupdated'] if prev else 0

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            stale = (now_ms - lastupdated) > 48 * 60 * 60 * 1000

            if banned_flag == 0 and now_ms > grace:  # avoid bans if the db died
                # new player?
                if not prev and NEW_PLAYER_BAN_CHECK == 1:
                    # first-seen: ban if VR>=15000
                    if ev >= 15000:
                        banned_flag = 1
                else:
                    # existing: only check delta if not stale
                    if not stale and VR_BAN_CHECK == 1:
                        if ev - prev['ev'] >= 1000:
                            banned_flag = 1
                    # else: skip delta check for staleness

            incoming_raw = raw_map.get(pid, None)
            # if the room API gave us null or no entry, fall back to existing raw
            raw_to_store = incoming_raw if incoming_raw is not None else prev.get('raw')

            incoming_proc = (p.get('mii') or [{}])[0].get('data')
            proc_to_store = incoming_proc if incoming_proc is not None else prev.get('proc')

            values = (
                pid,
                p.get('fc'),
                int(p.get('eb', 5000)),
                ev,
                p.get('name'),
                raw_to_store,
                proc_to_store,
                (p.get('mii') or [{}])[0].get('name'),
                int(p.get('suspend', 0)),
                now_ms,
                1 if p.get('openhost') == 'true' else 0,
                banned_flag,
                rizz_flag
            )
            cur.execute(
                '''INSERT INTO players
                   (pid, fc, eb, ev, name, raw_mii_data, mii_data, mii_name,
                    suspend, lastupdated, openhost, banned, rizz)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(pid) DO
                UPDATE SET
                    fc=excluded.fc, eb=excluded.eb, ev=excluded.ev,
                    name =excluded.name, raw_mii_data=excluded.raw_mii_data,
                    mii_data=excluded.mii_data, mii_name=excluded.mii_name,
                    suspend=excluded.suspend, lastupdated=excluded.lastupdated,
                    openhost=excluded.openhost,
                    banned= CASE WHEN players.banned=1 THEN 1 ELSE excluded.banned
                END,
                    rizz= CASE WHEN players.rizz=1 THEN 1 ELSE excluded.rizz
                END
                ''',
                values
            )

            bucket = round_down_to_interval(now_ms)
            cur.execute(
                '''INSERT INTO VRHistory(timestamp, pid, vr)
                   VALUES (?, ?, ?) ON CONFLICT(timestamp,pid) DO
                UPDATE SET vr=excluded.vr''',
                (bucket, pid, ev)
            )

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        cur.execute('DELETE FROM metadata')
        cur.execute('INSERT INTO metadata(last_refresh) VALUES(?)', (now_ms,))
        conn.commit()

    except Exception as e:
        conn.rollback()
        app.logger.error(f"DB insert failed: {e}")

    finally:
        conn.close()
           
# MAIN

conn = sqlite3.connect(DB_PATH)
try:
    cur = conn.cursor()
    cur.execute("SELECT last_refresh FROM metadata LIMIT 1")
    row = cur.fetchone()
    last_refresh = row[0] if row else 0
    if grace < last_refresh + 30 * 60 * 1000:  # if db died for < 30min, ignore
        grace = 0
        app.logger.info("No grace applied")
    else:
        grace = last_refresh + 48 * 60 * 60 * 1000  # 2 days period where no bans if db died
        app.logger.info("Grace for 2 days after last refresh")

except Exception as e:
    app.logger.error(f"Could not get last refresh: {e}")
finally:
    conn.close()
        
scheduler.add_job(func=fetch_and_insert_from_api, trigger='interval', minutes=1, id='fetch_interval')
scheduler.start()


# ----- JSON endpoints -----
@app.route('/player')
def get_player():
    pid = request.args.get('pid')
    fc = request.args.get('fc')
    if not pid and not fc:
        abort(400, 'Provide pid or fc')

    init_sqlite_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1) Fetch the player record
    if pid:
        cur.execute('SELECT * FROM players WHERE pid=?', (pid,))
    else:
        cur.execute('SELECT * FROM players WHERE fc=?', (fc,))
    row = cur.fetchone()
    if not row:
        abort(404)
    player = dict(row)

    # 2) Compute their global leaderboard position:
    #    Count how many players outrank them, then +1.
    #    We order by banned ASC, then ev DESC, then lastupdated DESC as in leaderboard.
    cur.execute(
        '''
        SELECT COUNT(*)
        FROM players
        WHERE (banned < ?)
           OR (banned = ? AND ev > ?)
           OR (banned = ? AND ev = ? AND lastupdated > ?)
        ''',
        (
            player['banned'],
            player['banned'], player['ev'],
            player['banned'], player['ev'], player['lastupdated']
        )
    )
    outrank_count = cur.fetchone()[0]
    player['position'] = outrank_count + 1

    # 3) Append last_refresh and return
    player['last_refresh'] = get_last_refresh(conn)
    conn.close()
    return jsonify(player)


@app.route('/leaderboard')
def get_leaderboard():
    # parse pagination
    try:
        start = int(request.args.get('start', 1))
        end = int(request.args.get('end', 100))
    except ValueError:
        abort(400, 'Invalid start/end')

    limit = end - start + 1
    offset = start - 1

    # optional search filter
    q = request.args.get('q')
    has_q = bool(q)
    like_q = f"%{q}%"

    init_sqlite_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # total count for pagination controls
    if has_q:
        cur.execute(
            'SELECT COUNT(*) FROM players WHERE name LIKE ? OR fc LIKE ?',
            (like_q, like_q)
        )
    else:
        cur.execute('SELECT COUNT(*) FROM players')
    total_count = cur.fetchone()[0]

    # 1) Compute global positions in a CTE
    base_sql = """
               WITH ranked AS (SELECT *,
                                      ROW_NUMBER() OVER (ORDER BY banned ASC, ev DESC, lastupdated DESC) AS position
               FROM players
                   )
               SELECT *
               FROM ranked \
               """
    params = []
    if has_q:
        base_sql += "WHERE name LIKE ? OR fc LIKE ?\n"
        params += [like_q, like_q]

    base_sql += "ORDER BY banned ASC, ev DESC, lastupdated DESC\nLIMIT ? OFFSET ?"
    params += [limit, offset]

    cur.execute(base_sql, params)
    rows = cur.fetchall()
    players = [dict(r) for r in rows]

    # --- compute VR change over last 7 days ---
    # build a list of the current-page PIDs
    pids = [p['pid'] for p in players]
    if pids:
        # cutoff = now - 7 days in ms
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        seven_days_ms = 7 * 24 * 60 * 60 * 1000
        cutoff = now_ms - seven_days_ms

        # use a sub‑query to get each pid’s earliest timestamp ≥ cutoff
        placeholder = ','.join('?' for _ in pids)
        join_sql = f"""
        WITH first_posts AS (
          SELECT
            pid,
            MIN(timestamp) AS ts
          FROM VRHistory
          WHERE timestamp >= ?
            AND pid IN ({placeholder})
          GROUP BY pid
        )
        SELECT h.pid, h.vr
        FROM VRHistory h
        JOIN first_posts f
          ON h.pid = f.pid
         AND h.timestamp = f.ts
        """
        # parameters: cutoff, then all pids
        cur.execute(join_sql, [cutoff] + pids)
        old_map = dict(cur.fetchall())
    else:
        old_map = {}

    # attach vr_change_7d
    for p in players:
        curr = p['ev']
        old = old_map.get(p['pid'])
        p['vr_change_7d'] = curr - old if old is not None else 0

    result = {
        "players": players,
        "total_count": total_count,
        "last_refresh": get_last_refresh(conn)
    }
    conn.close()
    return jsonify(result)


@app.route('/vrhistory/<int:pid>')
def get_vr_history(pid: int):
    if not pid:
        abort(400, 'Provide pid')
    init_sqlite_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        'SELECT timestamp, vr FROM VRHistory WHERE pid=? ORDER BY timestamp ASC',
        (pid,)
    )
    rows = cur.fetchall()
    history = [{'timestamp': t, 'vr': vr} for t, vr in rows]
    result = {'pid': pid, 'history': history, 'last_refresh': get_last_refresh(conn)}
    conn.close()
    return jsonify(result)


@app.route('/load_json')
def load_json():
    key = request.args.get('key')
    if not key or key != getenv('ADMIN_KEY'):
        return 'Invalid key', 403

    insert_data_from_json('./rr-players.json', DB_PATH)
    return 'OK', 200


def insert_data_from_json(json_path: str, db_path: str) -> None:
    data = json.load(open(json_path, encoding='utf-8'))
    init_sqlite_db(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        conn.execute('BEGIN')
        if 'last_refresh' in data:
            cur.execute('DELETE FROM metadata')
            cur.execute('INSERT INTO metadata (last_refresh) VALUES (?)', (int(data['last_refresh']),))
        for key, p in data.items():
            if key == 'last_refresh':
                continue
            pid = p.get('pid')
            ev = int(p.get('ev', 0))
            lastupd = int(p.get('lastupdated', 0))
            raw = ''
            mii_data = (p.get('mii') or [{}])[0].get('data')
            mii_name = (p.get('mii') or [{}])[0].get('name')
            cur.execute(
                '''INSERT INTO players(pid, fc, eb, ev, name, raw_mii_data, mii_data, mii_name, suspend, lastupdated,
                                       openhost, banned, rizz)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0) ON CONFLICT(pid) DO
                UPDATE
                    SET
                        fc=excluded.fc, eb=excluded.eb, ev=excluded.ev, name =excluded.name, raw_mii_data=players.raw_mii_data, mii_data=excluded.mii_data, mii_name=excluded.mii_name, suspend=excluded.suspend, lastupdated=excluded.lastupdated, openhost=excluded.openhost, banned=players.banned''',
                (
                    pid,
                    p.get('fc'),
                    int(p.get('eb', 0)),
                    ev,
                    p.get('name'),
                    raw,
                    mii_data,
                    mii_name,
                    int(p.get('suspend', 0)),
                    lastupd,
                    1 if p.get('openhost') == 'true' else 0,
                    1 if p.get('banned', False) else 0
                )
            )
            bucket = round_down_to_interval(lastupd)
            cur.execute(
                '''INSERT INTO VRHistory(timestamp, pid, vr)
                   VALUES (?, ?, ?) ON CONFLICT(timestamp,pid) DO
                UPDATE
                    SET vr=excluded.vr''',
                (bucket, pid, ev)
            )
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.route('/updatePlayer')
def update_player():
    key = request.args.get('key')
    if not key or key != getenv('ADMIN_KEY'):
        return 'Invalid key', 403

    pid = request.args.get('pid')
    ban = request.args.get('ban')
    rizz = request.args.get('rizz')
    if not pid:
        abort(400, 'Provide pid')
    if not ban and not rizz:
        abort(400, 'Provide ban or rizz')

    init_sqlite_db(DB_PATH)

    if rizz:
        if rizz == '0' or rizz == '1':
            conn = sqlite3.connect(DB_PATH)
            try:
                conn.execute("BEGIN")
                conn.execute(
                    'UPDATE players SET rizz = ? WHERE pid = ?',
                    (rizz, pid)
                )
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                app.logger.error(f"DB update failed: {e}")
            finally:
                conn.close()
        else:
            abort(400, 'wrong rizz')

    if ban:
        if ban == '0' or ban == '1':
            conn = sqlite3.connect(DB_PATH)
            try:
                conn.execute("BEGIN")
                conn.execute(
                    'UPDATE players SET banned = ? WHERE pid = ?',
                    (ban, pid)
                )
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                app.logger.error(f"DB update failed: {e}")
            finally:
                conn.close()
        else:
            abort(400, 'wrong ban')

    return 'OK', 200


if __name__ == '__main__':
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)
