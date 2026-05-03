import sqlite3, os, sys

src = 'F:/Leadripper Databased/fleet-production-backup.db'
dst = 'F:/Leadripper Databased/fleet-production-backup-recovered.db'

if os.path.exists(dst):
    os.remove(dst)

# Connect to corrupted source with readonly and no mutex
src_conn = sqlite3.connect(f'file:{src}?mode=ro', uri=True)
src_conn.execute('PRAGMA read_uncommitted = 1')
src_cur = src_conn.cursor()

# Get schema
dst_conn = sqlite3.connect(dst)
dst_cur = dst_conn.cursor()

# Copy table schema
src_cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL")
for row in src_cur.fetchall():
    try:
        dst_cur.execute(row[0])
    except Exception as e:
        print(f'Schema error: {e}')

dst_conn.commit()

# Try to copy leads
src_cur.execute('SELECT * FROM leads')
cols = [d[0] for d in src_cur.description]
placeholders = ','.join('?' for _ in cols)
insert_sql = f"INSERT INTO leads ({','.join(cols)}) VALUES ({placeholders})"

count = 0
errors = 0
batch = []
while True:
    try:
        row = src_cur.fetchone()
        if row is None:
            break
        batch.append(row)
        if len(batch) >= 1000:
            dst_cur.executemany(insert_sql, batch)
            dst_conn.commit()
            count += len(batch)
            batch = []
            if count % 10000 == 0:
                print(f'Copied {count:,} rows...')
    except Exception as e:
        errors += 1
        if errors <= 5:
            print(f'Row error: {e}')
        if batch:
            for r in batch:
                try:
                    dst_cur.execute(insert_sql, r)
                except:
                    pass
            dst_conn.commit()
            count += len(batch)
            batch = []
        continue

if batch:
    try:
        dst_cur.executemany(insert_sql, batch)
        dst_conn.commit()
        count += len(batch)
    except:
        for r in batch:
            try:
                dst_cur.execute(insert_sql, r)
            except:
                pass
        dst_conn.commit()
        count += len(batch)

print(f'Done. Copied {count:,} rows. Errors: {errors}')
src_conn.close()
dst_conn.close()
