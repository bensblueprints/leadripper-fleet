import sqlite3, json, os, re

backup = 'F:/Leadripper Databased/fleet-production-backup-COMPLETE.db'
conn = sqlite3.connect(backup)
c = conn.cursor()

# Count total
c.execute('SELECT COUNT(*) FROM leads')
total = c.fetchone()[0]
print(f'Total leads in backup: {total:,}')

# Count empty/null states
c.execute("SELECT COUNT(*) FROM leads WHERE state = '' OR state IS NULL")
non_us = c.fetchone()[0]
print(f'Empty/null state leads: {non_us:,}')

# Sample non-empty states that aren't US states
us_states = {'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'}
c.execute("SELECT state, COUNT(*) as c FROM leads WHERE state != '' AND state IS NOT NULL GROUP BY state ORDER BY c DESC LIMIT 50")
print('\nAll non-empty states (check for non-US):')
for row in c.fetchall():
    flag = 'US' if row[0].upper() in us_states else '** NON-US **'
    print(f'  {row[0]}: {row[1]:,}  {flag}')

# Sample some empty-state leads to identify countries
c.execute("SELECT city, state, industry, address, phone FROM leads WHERE state = '' OR state IS NULL LIMIT 50")
print('\nSample empty-state leads:')
for row in c.fetchall():
    print(f'  city={row[0]}, state={row[1]}, industry={row[2]}, addr={row[3]}, phone={row[4]}')

conn.close()
