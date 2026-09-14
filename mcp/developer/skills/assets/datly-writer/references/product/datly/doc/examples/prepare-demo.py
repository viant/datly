"""Prepare a disposable source-backed demo from this exact development snapshot.
Run from the datly module. Prints the new directory; never edits the snapshot.
"""
import json
import pathlib
import shutil
import sqlite3
import subprocess
import tempfile

root = pathlib.Path.cwd().resolve()
if not (root / 'project/build/testdata/app').is_dir():
    raise SystemExit('Run from the datly snapshot module root')
demo = pathlib.Path(tempfile.mkdtemp(prefix='datly-demo-')).resolve()
fixture = root / 'project/build/testdata/app'
for package in ('records', 'hooks', 'models'):
    shutil.copytree(fixture / package, demo / package)
for source in demo.rglob('*.go'):
    source.write_text(source.read_text().replace('example.com/buildmodel', 'example.com/buildapp/models'))
shutil.copy2(root / 'go.mod', demo / 'go.mod')
shutil.copy2(root / 'go.sum', demo / 'go.sum')
manifest = json.loads(subprocess.check_output(['go', 'mod', 'edit', '-json'], cwd=root))
args = ['go', 'mod', 'edit', '-module=example.com/buildapp',
        '-require=github.com/viant/datly@v0.0.0',
        '-replace=github.com/viant/datly=' + str(root)]
for replacement in manifest.get('Replace', []):
    new, old = replacement['New'], replacement['Old']
    if not new.get('Version'):
        location = (root / new['Path']).resolve()
        if not (location / 'go.mod').is_file():
            raise SystemExit('Missing snapshot dependency: ' + old['Path'])
        key = old['Path'] + ('@' + old['Version'] if old.get('Version') else '')
        args.append('-replace=' + key + '=' + str(location))
subprocess.run(args, cwd=demo, check=True)
with sqlite3.connect(demo / 'records.db') as db:
    db.execute('CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT)')
    db.execute("INSERT INTO records VALUES (1, 'first')")
config = {'BaseDir': str(demo), 'Endpoint': {'Address': '127.0.0.1:8080'},
          'GoBootstrap': {'Packages': ['example.com/buildapp/...']},
          'Connector': 'main', 'Connectors': [{'Name': 'main', 'Driver': 'sqlite3', 'DSN': str(demo / 'records.db')}],
          'Info': {'title': 'Records demo', 'version': 'demo'}}
(demo / 'config.json').write_text(json.dumps(config, indent=2) + '\n')
print(demo)
