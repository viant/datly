CREATE TABLE test_ledger(tenant TEXT NOT NULL,id INTEGER NOT NULL,state TEXT NOT NULL,payload_hash TEXT NOT NULL,PRIMARY KEY(tenant,id));
CREATE TABLE test_documents(tenant TEXT NOT NULL,id INTEGER NOT NULL,ledger_id INTEGER NOT NULL,title TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(tenant,id));
CREATE TABLE test_drafts(tenant TEXT NOT NULL,document_id INTEGER NOT NULL,revision INTEGER NOT NULL,fingerprint TEXT NOT NULL,content TEXT NOT NULL,PRIMARY KEY(tenant,document_id));
