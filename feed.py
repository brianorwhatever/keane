import rethinkdb as r

r.connect(host='aws-us-east-1-portal.14.dblayer.com',
          port=10609,
	  auth_key='CV27GKNxUIZXid1XomHdqrqKoFnRbakf9FSobOJN1WM',
	  ssl={'ca_certs': './rethink.cert'}).repl()

cursor = r.table("authors").changes().run()
for document in cursor:
    print(document)
