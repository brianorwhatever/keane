import rethinkdb as r

conn = r.connect(host='aws-us-east-1-portal.14.dblayer.com',
          port=10609,
	  auth_key='CV27GKNxUIZXid1XomHdqrqKoFnRbakf9FSobOJN1WM',
	  ssl={'ca_certs': './rethink.cert'}).repl()

job = r.db("keane").table("paint_jobs").insert({
    "status": "queued",
    "priority": 1,
    "painting_url": "https://s3-us-west-2.amazonaws.com/bigeyeskeane/paintings/Monet.jpg",
    "painting_sem_url": "https://s3-us-west-2.amazonaws.com/bigeyeskeane/paintings/Monet_sem.png",
    "output_sem_url": "https://s3-us-west-2.amazonaws.com/bigeyeskeane/input/Landscape_sem.png",
    "type": "doodle",
    "created_date": r.now()
}, return_changes=True).run(conn)

print(job)
