import rethinkdb as r

conn = r.connect(
    host='aws-us-east-1-portal.14.dblayer.com',
    port=10609,
	auth_key='CV27GKNxUIZXid1XomHdqrqKoFnRbakf9FSobOJN1WM',
	ssl={'ca_certs': './rethink.cert'}).repl()

# r.db("keane").table_create("paint_jobs").run(conn)
# r.db("keane").table("paint_jobs").index_create(
#     "priorityOrder", [r.row["priority"], r.row["createdAt"]]
# ).run(conn)
# r.db("keane").table("paint_jobs").index_wait("priorityOrder").run(conn)


def paint( job ):
    "This completes the job given"
    # Grab image from S3
    # Do computation
    # Upload image to S3
    # Update job
    return

def start():
    jobs = r.db("keane").table("paint_jobs").order_by(index="priorityOrder").run(conn)
    for job in jobs:
        print(job)
    # Grab highest priority job
    # call paint on the job
    # grab another job
    # if no more jobs - wait

start()
