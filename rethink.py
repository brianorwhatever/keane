import rethinkdb as r
import requests
import os
import subprocess

conn = r.connect(
    host='aws-us-east-1-portal.14.dblayer.com',
    port=10609,
	auth_key='CV27GKNxUIZXid1XomHdqrqKoFnRbakf9FSobOJN1WM',
	ssl={'ca_certs': './rethink.cert'}).repl()

# r.db("keane").table_create("paint_jobs").run(conn)
# r.db("keane").table("paint_jobs").index_create(
#     "priorityOrder", [r.row["priority"], r.row["created_date"]]
# ).run(conn)
# r.db("keane").table("paint_jobs").index_wait("priorityOrder").run(conn)


def paint(job):
    "This paints the doodle in the given style"
    # Grab images from S3
    output_image_name = '/home/brianorwhatever/keane/images/output.jpg'

    output_sem_image_name = '/home/brianorwhatever/keane/images/output_sem.png'
    output_sem_request = requests.get(job.get("output_sem_url"))
    with open(output_sem_image_name,'wb') as f:
        f.write(output_sem_request.content)

    painting_image_name = '/home/brianorwhatever/keane/images/painting.jpg'
    painting_request = requests.get(job.get("painting_url"))
    with open(painting_image_name,'wb') as f:
        f.write(painting_request.content)

    painting_sem_image_name = '/home/brianorwhatever/keane/images/painting_sem.png'
    painting_sem_request = requests.get(job.get("painting_sem_url"))
    with open(painting_sem_image_name,'wb') as f:
        f.write(painting_sem_request.content)

    # Do computation
    # Run as bash script or import into python?
    # TODO
    subprocess.call("python3 neural-doodle/doodle.py --style {painting} --output {output} --device=gpu0 --iterations=80".format(painting=painting_image_name, output=output_image_name),shell=True)

    # Upload image to S3
    # TODO

    # Update job
    # TODO

    # Delete local images
    os.remove(output_sem_image_name)
    os.remove(painting_image_name)
    os.remove(painting_sem_image_name)

    return

def get_next_job():
    return r.db("keane").table("paint_jobs").order_by(index="priorityOrder").limit(1).run(conn).next()

def start():
    # Grab highest priority job
    job = get_next_job()

    # call paint on the job
    paint(job)

    # grab another job
    # TODO

    # if no more jobs - wait
    # TODO

start()
