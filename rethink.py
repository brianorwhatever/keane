import rethinkdb as r
import requests
import os
import subprocess
import boto3
from shutil import copyfile

s3 = boto3.resource('s3')
bucket_name = 'bigeyeskeane'

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
    update_job(job.get("id"), {"status": "processing"})
    # subprocess.call("python3 neural-doodle/doodle.py --style {painting} --output {output} --device=gpu0 --iterations=80".format(painting=painting_image_name, output=output_image_name),shell=True)
    copyfile(output_sem_image_name, output_image_name)

    # Upload image to S3
    s3_image_name = 'output/doodle-{id}.jpg'.format(id=job.get("id"))
    s3_object = s3.Object(bucket_name, s3_image_name).put(ACL="public-read",Body=open(output_image_name, 'rb'))
    url = s3.meta.client.generate_presigned_url('get_object', Params = {'Bucket': bucket_name, 'Key': s3_image_name})

    # Update job
    update_job(job.get("id"), {"status": "complete", "output_url": url})

    # Delete local images
    os.remove(output_image_name)
    os.remove(output_sem_image_name)
    os.remove(painting_image_name)
    os.remove(painting_sem_image_name)

    return

def get_next_job():
    return r.db("keane").table("paint_jobs").order_by(index="priorityOrder").filter({"status": "queued"}).limit(1).run(conn).next()

def update_job(id, updates):
    return r.db("keane").table("paint_jobs").get(id).update(updates).run(conn)

def start():
    while(1):
        try:
            # Grab highest priority job
            job = get_next_job()
            # call paint on the job
            paint(job)
        except:
            feed = r.db("keane").table("paint_jobs").changes().run(conn)

start()
