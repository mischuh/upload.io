from invoke import task, Collection

from tasks.config import LOCAL_IMAGE_NAME


@task
def build(ctx):
    """Build the docker image."""
    ctx.run(
        'docker build'
        '  -t {}'
        '  --build-arg VCS_REF=`git rev-parse --short HEAD`'
        '  --build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"`'
        '  .'.format(LOCAL_IMAGE_NAME)
    )


# @task(build)
# def publish(ctx, tag):
#     ctx.run('docker tag {local} {public}:{tag}'.format(local=LOCAL_IMAGE_NAME, public=PUBLIC_IMAGE_NAME, tag=tag))
#     ctx.run('docker push {public}:{tag}'.format(public=PUBLIC_IMAGE_NAME, tag=tag))
#     ctx.run('docker tag {local} {public}:latest'.format(local=LOCAL_IMAGE_NAME, public=PUBLIC_IMAGE_NAME))
#     ctx.run('docker push {public}:latest'.format(public=PUBLIC_IMAGE_NAME))


docker = Collection()
docker.add_task(build)
# docker.add_task(publish)
