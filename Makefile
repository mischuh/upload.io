#
# IMPORTANT:
# All automatated tasks are run by invoke.
# This Makefile only ensures compatibility with our
# linting and testing docker image.
#

.PHONY: lint test

# Docker stuff
TAG?=dev  # You can inject it from the outside
LOCAL_IMAGE_NAME=upload.io:local
# PUBLIC_IMAGE_NAME=559151583148.dkr.ecr.eu-central-1.amazonaws.com/ngmr

lint:
	inv lint

test:
	inv test

docker-build:
	@docker build \
		-t $(LOCAL_IMAGE_NAME) \
		--build-arg VCS_REF=`git rev-parse --short HEAD` \
		--build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"` \
		.

docker-push: docker-build
	docker tag $(LOCAL_IMAGE_NAME) $(PUBLIC_IMAGE_NAME):$(TAG)
	docker push $(PUBLIC_IMAGE_NAME):$(TAG)
	docker tag $(LOCAL_IMAGE_NAME) $(PUBLIC_IMAGE_NAME):latest
	docker push $(PUBLIC_IMAGE_NAME):latest