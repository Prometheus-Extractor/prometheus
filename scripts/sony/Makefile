#PROXY_HOST ?=10.129.250.100
PROXY_HOST ?=127.0.0.1
PROXY_PORT ?=3128

DOCKER_REGISTRY=prometheus
DOCKER_REPOSITORY =relation_extractor
DOCKER_TAG=latest
DOCKER_APP_NAME=prometheus-relation-extractor

all: docker-run

docker-build:
	docker build \
		--network="host" \
		--build-arg http_proxy=http://$(PROXY_HOST):$(PROXY_PORT) \
		--build-arg https_proxy=http://$(PROXY_HOST):$(PROXY_PORT) \
		--build-arg HTTP_PROXY=http://$(PROXY_HOST):$(PROXY_PORT) \
		--build-arg HTTPS_PROXY=http://$(PROXY_HOST):$(PROXY_PORT) \
		--build-arg MAVEN_OPTS='-DproxySet=true -DproxyHost=$(PROXY_HOST) -DproxyPort=$(PROXY_PORT)' \
		--build-arg SBT_OPTS='-Dhttp.proxyHost=$(PROXY_HOST) -Dhttp.proxyPort=$(PROXY_PORT) -Dhttps.proxyHost=$(PROXY_HOST) -Dhttps.proxyPort=$(PROXY_PORT)' \
		-t $(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY):$(DOCKER_TAG) .

docker-upload: docker-build
	docker push $(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY):$(DOCKER_TAG)

docker-run: docker-build docker-stop
	docker run -d --rm \
		--net="host" \
		-p 8080:80 \
		-e http_proxy=http://$(PROXY_HOST):$(PROXY_PORT) \
		-e https_proxy=http://$(PROXY_HOST):$(PROXY_PORT) \
		-e HTTP_PROXY=http://$(PROXY_HOST):$(PROXY_PORT) \
		-e HTTPS_PROXY=http://$(PROXY_HOST):$(PROXY_PORT) \
		-e _JAVA_OPTIONS='-Dhttp.proxyHost=$(PROXY_HOST) -Dhttp.proxyPort=$(PROXY_PORT) -Dhttps.proxyHost=$(PROXY_HOST) -Dhttps.proxyPort=$(PROXY_PORT)' \
		--name $(DOCKER_APP_NAME) \
		$(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY):$(DOCKER_TAG)

docker-logs:
	docker logs -f $(DOCKER_APP_NAME)

docker-stop:
	docker stop $(DOCKER_APP_NAME) || echo "Not running"

.PHONY: docker-build docker-upload docker-run docker-logs docker-stop
