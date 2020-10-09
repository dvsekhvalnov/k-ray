# K-Ray
Generic web frontend to view, search, explore and visualize keys, headers and payloads flying through your Kafka broker.
Fancy project name was derived from words: Kafka + X-Ray.

## Status
Experimental, born from hackthon project. Mostly useful as personal development tool.
But we also have been running it within 'test' environment for couple years.

## Idea & history
### Motivation
1. Need a better tool to have insights on what's going on with kafka during development or testing stage
(may be production too)
    - troubleshoot basic stuff (why bad data, what's getting written to topic)
    - examine topics (especially debugging kafka streams, why we have x20 more message than from source)
    - onboard new team members quicker by visually
    - everything else :)

2. Standalone command line tools never enough:
    - have to combine them to get something meaningul about data,
    - ending up writing custom python/ruby/bash/whatever in 90% of real cases
    - hard to explain 'black woodoo magic' to other people

just take a look how bad it can become very quickly:
```
kafkacat -L -b internal-test-cluster-alpha-17312321.us-east-1.elb.amazonaws.com:9092
-X security.protocol=SSL
-X ssl.ca.location=/var/certs/kafka-test-cluster/ca.cer
| grep topic
| awk '{print $2}'
| sed 's/\"//g'
| awk '/summary.daily|summary.60m|metrics.daily|metrics.60m/'
| xargs -I {} /usr/local/kafka/bin/kafka-topics.sh
--zookeeper 172.16.1.10:2181,172.16.1.98:2181,172.16.1.44:2181 --delete --topic {}
```

So, wouldn't it be nice to have a tool with modern web ui where you can explore topics, messages and all other details during your development work?

### Goals
We started the tool with following goals in mind:
1. Single binary (drop & run) server, can run locally or on broker node, no complex installation.
2. Docker friendly (run anywhere)
3. Broker auto discovery, zero configuration.
4. Slick UI, ability to view structured payloads as json or avro. Different representations for binary payloads (as hex dumps, integeres, e.t.c.)
5. Extensible enrichments (plugins) to provide special visualization meaning for your specific data

## Architecture
The main idea is to capture every single message from kafka broker and store in embeddable key-value database.
Project is leveraging awesome BadgerDB by https://dgraph.io/ as persistent layer: https://github.com/dgraph-io/badger

Bird eye view is shown on diagram below:

<img src="https://github.com/dvsekhvalnov/web-static-content/blob/master/k-ray/k-ray-architecture.png?raw=true" width="600" alt="Adjust timeline" />

## Show me how it looks like?
Please checkout screenshots from UI interface in frontend part of project: https://github.com/dvsekhvalnov/k-ray-ui

## How to build it
### Structure
The project is consist of 3 different repos:
1. Go sync library for kafka: https://github.com/dvsekhvalnov/sync4kafka
2. Backend engine, storage and REST API (given project): https://github.com/dvsekhvalnov/k-ray
3. Frontend SPA part living at: https://github.com/dvsekhvalnov/k-ray-ui

### Build steps
1. clone backend repo (https://github.com/dvsekhvalnov/k-ray)
2. clone frontend repo from (https://github.com/dvsekhvalnov/k-ray-ui)
3. install all dependencies with `go get -u` that project is complaining about
4. run `./build.sh` to produce combined backend + frontend binary
5. adjust `./k-ray.yaml` as needed
6. start it with `go run main/main.go`
7. access it on http://localhost:8080/

## Maintainers wanted
As with many former hackaton projects nobody actively working on it fulltime. If somebody from
community have interest in supporting or evolving it please drop me a message i whould be happy
if project gets second chance.

We also appreciate fancy Mascot for the project ;)



