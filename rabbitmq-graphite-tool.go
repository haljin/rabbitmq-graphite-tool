package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/marpaia/graphite-golang"
	"github.com/streadway/amqp"
)

func rabbitmqConnect(uri string, queueName string) (queueConnection *amqp.Connection, queueChannel *amqp.Channel, err error) {
	queueConnection, err = amqp.Dial(uri)
	if err == nil {
		queueChannel, err = queueConnection.Channel()
	}
	return
}

func nonFatalError(msg string, err error, pauseMsec int) bool {
	if err == nil {
		return false
	}
	log.Printf("non-fatal error - %s: %s", msg, err)
	time.Sleep(time.Millisecond * time.Duration(pauseMsec))
	return true
}

func fetchUrl(requestUrl string) (body []byte, statusCode int, err error) {
	resp, err := http.Get(requestUrl)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	statusCode = resp.StatusCode
	body, err = ioutil.ReadAll(resp.Body)
	return
}

func findObject(query string, obj interface{}) (item interface{}) {
	if reflect.ValueOf(obj).Kind() != reflect.Map {
		return
	}
	i := strings.Index(query, ".")
	objMap := obj.(map[string]interface{})
	if i == -1 {
		item, _ = objMap[query]
	} else {
		item = findObject(query[i+1:], objMap[query[:i]])
	}
	return
}

func findNumber(query string, obj interface{}) (result float64) {
	item := findObject(query, obj)
	if item != nil {
		result = item.(float64)
	}
	return
}

func findString(query string, obj interface{}) (result string) {
	item := findObject(query, obj)
	if item != nil {
		result = item.(string)
	}
	return
}

func fetchQueueMetrics(mgmtUri string, prefix string) (metrics []graphite.Metric) {
	url := mgmtUri + "/api/queues"
	response, statusCode, err := fetchUrl(url)
	if err != nil || statusCode != 200 {
		log.Printf("error fetch rabbiqmq queues: %d - %s", statusCode, err)
		return
	}
	var stats []interface{}
	json.Unmarshal(response, &stats)
	for _, stat := range stats {
		name := findString("name", stat)
		if name == "" {
			continue
		}
		rate_publish := findNumber("message_stats.publish_details.rate", stat)
		rate_get := findNumber("message_stats.deliver_get_details.rate", stat)
		rate_noack := findNumber("message_stats.deliver_no_ack_details.rate", stat)
		msg_ready := findNumber("messages_ready", stat)
		msg_unack := findNumber("messages_unacknowledged", stat)
		metric := graphite.Metric{Name: prefix + "queue." + name + ".rate_publish",
			Value: strconv.Itoa(int(rate_publish)), Timestamp: time.Now().Unix()}
		metrics = append(metrics, metric)
		metric = graphite.Metric{Name: prefix + "queue." + name + ".rate_get",
			Value: strconv.Itoa(int(rate_get)), Timestamp: time.Now().Unix()}
		metrics = append(metrics, metric)
		metric = graphite.Metric{Name: prefix + "queue." + name + ".rate_noack",
			Value: strconv.Itoa(int(rate_noack)), Timestamp: time.Now().Unix()}
		metrics = append(metrics, metric)
		metric = graphite.Metric{Name: prefix + "queue." + name + ".msg_ready",
			Value: strconv.Itoa(int(msg_ready)), Timestamp: time.Now().Unix()}
		metrics = append(metrics, metric)
		metric = graphite.Metric{Name: prefix + "queue." + name + ".msg_unack",
			Value: strconv.Itoa(int(msg_unack)), Timestamp: time.Now().Unix()}
		metrics = append(metrics, metric)
	}
	return
}

func fetchExchangeMetrics(mgmtUri string, prefix string) (metrics []graphite.Metric) {
	url := mgmtUri + "/api/exchanges"
	response, statusCode, err := fetchUrl(url)
	if err != nil || statusCode != 200 {
		log.Printf("error fetch rabbiqmq queues: %d - %s", statusCode, err)
		return
	}
	var stats []interface{}
	json.Unmarshal(response, &stats)
	for _, stat := range stats {
		name := findString("name", stat)
		if name == "" {
			continue
		}
		rate_in := findNumber("message_stats.publish_in_details.rate", stat)
		rate_out := findNumber("message_stats.publish_out_details.rate", stat)
		metric := graphite.Metric{Name: prefix + "exchange." + name + ".rate_in",
			Value:     strconv.Itoa(int(rate_in)),
			Timestamp: time.Now().Unix()}
		metrics = append(metrics, metric)
		metric = graphite.Metric{Name: prefix + "exchange." + name + ".rate_out",
			Value:     strconv.Itoa(int(rate_out)),
			Timestamp: time.Now().Unix()}
		metrics = append(metrics, metric)

	}
	return
}

func monitoring(graphiteHost string, graphitePort int, mgmtUri string, prefix string, rate int) {
	var err error
	graphiteConn, err := graphite.NewGraphite(graphiteHost, graphitePort)
	if err != nil {
		return
	}
	for {
		log.Printf("fetch rabbitmq stats")
		var metrics []graphite.Metric
		for _, metric := range fetchQueueMetrics(mgmtUri, prefix) {
			metrics = append(metrics, metric)
		}
		for _, metric := range fetchExchangeMetrics(mgmtUri, prefix) {
			metrics = append(metrics, metric)
		}
		for _, metric := range metrics {
			metric := graphite.Metric{Name: metric.Name, Value: metric.Value, Timestamp: metric.Timestamp}
			err = graphiteConn.SendMetric(metric)
			if err != nil {
				return
			}

		}

		time.Sleep(time.Second * time.Duration(rate))
	}
}

func main() {
	log.Printf("Welcome to rabbitmq-graphite-tool")
	var (
		mgmtUri  string
		graphite string
		prefix   string
		rate     string
		err      error
	)

	flag.StringVar(&rate, "rate", "5", "polling rate")
	flag.StringVar(&mgmtUri,
		"rabbitmq-mgmt-uri",
		"http://guest:guest@localhost:15672", "rabbitmq managment plugin address host:port")
	flag.StringVar(&graphite,
		"graphite", "localhost:2003", "graphite server address host:port")
	flag.StringVar(&prefix,
		"prefix", "rabbitmq.node01.", "prefix for rabbitmq monitoring in graphite")
	flag.Parse()

	rabbitUrl, err := url.Parse(mgmtUri)

	if err != nil {
		log.Fatalf("can't parse rabbitmq Url: %s", mgmtUri)
		return
	}

	graphiteHost, _graphitePort, err := net.SplitHostPort(graphite)

	if err != nil {
		log.Fatalf("can't parse graphite host:port: %s", graphite)
		return
	}
	graphitePort, _ := strconv.Atoi(_graphitePort)
	parsedRate, _ := strconv.Atoi(rate)

	log.Printf("rabbitmq-host:      %s", rabbitUrl.Host)
	log.Printf("graphite-addr:      %s", graphiteHost)
	log.Printf("prefix:             %s", prefix)

	for {
		log.Printf("start monitoring")
		monitoring(graphiteHost, graphitePort, mgmtUri, prefix, parsedRate)
	}
}
