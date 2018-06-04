package main

import (
	"github.com/Shopify/sarama"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"
	"uuzu.com/kafka-dispatcher/kafka/consumergroup"
	"uuzu.com/kafka-dispatcher/zoo"
)

const (
	ConfigFile = "config.yml"
)

type Config struct {
	ConsumerGroup *string `yaml:"consumer_group"`
	Topics        *string `yaml:"topics"`
	Zookeeper     *string `yaml:"zookeeper"`
	Command       string  `yaml:"command"`
	Params        string  `yaml:"params"`
	ProcessNum    int     `yaml:"process_number"`
	BuffNum       int     `yaml:"buff_number"`
}

var (
	c              Config
	zookeeperNodes []string
)
var msg = make(chan string, 300)

func init() {
	c.getConf()
	sarama.Logger = log.New(os.Stdout, "[Logger] ", log.LstdFlags)
	commandProcessStart()
}

func main() {
	consumeStart()
}

func (c *Config) getConf() *Config {

	yamlFile, err := ioutil.ReadFile(ConfigFile)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return c
}

func consumeStart() {
	if *c.Zookeeper == "" {
		log.Fatalln("Zookeeper Config is invalid.")
		os.Exit(1)
	}

	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(*c.Zookeeper)

	kafkaTopics := strings.Split(*c.Topics, ",")

	consumer, consumerErr := consumergroup.JoinConsumerGroup(*c.ConsumerGroup, kafkaTopics, zookeeperNodes, config)
	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			sarama.Logger.Println("Error closing the consumer", err)
		}
	}()

	go func() {
		for err := range consumer.Errors() {
			log.Println(err)
		}
	}()

	eventCount := 0
	offsets := make(map[string]map[int32]int64)

	for message := range consumer.Messages() {
		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount += 1
		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
			log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
		}

		offsets[message.Topic][message.Partition] = message.Offset
		msg <- string(message.Value)
		consumer.CommitUpto(message)
		log.Printf("Value %s", message.Value)
	}

	log.Printf("Processed %d events.", eventCount)
	log.Printf("%+v", offsets)
}

func commandProcessStart() {
	start := time.Now().Unix()
	var ch = make(chan int)

	// init process
	for i := 0; i < c.ProcessNum; i++ {
		go commandProcess(ch)
	}
	count := 0

	// supply process
	go func() {
		for {
			<-ch
			count += 1
			// go commandProcess(ch)
			log.Printf("cost:%d , concur:%d", time.Now().Unix()-start, (time.Now().Unix()-start)/int64(count))
		}
	}()
}

func commandProcess(ch chan int) {
	for {
		dateCmd := exec.Command(c.Command, c.Params, <-msg)
		dateOut, err := dateCmd.Output()
		if err != nil {
			log.Printf("Error :%s \n", err)
		}
		log.Printf("Command Output:%s", string(dateOut))
		ch <- 1
	}
}
