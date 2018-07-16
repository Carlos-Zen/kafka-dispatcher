package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/urfave/cli"
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
	"io"
	"bytes"
)

const (
	ConfigFile = "config.yml"
)

type Config struct {
	ConsumerGroup string  `yaml:"consumer_group"`
	Topics        *string `yaml:"topics"`
	Zookeeper     *string `yaml:"zookeeper"`
	Command       string  `yaml:"command"`
	Params        string  `yaml:"params"`
	ProcessNum    int     `yaml:"process_number"`
	BuffNum       int     `yaml:"buff_number"`
	OffsetStart   int64   `yaml:"offset_start"`
	OffsetEnd     int64   `yaml:"offset_end"`
	LogFile       string  `yaml:"log_path"`
}

var (
	c               Config
	zookeeperNodes  []string
	logger          *log.Logger
	OffsetStartFlag = cli.Uint64Flag{
		Name:  "offsetstart",
		Usage: "The number of kafka pull offset for start.",
	}
	OffsetEndFlag = cli.Uint64Flag{
		Name:  "offsetend",
		Usage: "The number of offset which control the end point has consumed.",
		Value: 0,
	}
	GroupFlag = cli.StringFlag{
		Name:  "group",
		Usage: "The consumer group.",
	}
)
var msg = make(chan string, 300)

// init yaml config file.
// init sarama logger.
func init() {
	c.getConf()
	rotateLogger()
}

func rotateLogger(){
	var out io.Writer
	if c.LogFile != "" {
		fileName := c.LogFile
		var buffer bytes.Buffer
		segs := strings.Split(fileName,".")
		buffer.WriteString(segs[0])
		buffer.WriteString(fmt.Sprintf("%v",time.Now().Format("2006-01-02")))
		buffer.WriteString(".")
		buffer.WriteString(segs[1])
		trueFile := buffer.String()
		fmt.Println(trueFile)
		file,err := os.Create(trueFile)
		if err != nil {
			log.Fatalln("open file error")
		}
		out = file
	} else {
		out = os.Stdout
	}
	sarama.Logger = log.New(out, "[Kcher] ", log.LstdFlags)
	logger = log.New(out, "[Kcher]", log.LstdFlags)
}

// ticker count
func tickerCall(method func()) {
	ticker := time.NewTicker(30 * time.Minute)
	for {
		select {
		case <- ticker.C:
			method()
		}
	}
}

func setupAPP() *cli.App {
	app := cli.NewApp()
	app.Name = "kcher"
	app.Usage = "kafka dispatcher"
	app.Action = startDispatcher
	app.Version = "1.0"
	app.Copyright = "Copyright in 2018 The Carlos Authors"
	//app.Commands = []cli.Command{
	//	cmd.AccountCommand,
	//}
	app.Flags = []cli.Flag{
		//common setting
		GroupFlag,
		OffsetStartFlag,
		OffsetEndFlag,
	}
	return app
}

func main() {
	if err := setupAPP().Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func startDispatcher(cnx *cli.Context) {
	if cnx.GlobalIsSet(OffsetStartFlag.Name) {
		c.OffsetStart = cnx.Int64(OffsetStartFlag.Name)
	}
	if cnx.GlobalIsSet(OffsetEndFlag.Name) {
		c.OffsetEnd = cnx.Int64(OffsetEndFlag.Name)
	}
	if cnx.GlobalIsSet(GroupFlag.Name) {
		c.ConsumerGroup = cnx.String(GroupFlag.Name)
	}
	commandProcessStart()
	consumeStart()
	go tickerCall(rotateLogger)
}

func (c *Config) getConf() *Config {

	yamlFile, err := ioutil.ReadFile(ConfigFile)
	if err != nil {
		logger.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		logger.Fatalf("Unmarshal: %v", err)
	}

	return c
}

// consumer start and push message to chan.
func consumeStart() {
	if *c.Zookeeper == "" {
		logger.Fatalln("Zookeeper Config is invalid.")
		os.Exit(1)
	}

	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest

	if c.OffsetStart > 0 {
		config.Offsets.Initial = c.OffsetStart
	}

	config.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(*c.Zookeeper)

	kafkaTopics := strings.Split(*c.Topics, ",")

	consumer, consumerErr := consumergroup.JoinConsumerGroup(c.ConsumerGroup, kafkaTopics, zookeeperNodes, config)
	if consumerErr != nil {
		logger.Fatalln(consumerErr)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		if err := consumer.Close(); err != nil {
			sarama.Logger.Println("Error closing the consumer", err)
		}
	}()

	go func() {
		for err := range consumer.Errors() {
			logger.Fatalln(err)
		}
	}()

	eventCount := 0
	offsets := make(map[string]map[int32]int64)

	for message := range consumer.Messages() {
		// if offset beyond the offsetEnd number , exit.
		if c.OffsetEnd > 0 && message.Offset > c.OffsetEnd {
			logger.Println("Offset beyond the end offset point.")
			os.Exit(1)
		}
		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount += 1
		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
			logger.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
		}

		offsets[message.Topic][message.Partition] = message.Offset
		msg <- string(message.Value)
		consumer.CommitUpto(message)
		logger.Printf("Value:%s,Partition:%v, Offset:%v", message.Value, message.Partition, message.Offset)
	}

	logger.Printf("Processed %d events.", eventCount)
}

// start all processes.
func commandProcessStart() {
	// init process
	for i := 0; i < c.ProcessNum; i++ {
		go commandProcess()
	}
}

func commandProcess() {
	for {
		start := time.Now().Unix()
		dateCmd := exec.Command(c.Command, c.Params, <-msg)
		dateOut, err := dateCmd.Output()
		if err != nil {
			logger.Printf("Error :%s \n", err)
		}
		logger.Printf("Cost Time:%d , Command Output:%s", time.Now().Unix()-start, string(dateOut))
	}
}
