package gokafka

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
	"testing"
)

func TestConsumerAsyncCommit(t *testing.T) {
	configYaml, _ := os.Open("conf/conf.yaml")
	c := Config{}
	_ = yaml.NewDecoder(configYaml).Decode(&c)
	consumer, err := NewConsumer(c)
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.PollWithSyncCommit(func(message *Message) error {
		fmt.Println(message.Value, "----")
		//return fmt.Errorf("%s", message.Value)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConsumerSyncCommit(t *testing.T) {
	configYaml, _ := os.Open("conf/conf.yaml")
	c := Config{}
	_ = yaml.NewDecoder(configYaml).Decode(&c)
	consumer, err := NewConsumer(c)
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.PollWithSyncCommit(func(message *Message) error {
		fmt.Println(message.Value, "----")
		//return fmt.Errorf("%s", message.Value)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConsumerAutoCommit(t *testing.T) {
	configYaml, _ := os.Open("conf/conf.yaml")
	c := Config{}
	_ = yaml.NewDecoder(configYaml).Decode(&c)
	consumer, err := NewConsumer(c)
	if err != nil {
		t.Fatal(err)
	}
	m, err := consumer.PollWithAutoCommit()
	if err != nil {
		t.Fatal(err)
	}
	consumer.innerConsumer.Close()
	fmt.Println(m.Value, "----")
}
