package gokafka

import (
	"context"
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
	"testing"
	"time"
)

func TestForAsyncSend(t *testing.T) {
	configYaml, _ := os.Open("conf/conf.yaml")
	c := Config{}
	_ = yaml.NewDecoder(configYaml).Decode(&c)
	p, err := NewProducer(c)
	if err != nil {
		t.Fatal(err)
	}
	n := time.Now()

	for i := 0; i < 100000; i++ {
		if err := p.AsyncSendMessage(&Message{
			Topic: "test",
			Value: []byte(fmt.Sprintf("hello : %d", i)),
			Key:   fmt.Sprintf("%d", i),
		}, nil); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("ok : %d", i)
		}
	}
	t.Logf("%v", time.Now().Sub(n))

}
func TestFroSyncSend(t *testing.T) {
	configYaml, _ := os.Open("conf/conf.yaml")
	c := Config{}
	_ = yaml.NewDecoder(configYaml).Decode(&c)
	p, err := NewProducer(c)
	if err != nil {
		t.Fatal(err)
	}
	n := time.Now()

	for i := 0; i < 100000; i++ {
		if err := p.SyncSendMessage(context.Background(), &Message{
			Topic: "test",
			Value: []byte(fmt.Sprintf("hello : %d", i)),
			Key:   fmt.Sprintf("%d", i),
		}); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("ok : %d", i)
		}
	}
	t.Logf("%v", time.Now().Sub(n))
}
