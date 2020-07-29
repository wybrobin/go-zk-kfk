package zkkfk

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"strings"
)

type kafkaZkInfo struct {
	Endpoints []string `json:"endpoints"`
}

type NoLogger struct {

}

func (self NoLogger)Printf(string, ...interface{}) {

}

var gZkConn *zk.Conn

func GetKafkaAddrs(servers []string, sessionTimeout time.Duration) []string {
	if gZkConn == nil {
		var err error
		gZkConn, _, err = zk.Connect(servers, sessionTimeout, func(c *zk.Conn) {
			c.SetLogger(&NoLogger{})
		})
		if err != nil {
			panic(err)
		}
	}

	idsPath := "/brokers/ids"

	kafkaIds, _, err := gZkConn.Children(idsPath)
	if err != nil {
		panic(err)
	}

	var kfkAddr []string

	for _, kafkaId := range kafkaIds{
		bKfkInfo, _, err := gZkConn.Get(idsPath+"/"+kafkaId)
		if err != nil {
			panic(err)
		}

		kfkInfo := &kafkaZkInfo{}
		if err := json.Unmarshal(bKfkInfo, kfkInfo); err != nil {
			panic(err)
		}
		for _, addr := range kfkInfo.Endpoints {
			arr := strings.Split(addr, "//")
			kfkAddr = append(kfkAddr, arr[1])
		}
	}

	return kfkAddr
}
