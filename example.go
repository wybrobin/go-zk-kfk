package main

import (
	"fmt"
	"go-zk-kfk/zkkfk"
	"time"
)

func main() {
	kfkAddr := zkkfk.GetKafkaAddrs([]string{"127.0.0.1:2181", "127.0.0.1:2182"}, time.Second*3)
	fmt.Println(kfkAddr)
}
