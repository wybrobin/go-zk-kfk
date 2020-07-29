package zkkfk

import (
	"github.com/samuel/go-zookeeper/zk"
)

type Conn struct {
	zkConn *zk.Conn
}
