//
// boltdbweb is a webserver base GUI for interacting with BoltDB databases.
//
// For authorship see https://github.com/evnix/boltdbweb
// MIT license is included in repository
//
package main

// go get -u github.com/jteeuwen/go-bindata/...
// go get github.com/elazarl/go-bindata-assetfs/...

//go:generate go-bindata-assetfs -pkg webb -o webs/web_static.go assets/...

import (
	"flag"
	"github.com/gin-gonic/gin"
	"github.com/jiuzhou-zhao/bolt-server/dbs"
	"github.com/jiuzhou-zhao/bolt-server/webs"
)

func main() {
	address := flag.String("listen", ":12311", "listening address")
	flag.Parse()

	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	webs.Register(r)
	dbs.Register(r)

	_ = r.Run(*address)
}
