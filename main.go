//
// boltdbweb is a webserver base GUI for interacting with BoltDB databases.
//
// For authorship see https://github.com/evnix/boltdbweb
// MIT license is included in repository
//
package main

// go get -u github.com/jteeuwen/go-bindata/...
// go get github.com/elazarl/go-bindata-assetfs/...

//go:generate go-bindata-assetfs -pkg webb -o webb/web_static.go assets/...

import (
	"github.com/gin-gonic/gin"
	"github.com/jiuzhou-zhao/bolt-server/webb"
	"github.com/jiuzhou-zhao/bolt-server/webs"
)

const version = "v0.0.0"

func main() {
	// OK, we should be ready to define/run assets server safely.
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	webb.Register(r)
	webs.Register(r)

	_ = r.Run(":12311")
}
