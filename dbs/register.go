package dbs

import (
	"github.com/gin-gonic/gin"
	"github.com/jiuzhou-zhao/bolt-server/pkg/boltsc"
)

var _s = NewServer()

func Register(r *gin.Engine) {
	r.POST("/"+boltsc.UriDB, func(c *gin.Context) {
		_ = _s.doResp(c.Writer, _s.handleDBRequest(c.Request.Body))
	})
	r.POST("/"+boltsc.UriTX, func(c *gin.Context) {
		_ = _s.doResp(c.Writer, _s.handleTXRequest(c.Request.Body))
	})
	r.POST("/"+boltsc.UriBucket, func(c *gin.Context) {
		_ = _s.doResp(c.Writer, _s.handleBucketRequest(c.Request.Body))
	})
	r.POST("/"+boltsc.UriCursor, func(c *gin.Context) {
		_ = _s.doResp(c.Writer, _s.handleCursorRequest(c.Request.Body))
	})
}

func GetServer() *Server {
	return _s
}
