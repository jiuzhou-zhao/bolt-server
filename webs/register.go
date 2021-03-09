package webs

import (
	"github.com/gin-gonic/gin"
)

func Register(r *gin.Engine) {
	r.GET("/", Index)

	r.GET("/buckets", Buckets)
	r.POST("/createBucket", CreateBucket)
	r.POST("/put", Put)
	r.POST("/get", Get)
	r.POST("/deleteKey", DeleteKey)
	r.POST("/deleteBucket", DeleteBucket)
	r.POST("/prefixScan", PrefixScan)

	r.StaticFS("/assets", assetFS())
}
