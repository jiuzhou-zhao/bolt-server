package webb

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/gin-gonic/gin"
	"github.com/jiuzhou-zhao/bolt-server/webs"
)

var _dbName string

func GetBoltDB() *bolt.DB {
	s := webs.GetServer()
	if _dbName != "" {
		db := s.GetBoltDBHandler(_dbName)
		if db != nil {
			return db
		}
	}
	dbs := s.ListDBs()
	if len(dbs) == 0 {
		return nil
	}
	_dbName = dbs[0]
	return s.GetBoltDBHandler(_dbName)
}

func Index(c *gin.Context) {
	c.Redirect(301, "/assets/html/layout.html")
}

func CreateBucket(c *gin.Context) {
	if c.PostForm("bucket") == "" {
		c.String(200, "no bucket name | n")
		return
	}

	db := GetBoltDB()
	if db == nil {
		c.String(200, "no db")
		return
	}

	_ = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(c.PostForm("bucket")))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	c.String(200, "ok")

}

func DeleteBucket(c *gin.Context) {
	if c.PostForm("bucket") == "" {
		c.String(200, "no bucket name | n")
		return
	}

	db := GetBoltDB()
	if db == nil {
		c.String(200, "no db")
		return
	}

	_ = db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(c.PostForm("bucket")))

		if err != nil {

			c.String(200, "error no such bucket | n")
			return fmt.Errorf("bucket: %s", err)
		}

		return nil
	})

	c.String(200, "ok")

}

func DeleteKey(c *gin.Context) {
	if c.PostForm("bucket") == "" || c.PostForm("key") == "" {
		c.String(200, "no bucket name or key | n")
		return
	}

	db := GetBoltDB()
	if db == nil {
		c.String(200, "no db")
		return
	}

	_ = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(c.PostForm("bucket")))
		b = b
		if err != nil {

			c.String(200, "error no such bucket | n")
			return fmt.Errorf("bucket: %s", err)
		}

		err = b.Delete([]byte(c.PostForm("key")))

		if err != nil {

			c.String(200, "error Deleting KV | n")
			return fmt.Errorf("delete kv: %s", err)
		}

		return nil
	})

	c.String(200, "ok")

}

func Put(c *gin.Context) {
	if c.PostForm("bucket") == "" || c.PostForm("key") == "" {
		c.String(200, "no bucket name or key | n")
		return
	}

	db := GetBoltDB()
	if db == nil {
		c.String(200, "no db")
		return
	}

	_ = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(c.PostForm("bucket")))
		b = b
		if err != nil {

			c.String(200, "error  creating bucket | n")
			return fmt.Errorf("create bucket: %s", err)
		}

		err = b.Put([]byte(c.PostForm("key")), []byte(c.PostForm("value")))

		if err != nil {

			c.String(200, "error writing KV | n")
			return fmt.Errorf("create kv: %s", err)
		}

		return nil
	})

	c.String(200, "ok")

}

func Get(c *gin.Context) {

	res := []string{"nok", ""}

	if c.PostForm("bucket") == "" || c.PostForm("key") == "" {

		res[1] = "no bucket name or key | n"
		c.JSON(200, res)
		return
	}

	db := GetBoltDB()
	if db == nil {
		c.String(200, "no db")
		return
	}

	_ = db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(c.PostForm("bucket")))

		if b != nil {

			v := b.Get([]byte(c.PostForm("key")))

			res[0] = "ok"
			res[1] = string(v)

			fmt.Printf("Key: %s\n", v)

		} else {

			res[1] = "error opening bucket| does it exist? | n"

		}
		return nil

	})

	c.JSON(200, res)

}

type Result struct {
	Result string
	M      map[string]string
}

func PrefixScan(c *gin.Context) {

	res := Result{Result: "nok"}

	res.M = make(map[string]string)

	if c.PostForm("bucket") == "" {

		res.Result = "no bucket name | n"
		c.JSON(200, res)
		return
	}

	db := GetBoltDB()
	if db == nil {
		c.String(200, "no db")
		return
	}

	count := 0

	if c.PostForm("key") == "" {

		_ = db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket([]byte(c.PostForm("bucket")))

			if b != nil {

				c := b.Cursor()

				for k, v := c.First(); k != nil; k, v = c.Next() {
					res.M[string(k)] = string(v)

					if count > 2000 {
						break
					}
					count++
				}

				res.Result = "ok"

			} else {

				res.Result = "no such bucket available | n"

			}

			return nil
		})

	} else {

		_ = db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket([]byte(c.PostForm("bucket"))).Cursor()

			if b != nil {

				prefix := []byte(c.PostForm("key"))

				for k, v := b.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = b.Next() {
					res.M[string(k)] = string(v)
					if count > 2000 {
						break
					}
					count++
				}

				res.Result = "ok"

			} else {

				res.Result = "no such bucket available | n"

			}

			return nil
		})

	}

	c.JSON(200, res)

}

func Buckets(c *gin.Context) {

	res := []string{}

	db := GetBoltDB()
	if db == nil {
		c.String(200, "no db")
		return
	}

	_ = db.View(func(tx *bolt.Tx) error {

		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {

			b := []string{string(name)}
			res = append(res, b...)
			return nil
		})

	})

	c.JSON(200, res)

}
