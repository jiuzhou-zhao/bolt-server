package dbs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/jiuzhou-zhao/bolt-server/pkg/boltsc"
	"github.com/satori/go.uuid"
)

const (
	datasDir = "datas"
)

type dbWithInfo struct {
	db     *bolt.DB
	dbName string
	cnt    int
}

type txWithInfo struct {
	dbID     string
	tx       *bolt.Tx
	chResult chan error
	chTxDone chan interface{}
}

type bucketWithInfo struct {
	txID   string
	bucket *bolt.Bucket
}

type cursorWithInfo struct {
	bucketID string
	cursor   *bolt.Cursor
}

type Server struct {
	dbLock  sync.RWMutex
	dbNames map[string]string
	dbs     map[string]*dbWithInfo

	txLock sync.RWMutex
	txs    map[string]*txWithInfo

	bucketLock sync.RWMutex
	buckets    map[string]*bucketWithInfo

	cursorLock sync.RWMutex
	cursors    map[string]*cursorWithInfo

	txTraceCnt int64
}

func NewServer() *Server {
	_ = os.MkdirAll(datasDir, os.ModePerm)

	s := &Server{
		dbNames: make(map[string]string),
		dbs:     make(map[string]*dbWithInfo),
		txs:     make(map[string]*txWithInfo),
		buckets: make(map[string]*bucketWithInfo),
		cursors: make(map[string]*cursorWithInfo),
	}
	go func() {
		for {
			time.Sleep(10 * time.Second)
			fmt.Printf("tx cnt: %d\n", atomic.LoadInt64(&s.txTraceCnt))
		}
	}()
	return s
}

func (s *Server) addNewTx(tx *bolt.Tx, chResult chan error, chTxDone chan interface{}, dbID string) string {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	id := "tx:" + uuid.NewV4().String()
	s.txs[id] = &txWithInfo{
		dbID:     dbID,
		tx:       tx,
		chResult: chResult,
		chTxDone: chTxDone,
	}
	return id
}

func (s *Server) getTx(id string) *txWithInfo {
	s.txLock.RLock()
	defer s.txLock.RUnlock()
	if txI, ok := s.txs[id]; ok {
		return txI
	}
	return nil
}

func (s *Server) removeTx(id string) {
	fnImpl := func() {
		s.txLock.Lock()
		defer s.txLock.Unlock()

		delete(s.txs, id)
	}

	fnImpl()

	s.afterRemoveTx(id)
}

func (s *Server) addNewBucket(bucket *bolt.Bucket, txID string) string {
	s.bucketLock.Lock()
	defer s.bucketLock.Unlock()

	id := "bucket:" + uuid.NewV4().String()
	s.buckets[id] = &bucketWithInfo{
		txID:   txID,
		bucket: bucket,
	}
	return id
}

func (s *Server) getBucket(id string) *bolt.Bucket {
	s.bucketLock.RLock()
	defer s.bucketLock.RUnlock()

	if bucket, ok := s.buckets[id]; ok {
		return bucket.bucket
	}
	return nil
}

func (s *Server) removeBucket(id string) {
	fnImpl := func() {
		s.bucketLock.Lock()
		defer s.bucketLock.Unlock()

		delete(s.buckets, id)
	}

	fnImpl()

	s.afterRemoveBucket(id)
}

func (s *Server) addNewCursor(cursor *bolt.Cursor, bucketID string) string {
	s.cursorLock.Lock()
	defer s.cursorLock.Unlock()

	id := "cursor:" + uuid.NewV4().String()
	s.cursors[id] = &cursorWithInfo{
		bucketID: bucketID,
		cursor:   cursor,
	}
	return id
}

func (s *Server) getCursor(id string) *bolt.Cursor {
	s.cursorLock.RLock()
	defer s.cursorLock.RUnlock()

	if cursor, ok := s.cursors[id]; ok {
		return cursor.cursor
	}
	return nil
}

func (s *Server) delCursor(id string) {
	s.cursorLock.Lock()
	defer s.cursorLock.Unlock()

	delete(s.cursors, id)
}

func (s *Server) getDbID(dbName string) string {
	s.dbLock.RLock()
	defer s.dbLock.RUnlock()
	if id, ok := s.dbNames[dbName]; ok {
		return id
	}
	return ""
}

func (s *Server) tryIncDbCnt(id string) bool {
	s.dbLock.Lock()
	defer s.dbLock.Unlock()

	if dbI, ok := s.dbs[id]; ok {
		dbI.cnt++
		return true
	}
	return false
}

func (s *Server) DecDbCnt(id string) int {
	s.dbLock.Lock()
	defer s.dbLock.Unlock()

	if dbI, ok := s.dbs[id]; ok {
		dbI.cnt--
		return dbI.cnt
	}
	return -1
}

func (s *Server) addNewDb(dbName string, db *bolt.DB) string {
	s.dbLock.Lock()
	defer s.dbLock.Unlock()

	id := "db:" + uuid.NewV4().String()
	s.dbNames[dbName] = id
	s.dbs[id] = &dbWithInfo{
		db:     db,
		dbName: dbName,
		cnt:    1,
	}
	return id
}

func (s *Server) getDb(id string) *bolt.DB {
	s.dbLock.RLock()
	defer s.dbLock.RUnlock()

	if dbI, ok := s.dbs[id]; ok {
		return dbI.db
	}
	return nil
}

func (s *Server) closeDb(id string) {
	fnImpl := func() {
		s.dbLock.Lock()
		defer s.dbLock.Unlock()

		dbI, ok := s.dbs[id]
		if !ok {
			return
		}
		delete(s.dbNames, dbI.dbName)
		delete(s.dbs, id)
	}

	fnImpl()

	s.afterCloseDb(id)
}

func (s *Server) afterCloseDb(dbID string) {
	txs := make([]string, 0)

	fnImpl := func() {
		s.txLock.Lock()
		defer s.txLock.Unlock()

		for txID, txI := range s.txs {
			if txI.dbID == dbID {
				txI.chResult <- errors.New("cancel")
				delete(s.txs, txID)
				txs = append(txs, txID)
			}
		}
	}

	fnImpl()

	for _, tx := range txs {
		s.afterRemoveTx(tx)
	}
}

func (s *Server) dbName2FileName(dbName string) string {
	return filepath.Join(datasDir, dbName)
}

func (s *Server) afterRemoveTx(txID string) {
	buckets := make([]string, 0)

	fnImpl := func() {
		s.bucketLock.Lock()
		defer s.bucketLock.Unlock()

		for bucketID, bucketI := range s.buckets {
			if bucketI.txID == txID {
				delete(s.buckets, bucketID)
				buckets = append(buckets, bucketID)
			}
		}
	}

	fnImpl()

	for _, bucket := range buckets {
		s.afterRemoveBucket(bucket)
	}
}

func (s *Server) afterRemoveBucket(bucketID string) {
	s.cursorLock.Lock()
	defer s.cursorLock.Unlock()

	for cursorID, cursorI := range s.cursors {
		if cursorI.bucketID == bucketID {
			delete(s.cursors, cursorID)
		}
	}
}

func (s *Server) bindJSON(r io.ReadCloser, req interface{}) (err error) {
	defer func() {
		_ = r.Close()
	}()
	d, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}
	err = json.Unmarshal(d, req)
	return
}

func (s *Server) doResp(writer http.ResponseWriter, resp interface{}) (err error) {
	d, err := json.Marshal(resp)
	if err != nil {
		return
	}
	_, err = writer.Write(d)
	return
}

func (s *Server) doDBRequest(req *boltsc.DBRequest) (resp *boltsc.DBResponse) {
	resp = &boltsc.DBResponse{}
	if req.Method == boltsc.DBMethodOpen {
		dbID := s.getDbID(req.DBName)
		if dbID != "" {
			if !s.tryIncDbCnt(dbID) {
				resp.SetErrorMessage("internal")
				return
			}
		} else {
			dB, err := bolt.Open(s.dbName2FileName(req.DBName), 0600, nil)
			if err != nil {
				resp.SetError(err)
				return
			}
			dbID = s.addNewDb(req.DBName, dB)
		}
		resp.DbID = dbID
		return
	}

	if req.Method == boltsc.DBMethodClose {
		cnt := s.DecDbCnt(req.DbID)
		if cnt > 0 {
			return
		}
		s.closeDb(req.DbID)
		return
	}

	if req.Method == boltsc.DBMethodUpdate || req.Method == boltsc.DBMethodView {
		dB := s.getDb(req.DbID)
		if dB == nil {
			resp.SetErrorMessage("no db")
		}

		fnDbDo := dB.Update
		if req.Method == boltsc.DBMethodView {
			fnDbDo = dB.View
		}

		var txID string
		var err error
		chTxID := make(chan string, 4)
		chTxExec := make(chan error, 2)
		chTxDone := make(chan interface{}, 2)
		go func() {
			atomic.AddInt64(&s.txTraceCnt, 1)
			err = fnDbDo(func(tx *bolt.Tx) error {
				txID = s.addNewTx(tx, chTxExec, chTxDone, req.DbID)
				chTxID <- txID
				return <-chTxExec
			})
			atomic.AddInt64(&s.txTraceCnt, -1)
			chTxDone <- nil
			if err != nil {
				if txID == "" {
					chTxID <- ""
				}
			}
		}()
		<-chTxID
		close(chTxID)
		if txID == "" {
			resp.SetError(fmt.Errorf("%w", err))
			return
		}
		resp.TxID = txID
		return
	}

	if req.Method == boltsc.DBMethodRebuild4Debug {
		dbID := s.getDbID(req.DBName)
		if dbID != "" {
			s.closeDb(dbID)
		}
		err := os.Remove(s.dbName2FileName(req.DBName))
		if err != nil {
			resp.SetError(err)
			return
		}
		return
	}

	resp.SetErrorMessage(fmt.Sprintf("unknown method: %v", req.Method))
	return
}

func (s *Server) handleDBRequest(r io.ReadCloser) *boltsc.DBResponse {
	var req boltsc.DBRequest
	err := s.bindJSON(r, &req)
	if err != nil {
		return &boltsc.DBResponse{
			ResponseBase: boltsc.ResponseBase{
				Status:  -1,
				Message: err.Error(),
			},
		}
	}
	return s.doDBRequest(&req)
}

func (s *Server) doTXRequest(req *boltsc.TXRequest) (resp *boltsc.TXResponse) {
	resp = &boltsc.TXResponse{}
	tx := s.getTx(req.TxID)
	if tx == nil {
		resp.SetErrorMessage("no tx")
		return
	}

	if req.Method == boltsc.TXMethodCreateBucket {
		bucket, err := tx.tx.CreateBucket([]byte(req.BucketName))
		if err != nil {
			resp.SetError(err)
			return
		}
		resp.BucketID = s.addNewBucket(bucket, req.TxID)
		return
	}

	if req.Method == boltsc.TXMethodGetBucket {
		bucket := tx.tx.Bucket([]byte(req.BucketName))
		if bucket == nil {
			resp.SetStatusWithMessage(boltsc.StatusNotExists, "no bucket")
			return
		}
		resp.BucketID = s.addNewBucket(bucket, req.TxID)
		return
	}

	if req.Method == boltsc.TXMethodDeleteBucket {
		err := tx.tx.DeleteBucket([]byte(req.BucketName))
		if err != nil {
			resp.SetError(err)
			return
		}
		return
	}

	if req.Method == boltsc.TXMethodClose {
		if req.RollbackMessage == "" {
			tx.chResult <- nil
		} else {
			tx.chResult <- errors.New(req.RollbackMessage)
		}
		select {
		case <-tx.chTxDone:
		case <-time.After(time.Second):
		}
		s.removeTx(req.TxID)
		return
	}

	resp.SetErrorMessage(fmt.Sprintf("unknown method: %v", req.Method))
	return
}

func (s *Server) handleTXRequest(r io.ReadCloser) *boltsc.TXResponse {
	var req boltsc.TXRequest
	err := s.bindJSON(r, &req)
	if err != nil {
		return &boltsc.TXResponse{
			ResponseBase: boltsc.ResponseBase{
				Status:  -1,
				Message: err.Error(),
			},
		}
	}
	return s.doTXRequest(&req)
}

func (s *Server) doBucketRequest(req *boltsc.BucketRequest) (resp *boltsc.BucketResponse) {
	resp = &boltsc.BucketResponse{}
	bucket := s.getBucket(req.BucketID)
	if bucket == nil {
		resp.SetErrorMessage("no bucket")
		return
	}
	if req.Method == boltsc.BucketMethodPut {
		err := bucket.Put(req.Key, req.Value)
		if err != nil {
			resp.SetError(err)
			return
		}
		return
	}

	if req.Method == boltsc.BucketMethodGet {
		resp.Value = bucket.Get(req.Key)
		return
	}

	if req.Method == boltsc.BucketMethodDelete {
		err := bucket.Delete(req.Key)
		if err != nil {
			resp.SetError(err)
			return
		}
		return
	}

	if req.Method == boltsc.BucketMethodCursor {
		cursor := bucket.Cursor()
		if cursor == nil {
			resp.SetErrorMessage("no cursor")
			return
		}
		resp.CursorID = s.addNewCursor(cursor, req.BucketID)
		return
	}

	if req.Method == boltsc.BucketMethodClose {
		s.removeBucket(req.BucketID)
		return
	}

	resp.SetErrorMessage(fmt.Sprintf("unknown method: %v", req.Method))
	return
}

func (s *Server) handleBucketRequest(r io.ReadCloser) *boltsc.BucketResponse {
	var req boltsc.BucketRequest
	err := s.bindJSON(r, &req)
	if err != nil {
		return &boltsc.BucketResponse{
			ResponseBase: boltsc.ResponseBase{
				Status:  -1,
				Message: err.Error(),
			},
		}
	}
	return s.doBucketRequest(&req)
}

func (s *Server) doCursorRequest(req *boltsc.CursorRequest) (resp *boltsc.CursorResponse) {
	resp = &boltsc.CursorResponse{}
	cursorW := s.getCursor(req.CursorID)
	if cursorW == nil {
		resp.SetErrorMessage("no cursor")
		return
	}
	if req.Method == boltsc.CursorMethodFirst {
		k, v := cursorW.First()
		resp.Key = k
		resp.Value = v
		return
	}

	if req.Method == boltsc.CursorMethodNext {
		k, v := cursorW.Next()
		resp.Key = k
		resp.Value = v
		return
	}

	if req.Method == boltsc.CursorMethodClose {
		s.delCursor(req.CursorID)
		return
	}

	resp.SetErrorMessage(fmt.Sprintf("unknown method: %v", req.Method))
	return
}
func (s *Server) handleCursorRequest(r io.ReadCloser) *boltsc.CursorResponse {
	var req boltsc.CursorRequest
	err := s.bindJSON(r, &req)
	if err != nil {
		return &boltsc.CursorResponse{
			ResponseBase: boltsc.ResponseBase{
				Status:  -1,
				Message: err.Error(),
			},
		}
	}
	return s.doCursorRequest(&req)
}

func (s *Server) ListDBs() (dbs []string) {
	s.dbLock.RLock()
	defer s.dbLock.RUnlock()

	for db := range s.dbNames {
		dbs = append(dbs, db)
	}

	return
}

func (s *Server) GetBoltDBHandler(dbName string) *bolt.DB {
	s.dbLock.RLock()
	defer s.dbLock.RUnlock()

	if id, ok := s.dbNames[dbName]; ok {
		if dbI, ok := s.dbs[id]; ok {
			return dbI.db
		}
	}

	return nil
}
