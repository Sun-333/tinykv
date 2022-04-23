package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	os.MkdirAll(conf.DBPath, os.ModePerm)
	db :=engine_util.CreateDB(conf.DBPath,false)
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return NewStandAloneReader(s.db), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	writeBatch := new(engine_util.WriteBatch)

	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(data.Cf,data.Key,data.Value)
		case storage.Delete:
			writeBatch.DeleteCF(data.Cf,data.Key)
		}
	}
	writeBatch.WriteToDB(s.db)
	return nil
}


type standaloneReader struct {
	tx *badger.Txn
}

func NewStandAloneReader(db *badger.DB) *standaloneReader {
	return &standaloneReader{db.NewTransaction(false)}
}

func (s *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.tx,cf,key)
	if err != nil {
		return nil, nil
	}
	return val, nil
}

func (s *standaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf,s.tx)
}

func (s *standaloneReader) Close() {
	s.tx.Discard()
}

