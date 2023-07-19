package commands

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion
	// YOUR CODE HERE (lab2).
	// Check if the commitTs is invalid, the commitTs must be greater than the transaction startTs. If not
	// report unexpected error.

	response := new(kvrpcpb.CommitResponse)

	if commitTs <= txn.StartTS {
		response.Error = &kvrpcpb.KeyError{
			Retryable: fmt.Sprintf("%vunexpected error: commitTs must be greater than transaction startTs", txn.StartTS)}
		return response, fmt.Errorf("")
	}

	// Commit each key.
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	// If there is no correspond lock for this transaction.
	log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))
	if lock == nil || lock.Ts != txn.StartTS {
		// YOUR CODE HERE (lab2).
		// Key is locked by a different transaction, or there is no lock on the key. It's needed to
		// check the commit/rollback record for this key, if nothing is found report lock not found
		// error. Also the commit request could be stale that it's already committed or rolled back.
		// 锁定键被其他事务锁定，或者键上没有锁定。
		// 需要检查此键的提交/回滚记录，如果找不到任何记录，则报告找不到锁定的错误。
		// 此外，提交请求可能已经过时，已经提交或回滚。
		// 找到该key最新的write（提交记录）
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		// 该key已经成功提交，不进行任何操作返回成功（幂等）
		if write != nil && write.Kind != mvcc.WriteKindRollback && write.StartTS == txn.StartTS {
			return nil, nil
		}
		// 该事务已经被回滚
		if write != nil && write.Kind == mvcc.WriteKindRollback && write.StartTS == txn.StartTS {
			respValue := reflect.ValueOf(response)
			// 表明原因
			keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("The transaction with stast_ts %v has been rolled back ", txn.StartTS)}
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
			return response, nil
		}
		// 该key不存在任何提交或回滚记录
		respValue := reflect.ValueOf(response)
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
	}

	// 正常情况下，该key应当存在同一个事务的锁
	// lock != nil && lock.Ts == txn.StartTS

	// Commit a Write object to the DB
	// 用commit_ts在write列写入提交记录
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	// 清理锁
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
