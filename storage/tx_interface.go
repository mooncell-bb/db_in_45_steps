package storage

type TXLike interface {
	Abort()
	Commit() error
}

func AbortOrCommit(tx TXLike, updated bool, err error) (bool, error) {
	if err != nil {
		tx.Abort()
	} else {
		err = tx.Commit()
	}
	return err == nil && updated, err
}
