package logdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/adi/rawlog"
)

// Database owns the queues
type Database struct {
	dataFolder string
	queues     map[string]*rawlog.RawBytesLog
}

const logSuffix = ".log"

// NewDatabase creates the data folder if it doesn't exist yet
// and intializes the queues with new or saved data
func NewDatabase(dataFolder string) (*Database, error) {
	err := os.MkdirAll(dataFolder, 0700)
	if err != nil {
		return nil, fmt.Errorf("Couldn't ensure data folder exists at '%s': %w", dataFolder, err)
	}
	db := &Database{
		dataFolder: dataFolder,
		queues:     make(map[string]*rawlog.RawBytesLog),
	}
	fileInfos, err := ioutil.ReadDir(dataFolder)
	if err != nil {
		return nil, fmt.Errorf("Couldn't list files in data folder at '%s': %w", dataFolder, err)
	}
	for _, fileInfo := range fileInfos {
		if strings.HasSuffix(fileInfo.Name(), logSuffix) {
			queueFileName := path.Join(dataFolder, fileInfo.Name())
			loadedQueue, err := rawlog.Open(queueFileName)
			if err != nil {
				return nil, fmt.Errorf("Couldn't load queue from file at '%s': %w", queueFileName, err)
			}
			queueName := strings.TrimSuffix(fileInfo.Name(), logSuffix)
			db.queues[queueName] = loadedQueue
		}
	}
	return db, nil
}

// CreateQueue creates a new queue in this database
func (db *Database) CreateQueue(queueName string) error {
	queueFileName := path.Join(db.dataFolder, queueName+logSuffix)
	loadedQueue, err := rawlog.Open(queueFileName)
	if err != nil {
		return err
	}
	db.queues[queueName] = loadedQueue
	return nil
}

// Append adds an entry to a certain queue from this database
func (db *Database) Append(queueName string, key string, entry interface{}) error {
	q, ok := db.queues[queueName]
	if !ok {
		return fmt.Errorf("no such queue")
	}
	jsonEncodedEntry, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	err = q.Append(&rawlog.Entry{Key: []byte(key), Bytes: jsonEncodedEntry})
	if err != nil {
		return err
	}
	return nil
}
