package migration

import (
	"database/sql"
	"fmt"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/atc/db/encryption"
	"github.com/concourse/atc/db/lock"
)

const (
	postgresTableName string = "migration_version"
	noTransaction     string = "NO_TRANSACTION;"
)

type DbHelper struct {
	dataSourceName string
	driver         string
	lockFactory    lock.LockFactory
	strategy       encryption.Strategy
}

func NewDbHelper(name, driver string, lockFactory lock.LockFactory, strategy encryption.Strategy) *DbHelper {
	return &DbHelper{
		name,
		driver,
		lockFactory,
		strategy,
	}
}

func (self *DbHelper) CurrentVersion() (int, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return -1, err
	}
	defer db.Close()

	return NewMigrator(db, self.lockFactory, self.strategy, AssetNames()).CurrentVersion()
}

func (self *DbHelper) Open() (*sql.DB, error) {

	return nil, nil
}

func (self *DbHelper) OpenAtVersion(version int) (*sql.DB, error) {
	return nil, nil
}

func (self *DbHelper) MigrateToVersion(version int) error {
	return nil
}

func (self *DbHelper) SupportedVersion() (int, error) {
	return 0, nil
}

func (self *migrator) checkOrCreateSchemaMigrationsTable() error {
	_, err := self.db.Exec("CREATE TABLE IF NOT EXISTS " + postgresTableName + " (version varchar(255) not null primary key)")
	return err
}

type Migrator interface {
	CurrentVersion() (int, error)
	SupportedVersion() (int, error)
	Down(version int) error
	Up() error
}

type migrator struct {
	db             *sql.DB
	lockFactory    lock.LockFactory
	strategy       encryption.Strategy
	logger         lager.Logger
	migrationFiles []string
}

func NewMigrator(db *sql.DB, lockFactory lock.LockFactory, strategy encryption.Strategy, migrations []string) *migrator {
	if len(migrations) == 0 {
		migrations = AssetNames()
	}

	return &migrator{
		db,
		lockFactory,
		strategy,
		lager.NewLogger("migrations"),
		migrations,
	}
}

func (self *migrator) CurrentVersion() (int, error) {
	err := self.checkOrCreateSchemaMigrationsTable()
	if err != nil {
		return 0, err
	}
	self.db.Exec("SELECT version from " + postgresTableName + " ORDER BY DESC")
	return 0, nil
}

func (self *migrator) SupportedVersion() (int, error) {
	return 0, nil
}

func (self *migrator) Down(version int) error {
	err := self.checkOrCreateSchemaMigrationsTable()
	if err != nil {
		return err
	}
	return nil
}

func (self *migrator) Up() error {
	err := self.checkOrCreateSchemaMigrationsTable()
	if err != nil {
		return err
	}
	_, err = self.checkLegacyVersion()
	if err != nil {
		return err
	}

	for _, migration := range self.migrationFiles {

		statements, err := self.ParseFile(migration)
		if err != nil {
			return err
		}

		if statements[0] == noTransaction {
			for _, statement := range statements[1:] {
				_, err := self.db.Exec(statement)
				if err != nil {
					fmt.Printf("err4: %v", err)
					return err
				}
			}
		} else {
			err = self.runTransaction(statements)
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func (self *migrator) runTransaction(statements []string) error {
	var migrationErr error

	tx, err := self.db.Begin()
	if err != nil {
		return err
	}

	defer func() error {
		if migrationErr != nil {
			if errRb := tx.Rollback(); errRb != nil {
				fmt.Errorf("Error rolling back: %s\n%s", errRb, err)
			}
			return err
		}

		commitErr := tx.Commit()
		if commitErr != nil {
			fmt.Printf("err2: %v", commitErr)
			return commitErr
		}
		return nil
	}()

	for _, statement := range statements {
		_, migrationErr = tx.Exec(statement)
		if migrationErr != nil {
			return migrationErr
		}
	}

	return nil
}

func (self *migrator) existLegacyVersion() bool {
	var exists bool
	err := self.db.QueryRow("SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_name = 'migration_version')").Scan(&exists)
	return err != nil || exists
}

func (self *migrator) checkLegacyVersion() (int, error) {
	oldMigrationLastVersion := 189
	newMigrationStartVersion := 1510262030

	var err error
	var dbVersion int

	exists := self.existLegacyVersion()
	if !exists {
		return -1, nil
	}

	if err = self.db.QueryRow("SELECT version FROM migration_version").Scan(&dbVersion); err != nil {
		return -1, nil
	}

	if dbVersion != oldMigrationLastVersion {
		return -1, fmt.Errorf("Must upgrade from db version %d (concourse 3.6.0), current db version: %d", oldMigrationLastVersion, dbVersion)
	}

	if _, err = self.db.Exec("DROP TABLE IF EXISTS migration_version"); err != nil {
		return -1, err
	}

	return newMigrationStartVersion, nil
}

func (self *migrator) ParseFile(migrationFileName string) ([]string, error) {
	migrationFileContents, err := Asset(migrationFileName)
	if err != nil {
		return nil, err
	}

	migrationStatements := strings.Split(string(migrationFileContents), ";")

	return migrationStatements, nil
}

// func (self *migrator) openWithLock() (*migrate.Migrate, lock.Lock, error) {
// 	var err error
// 	var acquired bool
// 	var newLock lock.Lock
// 	if self.lockFactory != nil {
// 		for {
// 			newLock, acquired, err = self.lockFactory.Acquire(self.logger, lock.NewDatabaseMigrationLockID())
// 			if err != nil {
// 				return nil, nil, err
// 			}
// 			if acquired {
// 				break
// 			}
// 			time.Sleep(1 * time.Second)
// 		}
// 	}
// 	m, err := self.open()
// 	if err != nil && newLock != nil {
// 		newLock.Release()
// 		return nil, nil, err
// 	}
// 	return m, newLock, err
// }
