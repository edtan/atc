package migration

import (
	"github.com/concourse/atc/db/migration"
	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"
)

var _ = Describe("Multidriver", func() {
	var (
		driver migration.Driver
		// fakedriver     *migrationfakes.FakeDriver
		// fakemigrations *migrationfakes.FakeMigrations
	)

	BeforeEach(func() {
		// fakedriver = new(migrationfakes.FakeDriver)
		// fakemigrations = new(migrationfakes.FakeMigrations)
		//
		// driver = migration.NewDriverForMigrations(fakedriver, fakemigrations)
	})
	Context("Run", func() {
		It("should update the schema_migration table", func() {

		})

		It("should handle both Go and SQL migrations", func() {

		})

		Context("Go migrations", func() {
			It("should use an encryption strategy", func() {

			})
		})

	})
})
