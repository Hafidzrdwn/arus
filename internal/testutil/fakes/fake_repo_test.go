package fakes_test

import (
	"arus/internal/app/ports"
	"arus/internal/testutil/fakes"
	"arus/internal/testutil/repocontract"
	"testing"
)

func TestFakeRepo_RepoContract(t *testing.T) {
	repocontract.RepoContractTest(t, func() ports.DownloadRepository {
		return fakes.NewFakeRepo()
	})
}
