package functional_tests

import (
	"encoding/json"
	"testing"

	"github.com/repoSpanner/repoSpanner/server/datastructures"
)

type testRepoInfo struct {
	name    string
	public  bool
	symrefs map[string]string
}

func verifyReposExist(t *testing.T, node nodeNrType, repos ...testRepoInfo) {
	repoout := runCommand(t, node.Name(), "admin", "repo", "list", "--json")

	var repolist datastructures.RepoList
	err := json.Unmarshal([]byte(repoout), &repolist)
	failIfErr(t, err, "parsing repo list")
	t.Log("Repolist:", repolist)

	for _, repo := range repos {
		retrieved, hadrepo := repolist.Repos[repo.name]
		if !hadrepo {
			t.Errorf("Could not find expected repo %s", repo.name)
			continue
		}
		if repo.public != retrieved.Public {
			t.Errorf("Public status (%v) not as expected (%v)",
				retrieved.Public,
				repo.public,
			)
		}
		if repo.symrefs == nil {
			// Default: HEAD -> refs/heads/master
			if len(retrieved.Symrefs) != 1 {
				t.Errorf("Unexpected number of symrefs retrieved: %d", len(retrieved.Symrefs))
			}
			target, ok := retrieved.Symrefs["HEAD"]
			if !ok {
				t.Errorf("Unable to find HEAD")
			} else {
				if target != "refs/heads/master" {
					t.Errorf("HEAD pointed to %s instead of master", target)
				}
			}
		} else {
			for symref, correcttarget := range repo.symrefs {
				target, hasref := retrieved.Symrefs[symref]
				if !hasref {
					t.Errorf("Missing symref %s", symref)
					continue
				}

				if target != correcttarget {
					t.Errorf("Symref %s points to %s instead of %s", symref, target, correcttarget)
				}
			}

			if len(retrieved.Symrefs) != len(repo.symrefs) {
				t.Error("Too many symrefs returned")
			}
		}
	}

	if len(repos) != len(repolist.Repos) {
		t.Error("Too many repos returned")
	}
}

func TestRepoManagement(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}

	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	spawnNode(t, nodea)
	joinNode(t, nodeb, nodea)
	joinNode(t, nodec, nodeb)

	// Verify all three nodes start with no repos
	verifyReposExist(t, nodea)
	verifyReposExist(t, nodeb)
	verifyReposExist(t, nodec)

	// Create three newrepos
	out1 := runCommand(t, nodea.Name(),
		"admin", "repo", "create", "test1", "--json")
	runCommand(t, nodeb.Name(),
		"admin", "repo", "create", "test2", "--public")
	runCommand(t, nodec.Name(),
		"admin", "repo", "create", "test3")
	runFailingCommand(t, nodea.Name(),
		"admin", "repo", "create", "test2")
	out4 := runCommand(t, nodea.Name(),
		"admin", "repo", "create", "test1", "--json")

	// Make sure out1 contained the correct json
	var resp1 datastructures.CommandResponse
	err := json.Unmarshal([]byte(out1), &resp1)
	failIfErr(t, err, "parsing response json")
	if !resp1.Success {
		t.Errorf("Error creating test repo: %s", resp1.Error)
	}

	// Make sure out4 contained the correct error
	var resp4 datastructures.CommandResponse
	err = json.Unmarshal([]byte(out4), &resp4)
	failIfErr(t, err, "parsing response json")
	if resp4.Success {
		t.Error("Recreating repo succeeded")
	}
	if resp4.Error != "Repo test1 already exists" {
		t.Errorf("Unexpected error message during recreating repo: %s", resp4.Error)
	}

	// Verify all repos exist on all nodes
	r1 := testRepoInfo{name: "test1", public: false}
	r2 := testRepoInfo{name: "test2", public: true}
	r3 := testRepoInfo{name: "test3", public: false}
	verifyReposExist(t, nodea, r1, r2, r3)
	verifyReposExist(t, nodeb, r1, r2, r3)
	verifyReposExist(t, nodec, r1, r2, r3)

	// Create a symrefs
	runCommand(t, nodea.Name(),
		"admin", "repo", "edit", "test1", "foo=bar")
	r1.symrefs = make(map[string]string)
	r1.symrefs["HEAD"] = "refs/heads/master"
	r1.symrefs["foo"] = "bar"
	verifyReposExist(t, nodea, r1, r2, r3)
	verifyReposExist(t, nodeb, r1, r2, r3)
	verifyReposExist(t, nodec, r1, r2, r3)

	// Update a symref
	runCommand(t, nodea.Name(),
		"admin", "repo", "edit", "test1", "HEAD=refs/foo/nobody")
	r1.symrefs["HEAD"] = "refs/foo/nobody"
	verifyReposExist(t, nodea, r1, r2, r3)
	verifyReposExist(t, nodeb, r1, r2, r3)
	verifyReposExist(t, nodec, r1, r2, r3)

	// Delete a symref
	runCommand(t, nodea.Name(),
		"admin", "repo", "edit", "test1", "HEAD=")
	delete(r1.symrefs, "HEAD")
	verifyReposExist(t, nodea, r1, r2, r3)
	verifyReposExist(t, nodeb, r1, r2, r3)
	verifyReposExist(t, nodec, r1, r2, r3)

	// Delete test3
	runCommand(t, nodec.Name(),
		"admin", "repo", "delete", "test3")
	verifyReposExist(t, nodea, r1, r2)
	verifyReposExist(t, nodeb, r1, r2)
	verifyReposExist(t, nodec, r1, r2)
}
