package service

import (
	"crypto/tls"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"repospanner.org/repospanner/server/constants"
)

type permissionInfo struct {
	Authenticated bool
	Username      string
	regions       []string
	repos         []string
	permissions   []constants.CertPermission
}

func (cfg *Service) getPermissionInfo(cstate *tls.ConnectionState) permissionInfo {
	info := permissionInfo{
		Authenticated: false,
		Username:      "",
		regions:       []string{},
		repos:         []string{},
		permissions:   []constants.CertPermission{},
	}
	if len(cstate.VerifiedChains) >= 1 {
		peercert := cstate.VerifiedChains[0][0]
		info.Authenticated = true
		info.Username = peercert.Subject.CommonName
		for _, ext := range peercert.Extensions {
			if ext.Id.Equal(constants.OIDRegionName) {
				info.regions = append(info.regions, string(ext.Value))
				continue
			}
			if ext.Id.Equal(constants.OIDRepoName) {
				info.repos = append(info.repos, string(ext.Value))
				continue
			}
			if ext.Id.Equal(constants.OIDPermission) {
				info.permissions = append(info.permissions, constants.CertPermission(ext.Value))
				continue
			}
		}
	}
	return info
}

func (cfg *Service) addPermToLogger(perminfo permissionInfo, log *logrus.Entry) *logrus.Entry {
	log = log.WithField(
		"authed", perminfo.Authenticated,
	)
	if perminfo.Authenticated {
		log = log.WithFields(logrus.Fields{
			"username":         perminfo.Username,
			"user-permissions": perminfo.permissions,
			"user-regions":     perminfo.regions,
			"user-repos":       perminfo.repos,
		})
	}

	return log
}

func lineMatches(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	matched, err := filepath.Match(pattern, name)
	return err == nil && matched
}

func (cfg *Service) checkAccess(i permissionInfo, repo string, action constants.CertPermission) bool {
	hasRegionAccess, hasRepoAccess, hasAction := false, false, false

	if strings.HasPrefix(repo, "admin/") {
		// Require admin for all "admin/" repositories
		for _, perm := range i.permissions {
			if perm == constants.CertPermissionAdmin {
				break
			}
			return false
		}
	}

	if cfg.statestore.IsRepoPublic(repo) && action == constants.CertPermissionRead {
		// Public repo, all is OK
		return true
	}

	for _, regionline := range i.regions {
		if lineMatches(regionline, cfg.region) {
			hasRegionAccess = true
			break
		}
	}

	for _, repoline := range i.repos {
		if lineMatches(repoline, repo) {
			hasRepoAccess = true
			break
		}
	}

	for _, perm := range i.permissions {
		if perm == action {
			hasAction = true
			break
		}
	}

	return hasRegionAccess && hasRepoAccess && hasAction
}
