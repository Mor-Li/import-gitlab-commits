// Package app provides the core functionality for the import-gitlab-commits application,
// including initializing the GitLab client, fetching user information,
// and importing commits into a local git repository.
package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	gogitlab "gitlab.com/gitlab-org/api/client-go"
)

const (
	getCurrentUserTimeout = 2 * time.Second

	maxProjects = 1000
)

type App struct {
	logger *log.Logger

	gitlabBaseURL *url.URL
	gitlab        *GitLab

	committerName  string
	committerEmail string
}

type User struct {
	Name      string
	Emails    []string
	Username  string
	CreatedAt time.Time
}

func New(logger *log.Logger, gitlabToken string, gitlabBaseURL *url.URL, committerName, committerEmail string,
) (*App, error) {
	gitlabClient, err := gogitlab.NewClient(gitlabToken, gogitlab.WithBaseURL(gitlabBaseURL.String()))
	if err != nil {
		return nil, fmt.Errorf("create GitLab client: %w", err)
	}

	f := NewGitLab(logger, gitlabClient)

	return &App{
		logger:         logger,
		gitlab:         f,
		gitlabBaseURL:  gitlabBaseURL,
		committerName:  committerName,
		committerEmail: committerEmail,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	ctxCurrent, cancel := context.WithTimeout(ctx, getCurrentUserTimeout)
	defer cancel()

	currentUser, err := a.gitlab.CurrentUser(ctxCurrent)
	if err != nil {
		return fmt.Errorf("get current user: %w", err)
	}

	a.logger.Printf("Found current user %q", currentUser.Name)

	repoPath := "./" + repoName(a.gitlabBaseURL, currentUser)

	repo, err := a.createOrOpenRepo(repoPath)
	if err != nil {
		return fmt.Errorf("create or open repo: %w", err)
	}

	worktree, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("get worktree: %w", err)
	}

	lastCommitDate := a.lastCommitDate(repo)

	// First, collect all commits from all projects into a single list
	allCommits := []struct {
		Commit    *Commit
		ProjectID int64
	}{}
	projectID := int64(0)
	page := int64(1)

	a.logger.Printf("Collecting all commits from all projects...")

	for page > 0 {
		projects, nextPage, errFetch := a.gitlab.FetchProjectPage(ctx, page, currentUser, projectID)
		if errFetch != nil {
			return fmt.Errorf("fetch projects: %w", errFetch)
		}

		for _, project := range projects {
			commits, errCommit := a.gitlab.FetchCommits(ctx, currentUser, project, lastCommitDate)
			if errCommit != nil {
				return fmt.Errorf("fetch commits for project %d: %w", project, errCommit)
			}

			if len(commits) > 0 {
				for _, commit := range commits {
					allCommits = append(allCommits, struct {
						Commit    *Commit
						ProjectID int64
					}{
						Commit:    commit,
						ProjectID: project,
					})
				}
				a.logger.Printf("Collected %d commits from project %d", len(commits), project)
			}
		}

		page = nextPage
	}

	// Sort ALL commits by timestamp (oldest first)
	// Use stable sort with secondary key (ProjectID) for deterministic ordering
	sort.SliceStable(allCommits, func(i, j int) bool {
		commitI := allCommits[i].Commit.CommittedAt
		commitJ := allCommits[j].Commit.CommittedAt

		if commitI.Equal(commitJ) {
			// Use ProjectID as tiebreaker for stable ordering
			return allCommits[i].ProjectID < allCommits[j].ProjectID
		}
		return commitI.Before(commitJ)
	})

	// Adjust duplicate timestamps to ensure uniqueness
	// This fixes GitHub's commit-activity statistics API
	duplicateCount := 0
	for i := 1; i < len(allCommits); i++ {
		prevTime := allCommits[i-1].Commit.CommittedAt
		currTime := allCommits[i].Commit.CommittedAt

		if !currTime.After(prevTime) {
			// Add 1 second to make it unique and maintain chronological order
			allCommits[i].Commit.CommittedAt = prevTime.Add(1 * time.Second)
			duplicateCount++
		}
	}

	if duplicateCount > 0 {
		a.logger.Printf("Adjusted %d duplicate timestamps by adding 1-second increments", duplicateCount)
	}

	a.logger.Printf("Processing %d commits in strict chronological order...", len(allCommits))

	// Apply commits one by one in chronological order
	projectCommitCounter := make(map[int64]int, maxProjects)

	commitCount, errCommit := a.applyAllCommits(ctx, worktree, allCommits, projectCommitCounter)
	if errCommit != nil {
		return fmt.Errorf("apply commits: %w", errCommit)
	}

	a.logger.Printf("Applied %d total commits", commitCount)

	for project, commit := range projectCommitCounter {
		a.logger.Printf("project %d: commits %d", project, commit)
	}

	return nil
}

func (a *App) createOrOpenRepo(repoPath string) (*git.Repository, error) {
	repo, err := git.PlainInit(repoPath, false)
	if err == nil {
		a.logger.Printf("Init repository %q", repoPath)

		return repo, nil
	}

	if errors.Is(err, git.ErrRepositoryAlreadyExists) {
		a.logger.Printf("Repository %q already exists, opening it", repoPath)

		repo, err = git.PlainOpen(repoPath)
		if err != nil {
			return nil, fmt.Errorf("open: %w", err)
		}

		return repo, nil
	}

	return nil, fmt.Errorf("init: %w", err)
}

func (a *App) lastCommitDate(repo *git.Repository) time.Time {
	head, err := repo.Head()
	if err != nil {
		if !errors.Is(err, plumbing.ErrReferenceNotFound) {
			a.logger.Printf("Failed to get repo head: %v", err)
		}

		return time.Time{}
	}

	headCommit, err := repo.CommitObject(head.Hash())
	if err != nil {
		a.logger.Printf("Failed to get head commit: %v", err)

		return time.Time{}
	}

	projectID, _, err := ParseCommitMessage(headCommit.Message)
	if err != nil {
		a.logger.Printf("Failed to parse commit message: %v", err)

		return time.Time{}
	}

	lastCommitDate := headCommit.Committer.When

	a.logger.Printf("Found last project id %d and last commit date %v", projectID, lastCommitDate)

	return lastCommitDate
}

func (a *App) applyAllCommits(
	ctx context.Context, worktree *git.Worktree, allCommits []struct {
		Commit    *Commit
		ProjectID int64
	}, projectCommitCounter map[int64]int,
) (int, error) {
	a.logger.Printf("Applying %d commits in chronological order", len(allCommits))

	var totalCommitCounter int

	committer := &object.Signature{
		Name:  a.committerName,
		Email: a.committerEmail,
	}

	for _, commitWithProject := range allCommits {
		commit := commitWithProject.Commit
		projectID := commitWithProject.ProjectID

		committer.When = commit.CommittedAt

		// Track per-project commit counter
		projectCounter := projectCommitCounter[projectID]

		// Create a file with commit message to ensure non-empty commit
		filename := fmt.Sprintf("commits/%d-%d.txt", projectID, projectCounter)
		err := worktree.Filesystem.MkdirAll("commits", 0755)
		if err != nil {
			return totalCommitCounter, fmt.Errorf("create commits dir: %w", err)
		}

		file, err := worktree.Filesystem.Create(filename)
		if err != nil {
			return totalCommitCounter, fmt.Errorf("create file: %w", err)
		}
		_, err = file.Write([]byte(commit.Message + "\n"))
		if err != nil {
			file.Close()
			return totalCommitCounter, fmt.Errorf("write file: %w", err)
		}
		file.Close()

		_, err = worktree.Add(filename)
		if err != nil {
			return totalCommitCounter, fmt.Errorf("add file: %w", err)
		}

		if _, errCommit := worktree.Commit(commit.Message, &git.CommitOptions{
			Author:            committer,
			Committer:         committer,
			AllowEmptyCommits: false,
		}); errCommit != nil {
			return totalCommitCounter, fmt.Errorf("commit: %w", errCommit)
		}

		totalCommitCounter++
		projectCommitCounter[projectID]++
	}

	return totalCommitCounter, nil
}

// repoName generates unique repo name for the user.
func repoName(baseURL *url.URL, user *User) string {
	host := baseURL.Host

	const hostPortLen = 2

	hostPort := strings.Split(host, ":")
	if len(hostPort) > hostPortLen {
		host = hostPort[0]
	}

	return "repo." + host + "." + user.Username
}
