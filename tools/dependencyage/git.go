// The MIT License (MIT)

// Copyright (c) 2026 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package dependencyage

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// GitSource reads changed go.mod files and their base and head contents using
// the git CLI.
type GitSource struct {
	Dir string
}

// ChangedGoModFiles returns added, copied, modified, and renamed go.mod files
// between baseRef and HEAD.
func (s *GitSource) ChangedGoModFiles(baseRef string) ([]string, error) {
	output, err := s.git(
		"diff",
		"--name-only",
		"--diff-filter=ACMR",
		baseRef+"...HEAD",
		"--",
		"go.mod",
		"**/go.mod",
	)
	if err != nil {
		return nil, fmt.Errorf("git diff failed: %w", err)
	}

	var files []string
	for _, path := range strings.Split(string(output), "\n") {
		if path != "" {
			files = append(files, path)
		}
	}
	return files, nil
}

// Contents returns the working-tree and base-ref contents for path. A failed
// git show means the path did not exist at the base ref.
func (s *GitSource) Contents(
	baseRef string,
	path string,
) (head string, base string, baseExists bool, err error) {
	headContent, err := os.ReadFile(filepath.Join(s.directory(), filepath.FromSlash(path)))
	if err != nil {
		return "", "", false, fmt.Errorf("read working-tree file %s: %w", path, err)
	}

	baseContent, err := s.git("show", baseRef+":"+path)
	if err != nil {
		return string(headContent), "", false, nil
	}
	return string(headContent), string(baseContent), true, nil
}

func (s *GitSource) git(args ...string) ([]byte, error) {
	commandArgs := append([]string{"-C", s.directory()}, args...)
	cmd := exec.Command("git", commandArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", err, message)
	}
	return output, nil
}

func (s *GitSource) directory() string {
	if s.Dir == "" {
		return "."
	}
	return s.Dir
}
