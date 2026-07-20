// Copyright (c) 2021 Uber Technologies, Inc.
// Portions of the Software are attributed to Copyright (c) 2020 Temporal Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package nosqlplugin

import (
	"os"
	"path/filepath"
	"strings"
)

func getCadencePackageDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// Walk up from the current working directory to find the repo/module root,
	// identified by the presence of the schema directory. This is robust to being
	// run from a git worktree (e.g. .../cadence/.worktrees/cadence/<branch>/...),
	// where the naive "last index of cadence/" heuristic below resolves to the
	// worktree container dir instead of the worktree root and loads the wrong schema.
	for dir := cwd; ; {
		if _, statErr := os.Stat(filepath.Join(dir, "schema", "cassandra")); statErr == nil {
			return dir + string(os.PathSeparator), nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	// Fallback to the legacy heuristic to preserve prior behavior if the schema
	// directory could not be located above the current working directory.
	cadenceIndex := strings.LastIndex(cwd, "cadence/")
	if cadenceIndex == -1 {
		return cwd + string(os.PathSeparator), nil
	}
	return cwd[:cadenceIndex+len("cadence/")], nil
}

func GetDefaultTestSchemaDir(testSchemaRelativePath string) (string, error) {
	cadencePackageDir, err := getCadencePackageDir()
	if err != nil {
		return "", err
	}
	return cadencePackageDir + testSchemaRelativePath, nil
}
