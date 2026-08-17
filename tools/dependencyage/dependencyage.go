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

// Package dependencyage contains the core of the dependency-age CI gate. It is
// deliberately free of git, HTTP, and CLI concerns so it can later be extracted
// into a standalone GitHub Action.
package dependencyage

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"sort"
	"time"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

var pseudoVersionPattern = regexp.MustCompile(`-(\d{14})-[0-9a-f]{12}$`)

// ModuleVersion is one introduced module path and version pair.
type ModuleVersion struct {
	Module  string
	Version string
}

// Replacement is one replace directive.
type Replacement struct {
	Old ModuleVersion
	New *ModuleVersion
}

// Violation is an introduced version younger than the configured threshold.
type Violation struct {
	ModuleVersion
	Published time.Time
}

// TimeFetcher returns the publish time of a module version. A false found value
// means the source affirmatively does not know the version; an error means the
// lookup failed.
type TimeFetcher func(
	ctx context.Context,
	module string,
	version string,
) (t time.Time, found bool, err error)

// ParseRequires returns the module path and version from every require
// directive in go.mod content.
func ParseRequires(content string) (map[string]string, error) {
	file, err := parseModFile(content)
	if err != nil {
		return nil, err
	}

	requires := make(map[string]string)
	for _, require := range file.Require {
		requires[require.Mod.Path] = require.Mod.Version
	}
	return requires, nil
}

// ParseReplaces returns every replace directive. A nil target represents a
// filesystem-path replacement.
func ParseReplaces(content string) ([]Replacement, error) {
	file, err := parseModFile(content)
	if err != nil {
		return nil, err
	}

	replaces := make([]Replacement, 0, len(file.Replace))
	for _, replace := range file.Replace {
		replacement := Replacement{
			Old: ModuleVersion{Module: replace.Old.Path, Version: replace.Old.Version},
		}
		if replace.New.Version != "" {
			replacement.New = &ModuleVersion{
				Module:  replace.New.Path,
				Version: replace.New.Version,
			}
		}
		replaces = append(replaces, replacement)
	}
	return replaces, nil
}

// NewRequirements returns head requirements that are new or have a different
// version than the corresponding base requirement.
func NewRequirements(baseContent, headContent string) ([]ModuleVersion, error) {
	base, err := ParseRequires(baseContent)
	if err != nil {
		return nil, err
	}
	head, err := ParseRequires(headContent)
	if err != nil {
		return nil, err
	}

	introduced := make([]ModuleVersion, 0, len(head))
	for module, version := range head {
		if base[module] != version {
			introduced = append(introduced, ModuleVersion{Module: module, Version: version})
		}
	}
	sortModuleVersions(introduced)
	return introduced, nil
}

// NewReplacements returns versioned head replacement targets not present among
// the base replacement targets.
func NewReplacements(baseContent, headContent string) ([]ModuleVersion, error) {
	base, err := ParseReplaces(baseContent)
	if err != nil {
		return nil, err
	}
	head, err := ParseReplaces(headContent)
	if err != nil {
		return nil, err
	}

	baseTargets := make(map[ModuleVersion]struct{}, len(base))
	for _, replacement := range base {
		if replacement.New != nil {
			baseTargets[*replacement.New] = struct{}{}
		}
	}
	headTargets := make(map[ModuleVersion]struct{}, len(head))
	var introduced []ModuleVersion
	for _, replacement := range head {
		if replacement.New == nil {
			continue
		}
		target := *replacement.New
		if _, exists := baseTargets[target]; exists {
			continue
		}
		if _, exists := headTargets[target]; exists {
			continue
		}
		headTargets[target] = struct{}{}
		introduced = append(introduced, target)
	}
	sortModuleVersions(introduced)
	return introduced, nil
}

// EscapeModulePath applies the uppercase escaping used by the Go module proxy
// protocol.
func EscapeModulePath(path string) string {
	escaped, err := module.EscapePath(path)
	if err != nil {
		return path
	}
	return escaped
}

// EscapeVersion applies the uppercase escaping used for versions in the Go
// module proxy protocol.
func EscapeVersion(version string) string {
	escaped, err := module.EscapeVersion(version)
	if err != nil {
		return version
	}
	return escaped
}

// PseudoVersionTime returns the UTC commit timestamp embedded at the end of a
// Go pseudo-version.
func PseudoVersionTime(version string) (time.Time, bool) {
	matches := pseudoVersionPattern.FindStringSubmatch(version)
	if matches == nil {
		return time.Time{}, false
	}

	published, err := time.Parse("20060102150405", matches[1])
	if err != nil {
		return time.Time{}, false
	}
	return published, true
}

// FindViolations returns introduced module versions younger than thresholdDays.
// Unknown publish times fall back to pseudo-version timestamps; pairs with no
// available time are warned about and skipped. Fetch errors abort the check.
func FindViolations(
	ctx context.Context,
	pairs []ModuleVersion,
	thresholdDays int,
	now time.Time,
	fetch TimeFetcher,
	warnW io.Writer,
) ([]Violation, error) {
	sortedPairs := append([]ModuleVersion(nil), pairs...)
	sortModuleVersions(sortedPairs)

	var violations []Violation
	for _, pair := range sortedPairs {
		published, found, err := fetch(ctx, pair.Module, pair.Version)
		if err != nil {
			return nil, fmt.Errorf("fetch publish time for %s@%s: %w", pair.Module, pair.Version, err)
		}
		if !found {
			published, found = PseudoVersionTime(pair.Version)
		}
		if !found {
			_, _ = fmt.Fprintf(
				warnW,
				"WARN could not determine publish time for %s@%s; skipping\n",
				pair.Module,
				pair.Version,
			)
			continue
		}

		if now.Sub(published) < time.Duration(thresholdDays)*24*time.Hour {
			violations = append(violations, Violation{
				ModuleVersion: pair,
				Published:     published,
			})
		}
	}

	return violations, nil
}

func parseModFile(content string) (*modfile.File, error) {
	file, err := modfile.Parse("go.mod", []byte(content), nil)
	if err == nil {
		return file, nil
	}

	fixes, ok := incompatibleVersionFixes(err)
	if !ok {
		return nil, err
	}
	originals := make(map[ModuleVersion]string, len(fixes))
	file, err = modfile.Parse("go.mod", []byte(content), func(path, version string) (string, error) {
		canonical := module.CanonicalVersion(version)
		if canonical == "" {
			return "", fmt.Errorf("invalid module version %q", version)
		}
		fixed, exists := fixes[ModuleVersion{Module: path, Version: canonical}]
		if !exists {
			return canonical, nil
		}
		originals[ModuleVersion{Module: path, Version: fixed}] = canonical
		return fixed, nil
	})
	if err != nil {
		return nil, err
	}

	for _, require := range file.Require {
		require.Mod.Version = originalVersion(require.Mod.Path, require.Mod.Version, originals)
	}
	for _, replace := range file.Replace {
		replace.Old.Version = originalVersion(replace.Old.Path, replace.Old.Version, originals)
		replace.New.Version = originalVersion(replace.New.Path, replace.New.Version, originals)
	}
	return file, nil
}

func incompatibleVersionFixes(err error) (map[ModuleVersion]string, bool) {
	errors, ok := err.(modfile.ErrorList)
	if !ok || len(errors) == 0 {
		return nil, false
	}

	fixes := make(map[ModuleVersion]string, len(errors))
	for _, parseError := range errors {
		if parseError.Verb != "require" && parseError.Verb != "replace" {
			return nil, false
		}
		invalidVersion, ok := parseError.Err.(*module.InvalidVersionError)
		if !ok {
			return nil, false
		}
		canonical := module.CanonicalVersion(invalidVersion.Version)
		_, pathMajor, pathOK := module.SplitPathVersion(parseError.ModPath)
		fixed := canonical + "+incompatible"
		if canonical == "" || !pathOK || pathMajor != "" ||
			module.CheckPathMajor(canonical, pathMajor) == nil ||
			module.CheckPathMajor(fixed, pathMajor) != nil {
			return nil, false
		}
		fixes[ModuleVersion{Module: parseError.ModPath, Version: canonical}] = fixed
	}
	return fixes, true
}

func originalVersion(path, version string, originals map[ModuleVersion]string) string {
	if original, exists := originals[ModuleVersion{Module: path, Version: version}]; exists {
		return original
	}
	return version
}

func sortModuleVersions(pairs []ModuleVersion) {
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Module == pairs[j].Module {
			return pairs[i].Version < pairs[j].Version
		}
		return pairs[i].Module < pairs[j].Module
	})
}
