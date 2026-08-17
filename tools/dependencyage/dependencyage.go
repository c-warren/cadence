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
	"strings"
	"time"
)

var (
	requireInlinePattern = regexp.MustCompile(`^require\s+(\S+)\s+(\S+)`)
	replaceInlinePattern = regexp.MustCompile(`^replace\s+(.+)$`)
	pseudoVersionPattern = regexp.MustCompile(`-(\d{14})-[0-9a-f]{12}$`)
)

// ModuleVersion is one introduced module path and version pair.
type ModuleVersion struct {
	Module  string
	Version string
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
func ParseRequires(content string) map[string]string {
	requires := make(map[string]string)
	inBlock := false

	for _, raw := range strings.Split(content, "\n") {
		line := stripComment(raw)
		if line == "" {
			continue
		}

		if inBlock {
			if line == ")" {
				inBlock = false
				continue
			}

			parts := strings.Fields(line)
			if len(parts) >= 2 {
				requires[parts[0]] = parts[1]
			}
			continue
		}

		if strings.HasPrefix(line, "require (") {
			inBlock = true
			continue
		}

		matches := requireInlinePattern.FindStringSubmatch(line)
		if matches != nil {
			requires[matches[1]] = matches[2]
		}
	}

	return requires
}

// ParseReplaces returns each raw replace source and its versioned target. A nil
// target represents a filesystem-path replacement.
func ParseReplaces(content string) map[string]*ModuleVersion {
	replaces := make(map[string]*ModuleVersion)
	inBlock := false

	for _, raw := range strings.Split(content, "\n") {
		line := stripComment(raw)
		if line == "" {
			continue
		}

		if inBlock {
			if line == ")" {
				inBlock = false
				continue
			}

			key, target, ok := parseReplaceClause(line)
			if ok {
				replaces[key] = target
			}
			continue
		}

		if strings.HasPrefix(line, "replace (") {
			inBlock = true
			continue
		}

		matches := replaceInlinePattern.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		key, target, ok := parseReplaceClause(matches[1])
		if ok {
			replaces[key] = target
		}
	}

	return replaces
}

// NewRequirements returns head requirements that are new or have a different
// version than the corresponding base requirement.
func NewRequirements(baseContent, headContent string) []ModuleVersion {
	base := ParseRequires(baseContent)
	head := ParseRequires(headContent)

	introduced := make([]ModuleVersion, 0, len(head))
	for module, version := range head {
		if base[module] != version {
			introduced = append(introduced, ModuleVersion{Module: module, Version: version})
		}
	}
	sortModuleVersions(introduced)
	return introduced
}

// NewReplacements returns versioned head replacement targets that differ from
// the target for the same source in base.
func NewReplacements(baseContent, headContent string) []ModuleVersion {
	base := ParseReplaces(baseContent)
	head := ParseReplaces(headContent)

	var introduced []ModuleVersion
	for key, target := range head {
		if target != nil && !moduleVersionsEqual(base[key], target) {
			introduced = append(introduced, *target)
		}
	}
	sortModuleVersions(introduced)
	return introduced
}

// EscapeModulePath applies the uppercase escaping used by the Go module proxy
// protocol. It may also be used to escape versions in proxy URLs.
func EscapeModulePath(path string) string {
	var escaped strings.Builder
	for _, character := range path {
		if character >= 'A' && character <= 'Z' {
			escaped.WriteByte('!')
			escaped.WriteRune(character + ('a' - 'A'))
			continue
		}
		escaped.WriteRune(character)
	}
	return escaped.String()
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

func stripComment(line string) string {
	content, _, _ := strings.Cut(line, "//")
	return strings.TrimSpace(content)
}

func parseReplaceClause(clause string) (string, *ModuleVersion, bool) {
	left, right, ok := strings.Cut(clause, "=>")
	if !ok {
		return "", nil, false
	}

	key := strings.TrimSpace(left)
	rightParts := strings.Fields(strings.TrimSpace(right))
	if len(rightParts) < 2 || !strings.HasPrefix(rightParts[1], "v") {
		return key, nil, true
	}
	return key, &ModuleVersion{Module: rightParts[0], Version: rightParts[1]}, true
}

func moduleVersionsEqual(left, right *ModuleVersion) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func sortModuleVersions(pairs []ModuleVersion) {
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Module == pairs[j].Module {
			return pairs[i].Version < pairs[j].Version
		}
		return pairs[i].Module < pairs[j].Module
	})
}
