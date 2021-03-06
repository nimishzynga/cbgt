//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/couchbase/blance"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rebalance"
)

func runRebalance(cfg cbgt.Cfg, server string, nodesToRemove []string,
	favorMinNodes bool, dryRun bool, verbose int) error {
	r, err := rebalance.StartRebalance(cbgt.VERSION, cfg, flags.Server,
		nodesToRemove,
		rebalance.RebalanceOptions{
			FavorMinNodes: favorMinNodes,
			DryRun:        dryRun,
			Verbose:       verbose,
		})
	if err != nil {
		return fmt.Errorf("rebalance: StartRebalance, err: %v", err)
	}

	err = reportProgress(r)
	if err != nil {
		return fmt.Errorf("rebalance: reportProgress, err: %v", err)
	}

	r.Stop()

	return nil
}

type ProgressEntry struct {
	pindex, sourcePartition, node string // Immutable.

	stateOp     rebalance.StateOp
	initUUIDSeq cbgt.UUIDSeq
	currUUIDSeq cbgt.UUIDSeq
	wantUUIDSeq cbgt.UUIDSeq

	move int
	done bool
}

func reportProgress(r *rebalance.Rebalancer) error {
	var firstError error

	var lastEmit string

	maxNodeLen := 0
	maxPIndexLen := 0

	seenNodes := map[string]bool{}
	seenNodesSorted := []string(nil)

	// Map of pindex -> (source) partition -> node -> *ProgressEntry
	progressEntries := map[string]map[string]map[string]*ProgressEntry{}

	seenPIndexes := map[string]bool{}
	seenPIndexesSorted := []string(nil)

	updateProgressEntry := func(pindex, sourcePartition, node string,
		cb func(*ProgressEntry)) {
		if !seenNodes[node] {
			seenNodes[node] = true
			seenNodesSorted = append(seenNodesSorted, node)
			sort.Strings(seenNodesSorted)

			if maxNodeLen < len(node) {
				maxNodeLen = len(node)
			}
		}

		if maxPIndexLen < len(pindex) {
			maxPIndexLen = len(pindex)
		}

		sourcePartitions, exists := progressEntries[pindex]
		if !exists || sourcePartitions == nil {
			sourcePartitions = map[string]map[string]*ProgressEntry{}
			progressEntries[pindex] = sourcePartitions
		}

		nodes, exists := sourcePartitions[sourcePartition]
		if !exists || nodes == nil {
			nodes = map[string]*ProgressEntry{}
			sourcePartitions[sourcePartition] = nodes
		}

		progressEntry, exists := nodes[node]
		if !exists || progressEntry == nil {
			progressEntry = &ProgressEntry{
				pindex:          pindex,
				sourcePartition: sourcePartition,
				node:            node,
				move:            -1,
			}
			nodes[node] = progressEntry
		}

		cb(progressEntry)

		// TODO: Check UUID matches, too.

		if !seenPIndexes[pindex] {
			seenPIndexes[pindex] = true
			seenPIndexesSorted =
				append(seenPIndexesSorted, pindex)

			sort.Strings(seenPIndexesSorted)
		}
	}

	for progress := range r.ProgressCh() {
		if progress.Error != nil {
			r.Log("main: error, progress: %+v", progress)

			if firstError == nil {
				firstError = progress.Error
			}

			r.Stop()

			continue
		}

		updateProgressEntries(r, updateProgressEntry)

		var b bytes.Buffer

		writeProgressTable(&b, maxNodeLen, maxPIndexLen,
			seenNodes,
			seenNodesSorted,
			seenPIndexes,
			seenPIndexesSorted,
			progressEntries)

		currEmit := b.String()
		if currEmit != lastEmit {
			r.Log("%s", currEmit)
		}

		lastEmit = currEmit
	}

	return firstError
}

func updateProgressEntries(
	r *rebalance.Rebalancer,
	updateProgressEntry func(pindex, sourcePartition, node string,
		cb func(*ProgressEntry)),
) {
	r.Visit(func(
		currStates rebalance.CurrStates,
		currSeqs rebalance.CurrSeqs,
		wantSeqs rebalance.WantSeqs,
		mapNextMoves map[string]*blance.NextMoves,
	) {
		for _, pindexes := range currStates {
			for pindex, nodes := range pindexes {
				for node, stateOp := range nodes {
					updateProgressEntry(pindex, "", node,
						func(pe *ProgressEntry) {
							pe.stateOp = stateOp
						})
				}
			}
		}

		for pindex, sourcePartitions := range currSeqs {
			for sourcePartition, nodes := range sourcePartitions {
				for node, currUUIDSeq := range nodes {
					updateProgressEntry(pindex,
						sourcePartition, node,
						func(pe *ProgressEntry) {
							pe.currUUIDSeq = currUUIDSeq

							if pe.initUUIDSeq.UUID == "" {
								pe.initUUIDSeq = currUUIDSeq
							}
						})
				}
			}
		}

		for pindex, sourcePartitions := range wantSeqs {
			for sourcePartition, nodes := range sourcePartitions {
				for node, wantUUIDSeq := range nodes {
					updateProgressEntry(pindex,
						sourcePartition, node,
						func(pe *ProgressEntry) {
							pe.wantUUIDSeq = wantUUIDSeq
						})
				}
			}
		}

		for pindex, nextMoves := range mapNextMoves {
			for i, nodeStateOp := range nextMoves.Moves {
				updateProgressEntry(pindex, "", nodeStateOp.Node,
					func(pe *ProgressEntry) {
						pe.move = i
						pe.done = i < nextMoves.Next
					})
			}
		}
	})
}

func writeProgressTable(b *bytes.Buffer,
	maxNodeLen, maxPIndexLen int,
	seenNodes map[string]bool,
	seenNodesSorted []string,
	seenPIndexes map[string]bool,
	seenPIndexesSorted []string,
	progressEntries map[string]map[string]map[string]*ProgressEntry,
) {
	written, _ := b.Write([]byte("%%%"))
	for i := written; i < maxPIndexLen; i++ {
		b.WriteByte(' ')
	}
	b.WriteByte(' ')

	for i, seenNode := range seenNodesSorted {
		if i > 0 {
			b.WriteByte(' ')
		}

		// TODO: Emit node human readable ADDR:PORT.
		b.Write([]byte(seenNode))
	}
	b.WriteByte('\n')

	for _, seenPIndex := range seenPIndexesSorted {
		b.Write([]byte(" %                  "))
		b.Write([]byte(seenPIndex))

		for _, seenNode := range seenNodesSorted {
			b.WriteByte(' ')

			sourcePartitions, exists :=
				progressEntries[seenPIndex]
			if !exists || sourcePartitions == nil {
				writeProgressCell(b, nil, nil, maxNodeLen)
				continue
			}

			nodes, exists := sourcePartitions[""]
			if !exists || nodes == nil {
				writeProgressCell(b, nil, nil, maxNodeLen)
				continue
			}

			pe, exists := nodes[seenNode]
			if !exists || pe == nil {
				writeProgressCell(b, nil, nil, maxNodeLen)
				continue
			}

			writeProgressCell(b, pe, sourcePartitions, maxNodeLen)
		}

		b.WriteByte('\n')
	}
}

var opMap = map[string]string{
	"":        ".",
	"add":     "+",
	"del":     "-",
	"promote": "P",
	"demote":  "D",
}

func writeProgressCell(b *bytes.Buffer,
	pe *ProgressEntry,
	sourcePartitions map[string]map[string]*ProgressEntry,
	maxNodeLen int) {
	written := 0

	totPct := 0.0 // To compute average pct.
	numPct := 0

	if pe != nil {
		written, _ = fmt.Fprintf(b, "%d ", pe.move)

		if sourcePartitions != nil {
			n, _ := b.Write([]byte(opMap[pe.stateOp.Op]))
			written = written + n

			for sourcePartition, nodes := range sourcePartitions {
				if sourcePartition == "" {
					continue
				}

				pex := nodes[pe.node]
				if pex == nil || pex.wantUUIDSeq.UUID == "" {
					continue
				}

				if pex.wantUUIDSeq.Seq <= pex.currUUIDSeq.Seq {
					totPct = totPct + 1.0
					numPct = numPct + 1
					continue
				}

				n := pex.currUUIDSeq.Seq - pex.initUUIDSeq.Seq
				d := pex.wantUUIDSeq.Seq - pex.initUUIDSeq.Seq
				if d > 0 {
					pct := float64(n) / float64(d)
					totPct = totPct + pct
					numPct = numPct + 1
				}
			}
		}
	} else {
		b.Write([]byte("  ."))
		written = 3
	}

	if numPct > 0 {
		avgPct := totPct / float64(numPct)

		n, _ := fmt.Fprintf(b, " %.1f%%", avgPct*100.0)
		written = written + n
	}

	for i := written; i < maxNodeLen; i++ {
		b.WriteByte(' ')
	}
}
