//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/couchbase/blance"
)

// JSON/struct definitions of what the Manager stores in the Cfg.
// NOTE: You *must* update VERSION if you change these
// definitions or the planning algorithms change.

// An IndexDefs is zero or more index definitions.
type IndexDefs struct {
	// IndexDefs.UUID changes whenever any child IndexDef changes.
	UUID        string               `json:"uuid"`        // Like a revision id.
	IndexDefs   map[string]*IndexDef `json:"indexDefs"`   // Key is IndexDef.Name.
	ImplVersion string               `json:"implVersion"` // See VERSION.
}

// An IndexDef is a logical index definition.
type IndexDef struct {
	Type         string     `json:"type"` // Ex: "blackhole", etc.
	Name         string     `json:"name"`
	UUID         string     `json:"uuid"` // Like a revision id.
	Params       string     `json:"params"`
	SourceType   string     `json:"sourceType"`
	SourceName   string     `json:"sourceName"`
	SourceUUID   string     `json:"sourceUUID"`
	SourceParams string     `json:"sourceParams"` // Optional connection info.
	PlanParams   PlanParams `json:"planParams"`

	// NOTE: Any auth credentials to access datasource, if any, may be
	// stored as part of SourceParams.
}

// A PlanParams holds input parameters to the planner, that control
// how the planner should split an index definition into one or more
// index partitions, and how the planner should assign those index
// partitions to nodes.
type PlanParams struct {
	// MaxPartitionsPerPIndex controls the maximum number of source
	// partitions the planner can assign to or clump into a PIndex (or
	// index partition).
	MaxPartitionsPerPIndex int `json:"maxPartitionsPerPIndex"`

	// NumReplicas controls the number of replicas for a PIndex, over
	// the first copy.  The first copy is not counted as a replica.
	// For example, a NumReplicas setting of 2 means there should be a
	// primary and 2 replicas... so 3 copies in total.  A NumReplicas
	// of 0 means just the first, primary copy only.
	NumReplicas int `json:"numReplicas"`

	// HierarchyRules defines the policy the planner should follow
	// when assigning PIndexes to nodes, especially for replica
	// placement.  Through the HierarchyRules, a user can specify, for
	// example, that the first replica should be not on the same rack
	// and zone as the first copy.  Some examples:
	// Try to put the first replica on the same rack...
	// {"replica":[{"includeLevel":1,"excludeLevel":0}]}
	// Try to put the first replica on a different rack...
	// {"replica":[{"includeLevel":2,"excludeLevel":1}]}
	HierarchyRules blance.HierarchyRules `json:"hierarchyRules"`

	// NodePlanParams allows users to specify per-node input to the
	// planner, such as whether PIndexes assigned to different nodes
	// can be readable or writable.  Keyed by node UUID.  Value is
	// keyed by planPIndex.Name or indexDef.Name.  The empty string
	// ("") is used to represent any node UUID and/or any planPIndex
	// and/or any indexDef.
	NodePlanParams map[string]map[string]*NodePlanParam `json:"nodePlanParams"`

	// PIndexWeights allows users to specify an optional weight for a
	// PIndex, where weights default to 1.  In a range-partitioned
	// index, for example, some index partitions (or PIndexes) may
	// have more entries (higher weight) than other index partitions.
	PIndexWeights map[string]int `json:"pindexWeights"`

	// PlanFrozen means the planner should not change the previous
	// plan for an index, even if as nodes join or leave and even if
	// there was no previous plan.  Defaults to false (allow
	// re-planning).
	PlanFrozen bool `json:"planFrozen"`
}

// A NodePlanParam defines whether a particular node can service a
// particular index definition.
type NodePlanParam struct {
	CanRead  bool `json:"canRead"`
	CanWrite bool `json:"canWrite"`
}

// ------------------------------------------------------------------------

// A NodeDefs is comprised of zero or more node definitions.
type NodeDefs struct {
	// NodeDefs.UUID changes whenever any child NodeDef changes.
	UUID        string              `json:"uuid"`        // Like a revision id.
	NodeDefs    map[string]*NodeDef `json:"nodeDefs"`    // Key is NodeDef.UUID.
	ImplVersion string              `json:"implVersion"` // See VERSION.
}

// A NodeDef is a node definition.
type NodeDef struct {
	HostPort    string   `json:"hostPort"`
	UUID        string   `json:"uuid"`
	ImplVersion string   `json:"implVersion"` // See VERSION.
	Tags        []string `json:"tags"`
	Container   string   `json:"container"`
	Weight      int      `json:"weight"`
	Extras      string   `json:"extras"`
}

// ------------------------------------------------------------------------

// A PlanPIndexes is comprised of zero or more planPIndexes.
type PlanPIndexes struct {
	// PlanPIndexes.UUID changes whenever any child PlanPIndex changes.
	UUID         string                 `json:"uuid"`         // Like a revision id.
	PlanPIndexes map[string]*PlanPIndex `json:"planPIndexes"` // Key is PlanPIndex.Name.
	ImplVersion  string                 `json:"implVersion"`  // See VERSION.
	Warnings     map[string][]string    `json:"warnings"`     // Key is IndexDef.Name.
}

// A PlanPIndex represents the plan for a particular index partition,
// including on what nodes that the index partition is assigned to.
// An index partition might be assigned to more than one node if the
// "plan params" has a replica count > 0.
type PlanPIndex struct {
	Name             string `json:"name"` // Stable & unique cluster wide.
	UUID             string `json:"uuid"`
	IndexType        string `json:"indexType"`   // See IndexDef.Type.
	IndexName        string `json:"indexName"`   // See IndexDef.Name.
	IndexUUID        string `json:"indexUUID"`   // See IndefDef.UUID.
	IndexParams      string `json:"indexParams"` // See IndexDef.Params.
	SourceType       string `json:"sourceType"`
	SourceName       string `json:"sourceName"`
	SourceUUID       string `json:"sourceUUID"`
	SourceParams     string `json:"sourceParams"` // Optional connection info.
	SourcePartitions string `json:"sourcePartitions"`

	Nodes map[string]*PlanPIndexNode `json:"nodes"` // Keyed by NodeDef.UUID.
}

// A PlanPIndexNode represents the kind of service a node has been
// assigned to provide for an index partition.
type PlanPIndexNode struct {
	CanRead  bool `json:"canRead"`
	CanWrite bool `json:"canWrite"`
	Priority int  `json:"priority"` // Lower is higher priority, 0 is highest.
}

// PlanPIndexNodeCanRead returns true if PlanPIndexNode.CanRead is
// true; it's useful as a filter arg for Manager.CoveringPIndexes().
func PlanPIndexNodeCanRead(p *PlanPIndexNode) bool {
	return p != nil && p.CanRead
}

// PlanPIndexNodeCanWrite returns true if PlanPIndexNode.CanWrite is
// true; it's useful as a filter arg for Manager.CoveringPIndexes().
func PlanPIndexNodeCanWrite(p *PlanPIndexNode) bool {
	return p != nil && p.CanWrite
}

// PlanPIndexNodeOk always returns true; it's useful as a filter arg
// for Manager.CoveringPIndexes().
func PlanPIndexNodeOk(p *PlanPIndexNode) bool {
	return true
}

// ------------------------------------------------------------------------

// INDEX_DEFS_KEY is the key used for Cfg access.
const INDEX_DEFS_KEY = "indexDefs"

// Returns an intiialized IndexDefs.
func NewIndexDefs(version string) *IndexDefs {
	return &IndexDefs{
		UUID:        NewUUID(),
		IndexDefs:   make(map[string]*IndexDef),
		ImplVersion: version,
	}
}

// Returns index definitions from a Cfg provider.
func CfgGetIndexDefs(cfg Cfg) (*IndexDefs, uint64, error) {
	v, cas, err := cfg.Get(INDEX_DEFS_KEY, 0)
	if err != nil {
		return nil, 0, err
	}
	if v == nil {
		return nil, 0, nil
	}
	rv := &IndexDefs{}
	err = json.Unmarshal(v, rv)
	if err != nil {
		return nil, 0, err
	}
	return rv, cas, nil
}

// Updates index definitions on a Cfg provider.
func CfgSetIndexDefs(cfg Cfg, indexDefs *IndexDefs, cas uint64) (uint64, error) {
	buf, err := json.Marshal(indexDefs)
	if err != nil {
		return 0, err
	}
	return cfg.Set(INDEX_DEFS_KEY, buf, cas)
}

// ------------------------------------------------------------------------

// GetNodePlanParam returns a relevant NodePlanParam for a given node
// from a nodePlanParams, defaulting to a less-specific NodePlanParam
// if needed.
func GetNodePlanParam(nodePlanParams map[string]map[string]*NodePlanParam,
	nodeUUID, indexDefName, planPIndexName string) *NodePlanParam {
	var nodePlanParam *NodePlanParam
	if nodePlanParams != nil {
		m := nodePlanParams[nodeUUID]
		if m == nil {
			m = nodePlanParams[""]
		}
		if m != nil {
			nodePlanParam = m[indexDefName]
			if nodePlanParam == nil {
				nodePlanParam = m[planPIndexName]
			}
			if nodePlanParam == nil {
				nodePlanParam = m[""]
			}
		}
	}
	return nodePlanParam
}

// ------------------------------------------------------------------------

const NODE_DEFS_KEY = "nodeDefs"  // NODE_DEFS_KEY is used for Cfg access.
const NODE_DEFS_KNOWN = "known"   // NODE_DEFS_KNOWN is used for Cfg access.
const NODE_DEFS_WANTED = "wanted" // NODE_DEFS_WANTED is used for Cfg access.

// Returns an initialized NodeDefs.
func NewNodeDefs(version string) *NodeDefs {
	return &NodeDefs{
		UUID:        NewUUID(),
		NodeDefs:    make(map[string]*NodeDef),
		ImplVersion: version,
	}
}

// CfgNodeDefsKey returns the Cfg access key for a NodeDef kind.
func CfgNodeDefsKey(kind string) string {
	return NODE_DEFS_KEY + "-" + kind
}

// Retrieves node definitions from a Cfg provider.
func CfgGetNodeDefs(cfg Cfg, kind string) (*NodeDefs, uint64, error) {
	v, cas, err := cfg.Get(CfgNodeDefsKey(kind), 0)
	if err != nil {
		return nil, 0, err
	}
	if v == nil {
		return nil, 0, nil
	}
	rv := &NodeDefs{}
	err = json.Unmarshal(v, rv)
	if err != nil {
		return nil, 0, err
	}
	return rv, cas, nil
}

// Updates node definitions on a Cfg provider.
func CfgSetNodeDefs(cfg Cfg, kind string, nodeDefs *NodeDefs,
	cas uint64) (uint64, error) {
	buf, err := json.Marshal(nodeDefs)
	if err != nil {
		return 0, err
	}
	return cfg.Set(CfgNodeDefsKey(kind), buf, cas)
}

// CfgRemoveNodeDef removes a NodeDef with the given uuid from the Cfg.
func CfgRemoveNodeDef(cfg Cfg, kind, uuid, version string) error {
	nodeDefs, cas, err := CfgGetNodeDefs(cfg, kind)
	if err != nil {
		return err
	}

	if nodeDefs == nil {
		return nil
	}

	nodeDefPrev, exists := nodeDefs.NodeDefs[uuid]
	if !exists || nodeDefPrev == nil {
		return nil
	}

	delete(nodeDefs.NodeDefs, uuid)

	nodeDefs.UUID = NewUUID()
	nodeDefs.ImplVersion = version

	_, err = CfgSetNodeDefs(cfg, kind, nodeDefs, cas)

	return err
}

// ------------------------------------------------------------------------

func UnregisterNodes(cfg Cfg, version string, nodeUUIDs []string) error {
	for _, nodeUUID := range nodeUUIDs {
		for _, kind := range []string{NODE_DEFS_WANTED, NODE_DEFS_KNOWN} {
			err := CfgRemoveNodeDef(cfg, kind, nodeUUID, version)
			if err != nil {
				return fmt.Errorf("defs: UnregisterNodes,"+
					" nodeUUID: %s, kind: %s, err: %v",
					nodeUUID, kind, err)
			}
		}
	}

	return nil
}

// ------------------------------------------------------------------------

// PLAN_PINDEXES_KEY is used for Cfg access.
const PLAN_PINDEXES_KEY = "planPIndexes"

// Returns an initialized PlanPIndexes.
func NewPlanPIndexes(version string) *PlanPIndexes {
	return &PlanPIndexes{
		UUID:         NewUUID(),
		PlanPIndexes: make(map[string]*PlanPIndex),
		ImplVersion:  version,
		Warnings:     make(map[string][]string),
	}
}

// CopyPlanPIndexes returns a copy of the given planPIndexes, albeit
// with a new UUID and given version.
func CopyPlanPIndexes(planPIndexes *PlanPIndexes,
	version string) *PlanPIndexes {
	r := NewPlanPIndexes(version)
	j, _ := json.Marshal(planPIndexes)
	json.Unmarshal(j, r)
	r.UUID = NewUUID()
	r.ImplVersion = version
	return r
}

// Retrieves PlanPIndexes from a Cfg provider.
func CfgGetPlanPIndexes(cfg Cfg) (*PlanPIndexes, uint64, error) {
	v, cas, err := cfg.Get(PLAN_PINDEXES_KEY, 0)
	if err != nil {
		return nil, 0, err
	}
	if v == nil {
		return nil, 0, nil
	}
	rv := &PlanPIndexes{}
	err = json.Unmarshal(v, rv)
	if err != nil {
		return nil, 0, err
	}
	return rv, cas, nil
}

// Updates PlanPIndexes on a Cfg provider.
func CfgSetPlanPIndexes(cfg Cfg, planPIndexes *PlanPIndexes, cas uint64) (
	uint64, error) {
	buf, err := json.Marshal(planPIndexes)
	if err != nil {
		return 0, err
	}
	return cfg.Set(PLAN_PINDEXES_KEY, buf, cas)
}

// Returns true if both PlanPIndexes are the same, where we ignore any
// differences in UUID or ImplVersion.
func SamePlanPIndexes(a, b *PlanPIndexes) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a.PlanPIndexes) != len(b.PlanPIndexes) {
		return false
	}
	return SubsetPlanPIndexes(a, b) && SubsetPlanPIndexes(b, a)
}

// Returns true if PlanPIndex children in a are a subset of those in
// b, using SamePlanPIndex() for sameness comparion.
func SubsetPlanPIndexes(a, b *PlanPIndexes) bool {
	for name, av := range a.PlanPIndexes {
		bv, exists := b.PlanPIndexes[name]
		if !exists {
			return false
		}
		if !SamePlanPIndex(av, bv) {
			return false
		}
	}
	return true
}

// Returns true if both PlanPIndex are the same, ignoring PlanPIndex.UUID.
func SamePlanPIndex(a, b *PlanPIndex) bool {
	// Of note, we don't compare UUID's.
	if a.Name != b.Name ||
		a.IndexName != b.IndexName ||
		a.IndexUUID != b.IndexUUID ||
		a.IndexParams != b.IndexParams ||
		a.SourceType != b.SourceType ||
		a.SourceName != b.SourceName ||
		a.SourceUUID != b.SourceUUID ||
		a.SourceParams != b.SourceParams ||
		a.SourcePartitions != b.SourcePartitions ||
		!reflect.DeepEqual(a.Nodes, b.Nodes) {
		return false
	}
	return true
}

// Returns true if both the PIndex meets the PlanPIndex, ignoring UUID.
func PIndexMatchesPlan(pindex *PIndex, planPIndex *PlanPIndex) bool {
	same := pindex.Name == planPIndex.Name &&
		pindex.IndexName == planPIndex.IndexName &&
		pindex.IndexUUID == planPIndex.IndexUUID &&
		pindex.IndexParams == planPIndex.IndexParams &&
		pindex.SourceType == planPIndex.SourceType &&
		pindex.SourceName == planPIndex.SourceName &&
		pindex.SourceUUID == planPIndex.SourceUUID &&
		pindex.SourceParams == planPIndex.SourceParams &&
		pindex.SourcePartitions == planPIndex.SourcePartitions
	return same
}
