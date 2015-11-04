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
	"github.com/couchbase/cbauth/metakv"
	log "github.com/couchbase/clog"
	"math"
	"sync"
    "strings"
	"encoding/json"
)

const (
	BASE_CFG_PATH = "/cbgt/cfg/"
	CAS_FORCE     = math.MaxUint64
)

type CfgMetaKv struct {
    uuid     string
	m        sync.Mutex
	path     string
	cancelCh chan struct{}
	rev      interface{}
    nodeDefKeys map[string]int
	cfgMem   *CfgMem
}

// NewCfgMetaKv returns a CfgMetaKv that reads and stores its single
// configuration file in the metakv.
func NewCfgMetaKv() (*CfgMetaKv, error) {
	cfg := &CfgMetaKv{
		path:     BASE_CFG_PATH,
		cancelCh: make(chan struct{}),
		cfgMem:   NewCfgMem(),
        uuid:     NewUUID(),
	}
	go func() {
		for {
			err := metakv.RunObserveChildren(cfg.path, cfg.metaKVCallback,
				cfg.cancelCh)
			if err == nil {
				return
			} else {
				log.Printf("metakv notifier failed (%v)", err)
			}
		}
	}()
	return cfg, nil
}

func (c *CfgMetaKv) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()
    if key == CfgNodeDefsKey(NODE_DEFS_WANTED) {
        rv := &NodeDefs{NodeDefs:make(map[string]*NodeDef)}
        tmp := &NodeDefs{NodeDefs:make(map[string]*NodeDef)}
        m := c.cfgMem.GetKeyWithPrefix(key)
        for _,v := range m {
            json.Unmarshal(v, tmp)
            for k1,v1 := range tmp.NodeDefs {
				log.Printf("combining key %v", k1)	
                rv.NodeDefs[k1] = v1
            }
            rv.UUID = tmp.UUID
            rv.ImplVersion = tmp.ImplVersion
        }
        data,_ := json.Marshal(rv)
        return data, 0, nil
    }
	return c.cfgMem.Get(key, cas)
}

func (c *CfgMetaKv) SetKey(key string, val []byte) (
    uint64, error) {
	var cas uint64
    rev, err := c.cfgMem.GetRev(key, 0)
        if err != nil {
            return 0, err
        }
    if rev == nil {
        err = metakv.Add(c.makeKey(key), val)
    } else {
        err = metakv.Set(c.makeKey(key), val, rev)
    }
    if err == nil {
        cas, err = c.cfgMem.Set(key, val, CAS_FORCE)
            if err != nil {
                return 0, err
            }
    }
	return cas,err
}

func (c *CfgMetaKv) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	log.Printf("setting key %v", key)
	c.m.Lock()
	defer c.m.Unlock()
    var err error
    if key == CfgNodeDefsKey(NODE_DEFS_WANTED) {
        // split the keys
        nd := &NodeDefs{}
        err = json.Unmarshal(val, nd)
        if err != nil {
            return 0, err
        }
        for k,v := range nd.NodeDefs {
            n := &NodeDefs{
                UUID : nd.UUID,
                NodeDefs:make(map[string]*NodeDef),
                ImplVersion : nd.ImplVersion,
            }
            n.NodeDefs[k] = v
            k = key + "_" + k
            val,_ = json.Marshal(n)
			log.Printf("splitted key %v", k)	
            cas, err = c.SetKey(k, val)
        }
    } else {
       cas,err = c.SetKey(key, val)
    }
	return cas, err
}

func (c *CfgMetaKv) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()
	return c.delUnlocked(key, cas)
}

func (c *CfgMetaKv) delUnlocked(key string, cas uint64) error {
    keys := []string{}
	var err error
    if strings.HasPrefix(key, CfgNodeDefsKey(NODE_DEFS_WANTED)) {
        m := c.cfgMem.GetKeyWithPrefix(CfgNodeDefsKey(NODE_DEFS_WANTED))
        for k,_ := range m {
            keys = append(keys, k)
        }
    } else {
        keys = append(keys, key)
    }
    // is this fine ? I dont think so.
    for _,key := range keys {
        rev, err := c.cfgMem.GetRev(key, cas)
            if err != nil {
                return err
            }
        err = metakv.Delete(c.makeKey(key), rev)
            if err != nil {
                return c.cfgMem.Del(key, 0)
            }
    }
    return err
}

func (c *CfgMetaKv) Load() error {
	metakv.IterateChildren(c.path, c.metaKVCallback)
	return nil
}

func (c *CfgMetaKv) metaKVCallback(path string, value []byte, rev interface{}) error {
	c.m.Lock()
	defer c.m.Unlock()
	key := c.getMetaKey(path)
	if value == nil {
		// key got deleted
		return c.delUnlocked(key, 0)
	}
	cas, err := c.cfgMem.Set(key, value, CAS_FORCE)
	if err == nil {
		c.cfgMem.SetRev(key, cas, rev)
	}
	return err
}

func (c *CfgMetaKv) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Subscribe(key, ch)
}

func (c *CfgMetaKv) Refresh() error {
	return nil
}

func (c *CfgMetaKv) OnError(err error) {
	log.Printf("cfg_metakv: OnError, err: %v", err)
}

func (c *CfgMetaKv) DelConf() {
	metakv.RecursiveDelete(c.path)
}

func (c *CfgMetaKv) makeKey(k string) string {
	return c.path + k
}

func (c *CfgMetaKv) getMetaKey(k string) string {
	return k[len(c.path):]
}
